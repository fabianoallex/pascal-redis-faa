# Decisões de projeto — racional

Este documento guarda o **porquê**. O `CLAUDE.md` guarda o **o quê** (decisões já fechadas,
em forma curta) e o `README.md` guarda o **como usar**. Origem: avaliação de viabilidade
feita em 2026-08-22, antes de escrever a primeira linha da lib.

O ponto de partida foi a `../pascal-amqp-faa`. A tentação natural é reaproveitar o desenho
inteiro dela — e é justamente isso que este documento existe para impedir. O protocolo RESP
tem uma forma diferente da do AMQP 0-9-1, e copiar a arquitetura levaria a complexidade
inútil em alguns pontos e a bugs em outros.

---

## 1. Não existe canal → existe pool

O AMQP multiplexa N canais lógicos sobre um socket: `TAMQPChannel` é a unidade de trabalho
e várias threads operam canais diferentes ao mesmo tempo, na mesma conexão.

O Redis não multiplexa nada. Uma conexão processa um comando por vez, em ordem estrita, e a
resposta N corresponde ao comando N. Não há campo de correlação no protocolo — a ordem *é*
a correlação.

Consequência direta: **o análogo de `TAMQPChannel` não existe**. A unidade de concorrência
é a conexão inteira. Para N threads trabalharem em paralelo, são necessárias N conexões.

```
TRedisConnection   1 socket, 1 comando por vez, serializado por lock
TRedisPool         N conexões, checkout/checkin
TRedisClient       fachada: pega do pool, executa, devolve. É o que 95% do código usa.
```

## 2. Sem thread de leitura nas conexões de comando

A `TAMQPConnection` precisa de uma `TAMQPReaderThread` porque o broker empurra métodos
assíncronos a qualquer momento (entregas, `Basic.Return`, confirms, `Channel.Close`). Todo
o maquinário de `TAMQPMonitor`, sinalização e despacho existe por causa disso.

Em RESP2, fora do pub/sub, **o servidor só fala quando perguntado**. Então:

- **Conexões de comando:** a própria thread chamadora escreve e lê, sob o lock da conexão.
  Zero threads extras, zero handoff, zero condvar.
- **Conexões de assinante e de comando bloqueante:** aí sim, thread dedicada de leitura, e
  o despacho de callback vai para o `RedisPool` (o thread pool herdado), no mesmo padrão dos
  work items da lib AMQP.

Ou seja: a maior parte da complexidade de concorrência da lib AMQP simplesmente não
aparece aqui. Isso é uma economia, não uma omissão.

## 3. Conexão suja nunca volta para o pool

Se um comando sofre timeout de leitura ou erro de I/O, **pode haver uma resposta órfã ainda
a caminho** no socket. Devolver essa conexão ao pool faz o próximo comando ler a resposta do
comando anterior — e a partir daí toda a conexão fica deslocada em uma resposta, silenciosa
e permanentemente.

Regra: erro de I/O ou timeout → a conexão é **fechada e destruída**, e o pool cria outra.
Nunca "limpar o buffer e reaproveitar".

É o bug clássico de cliente Redis e o motivo de o M3 ter um teste de integração dedicado a
ele (matar a conexão no meio de um comando e verificar que o próximo checkout não herda a
resposta).

## 4. Comando em voo não é re-executado

A lib AMQP tem `RepublishUnconfirmedOnReconnect`: publishes deixados sem confirmação por
uma queda são re-publicados na reconexão. Faz sentido lá porque publish é (quase sempre)
idempotente do ponto de vista do domínio, e o modelo é at-least-once por construção.

No Redis isso seria um erro grave. `INCR`, `LPUSH`, `SETNX`, `XADD` não são idempotentes:
se a conexão cai depois de o servidor executar o comando mas antes de a resposta chegar, um
retry duplica o efeito, e o cliente não tem como distinguir os dois casos.

Regra: o comando em voo falha com `ERedisConnectionLost` e a decisão de repetir fica com o
chamador, que é quem sabe se a operação é segura de repetir.

O que a reconexão **replaya** é só o estado de sessão, porque não há topologia no servidor:
`HELLO` (se RESP3), `AUTH`, `SELECT <db>` e as assinaturas de pub/sub.

## 5. Pub/Sub sequestra a conexão (em RESP2)

Depois de `SUBSCRIBE`, uma conexão RESP2 só aceita comandos de (un)subscribe — qualquer
outro comando é recusado. Por isso o assinante é uma **conexão dedicada, criada fora do
pool**, com thread de leitura própria.

Em RESP3 (`HELLO 3`) as mensagens chegam como push (tipo `>`), distinguíveis das respostas
normais, e a conexão continua utilizável para comandos comuns. A lib suporta os dois:
RESP2 como padrão (compatibilidade máxima) e RESP3 opt-in.

## 6. Comandos bloqueantes precisam de conexão própria

`BLPOP`, `BRPOP`, `XREAD BLOCK` e `WAIT` monopolizam a conexão por segundos ou minutos.
Uma conexão presa em `BLPOP` no pool comum é uma conexão a menos para todo mundo, e o read
timeout do pool a mataria no meio da espera.

Regra: comando bloqueante roda em conexão *detached*, com read timeout maior que o timeout
do próprio comando, e essa conexão não é devolvida ao pool enquanto estiver bloqueada.

## 7. O transporte precisa de read timeout (o AMQP não precisava)

`TAMQPTcpSocket` tem apenas `Connect/Receive/Send/Close`. Não há timeout de leitura, e no
AMQP isso é aceitável: o heartbeat do protocolo detecta um peer morto e a thread de
reconexão fecha o socket, desbloqueando a leitura.

O Redis não tem heartbeat de protocolo. Sem `SO_RCVTIMEO`, um comando pendurado prende a
conexão *e a thread do chamador* indefinidamente — e como a leitura acontece na thread
chamadora (decisão 2), isso trava o código do usuário.

Por isso o M3 acrescenta `SetReceiveTimeout`/`SetSendTimeout` ao `Redis.Transport`,
propagando até os dois backends TLS. É a **única mudança estrutural** nas units copiadas.

## 8. Binário-seguro por contrato

Bulk strings RESP são binárias: podem conter zeros, CRLF, qualquer byte. Se a API núcleo
trabalhar com `string`, o codepage dinâmico do FPC transcodifica valores silenciosamente e
corrompe dados — e só no FPC, o que faz o bug passar por toda a validação feita no Delphi.

Regra: o núcleo trabalha com `TBytes`. As sobrecargas `string` existem por conveniência e
passam por `RedisUtf8Encode/Decode`, exatamente como `AMQP.Wire` faz.

Pelo mesmo motivo, comandos são sempre emitidos no *unified request protocol* (array de
bulk strings, `*N\r\n$len\r\n...`). Comando inline (`PING\r\n`) é mais curto mas não é
binário-seguro — não será usado fora do smoke test do M0.

## 9. A árvore de respostas não usa `TValue`

`AMQP.Wire` modela field-values com `TValue`, e isso custou caro no FPC 3.2.2: dois erros
internos do compilador em construções específicas, e o `AmqpUnwrapValue` (necessário porque
o `TValue.Make` do FPC não colapsa TValue-dentro-de-TValue como o do Delphi).

A árvore RESP não justifica esse risco: são 5 tipos em RESP2 (`+ - : $ *`) e ~8 a mais em
RESP3 (`_ , # ! = ( % ~ | >`) — um conjunto pequeno e fechado. Modelar com um enum
(`TRedisReplyKind`) e uma interface (`IRedisReply`), sem RTTI.

A escolha por **interface** em vez de classe é deliberada: o refcount evita que a suíte de
testes com verificação de leaks (DUnitX reporta 0 leaks) vire uma caçada a `try/finally` em
cada nó de uma árvore aninhada.

## 10. Kernel genérico antes das fachadas tipadas

Redis tem 240+ comandos. Modelar todos antes de entregar qualquer coisa é receita para o
projeto nunca sair do papel; e escolher "os importantes" garante que o usuário vai esbarrar
justamente no que faltou.

Solução: `Execute('SET', ['k','v'])` no M2 já alcança **qualquer** comando presente ou
futuro. As fachadas tipadas por família (`Redis.Commands.Strings`, `.Hashes`, ...) são
camadas de conveniência por cima, entregues por milestone. O escopo pode crescer devagar
sem nunca bloquear quem usa a lib.

Essa separação espelha a que a lib AMQP já faz entre `AMQP.Connection` e as units
`AMQP.*.Methods`.

## 11. Cluster, Sentinel e client-side caching ficam fora do v1

Cada um é um projeto dentro do projeto:

- **Cluster:** mapa de 16384 slots, CRC16 das chaves, tratamento de `MOVED`/`ASK`,
  redescoberta de topologia, comandos multi-chave que precisam de hash tags.
- **Sentinel:** protocolo próprio de descoberta e failover, com sua própria reconexão.
- **Client-side caching:** `CLIENT TRACKING` + invalidação por push RESP3, ou seja, um cache
  local com invalidação distribuída.

Nenhum é necessário para o caso de uso alvo (Redis único, ou atrás de um proxy). Ficam
registrados aqui como fora de escopo para não serem reabertos por impulso — mesmo
tratamento que as transações `tx` receberam na lib AMQP.

## 12. Cópia renomeada, não repositório compartilhado

As quatro units herdadas (`Threading`, `Transport`, `Transport.Tls`, `Transport.OpenSSL`)
foram copiadas e renomeadas em vez de extraídas para uma lib comum.

Motivo: é o precedente já estabelecido pela `pascal-pipes-faa`, cujo `Pipes.Threading.pas` é
o `AMQP.Threading.pas` renomeado, com "sem dependência entre repositórios" registrado no
`CLAUDE.md`. Uma lib-núcleo compartilhada acoplaria três repositórios que evoluem em ritmos
diferentes, e o código em questão é estável (o transporte praticamente não muda desde a
validação em Linux/ARM64).

Custo aceito: correção de bug num transporte precisa ser portada manualmente para os outros
dois repos. Por isso cada unit copiada carrega um bloco de PROVENIENCIA no topo, dizendo de
onde veio.

---

## 13. O mapa do RESP3 é guardado achatado (M1)

Um mapa RESP3 chega como `%2\r\n` seguido de quatro elementos (chave, valor, chave, valor).
Seria natural expor `Count = 2` e um par por índice. A árvore expõe `Count = 4`, achatado.

O motivo é que o **mesmo dado tem duas formas no fio**. `HGETALL`, `CONFIG GET`, `XPENDING` e
a resposta do próprio `HELLO` chegam como array achatado em RESP2 e como mapa em RESP3.
Guardando achatado, as duas formas viram a mesma árvore, e o código da aplicação — inclusive
o das fachadas tipadas dos M4 em diante — não pergunta qual protocolo está em uso. O
`ValueByKey` funciona igual nos dois casos, e é isso que os testes
`Mapa_ValueByKey` e `ValueByKey_TambemEmArrayResp2` fixam.

Custo aceito: `Count` de um mapa não é o número de pares, o que surpreende quem leu a
especificação do RESP antes de ler esta lib. Está documentado na própria interface.

---

## 14. Nulo não é vazio, e converter nulo para zero é proibido (M1)

`$-1` (nulo) e `$0` (string vazia) são respostas diferentes: "a chave não existe" contra "a
chave existe e vale `''`". A árvore mantém a distinção com um kind próprio (`rkNull`) em vez
de representar nulo como bulk string de comprimento zero.

A consequência menos óbvia é nos acessores. `AsInteger` e `AsDouble` num `rkNull`
**levantam** `ERedisTypeError` em vez de devolver 0 — devolver 0 apagaria exatamente a
distinção que o Redis faz questão de manter, e o bug apareceria longe da causa (um saldo
lido como zero porque a chave sumiu). Quem quer o comportamento tolerante testa `IsNull`
antes, o que é uma linha e deixa a intenção explícita.

Já `AsString` num nulo devolve `''`, porque string tem um vazio natural e forçar exceção ali
seria irritante sem ganho, e `AsBoolean` devolve `False`, porque bulk string nula é
literalmente como o RESP2 responde "não" a um `SET NX` que não pegou.

---

## 15. O atributo (`|`) não é um tipo de resposta (M1)

Por especificação o atributo do RESP3 não é uma resposta: é metadado que **precede** a
resposta de verdade (o `key-popularity` do client-side caching, por exemplo). Modelá-lo como
mais um kind faria o chamador receber o mapa no lugar do valor que pediu, em resposta a um
comando comum — um bug intermitente, porque o servidor só manda atributo às vezes.

O leitor consome o mapa, lê a resposta seguinte e pendura o mapa em `IRedisReply.Attributes`.
Ignorar atributos passa a ser sempre seguro, que é o comportamento certo por padrão.

---

## 16. A leitura sai de uma fonte de bytes, não de um socket (M1)

O `TRedisReader` consome um `IRedisByteSource`, não um `TRedisTcpSocket`. Duas razões.

A primeira é testabilidade real. O `TRedisBytesSource` entrega a resposta em pedaços de
tamanho controlado, então dá para reler a mesma resposta com chunks de 1, 2, 3, 5, 7 e 13
bytes e exigir árvore idêntica. Leitura parcial é o modo de falha clássico de parser de
protocolo e quase nunca acontece por acidente numa LAN — um parser que só funciona quando a
resposta chega inteira numa syscall passa em todos os testes ingênuos e quebra em produção
sob carga. Sem essa costura, esse teste exigiria um servidor falso com socket.

A segunda é de dependência: a `Redis.Resp` não conhece a `Redis.Transport`. O adaptador
socket→fonte nasce na `Redis.Connection` (M2), junto de quem sabe o que fazer quando a
conexão morre.

Não há máquina de estados incremental do tipo "me dê bytes, eu devolvo respostas prontas":
como a lib lê na própria thread chamadora sob o lock da conexão (decisão 2), bloquear
esperando o resto da resposta é exatamente o comportamento desejado.

---

## 17. Erro do servidor levanta; erro de conexão invalida (M2)

São duas coisas diferentes e a conexão trata cada uma do seu jeito.

Um `-WRONGTYPE`, `-NOSCRIPT` ou `-NOAUTH` é uma **resposta válida**, só que negativa: o
protocolo funcionou, o comando é que não fazia sentido. `Execute` levanta
`ERedisReplyError` (com o `Code` pronto para testar), a conexão continua sã e volta ao pool
normalmente. Invalidar a conexão a cada erro de aplicação torraria uma conexão do pool por
`GET` em chave de tipo errado.

Um erro de I/O, um timeout ou um fluxo malformado são o oposto: a conexão está perdida. Ela
fecha o socket na hora, passa a recusar comandos e **não** reexecuta o comando em voo — `INCR`,
`LPUSH` e `SETNX` não são idempotentes, e repetir em silêncio corromperia dados (decisão 4).
Tudo o que vem da camada de transporte (`ERedisTransport`, `ERedisTls`, erro de socket da RTL)
é traduzido para `ERedisConnectionLost`: do ponto de vista de quem chamou é tudo o mesmo fato,
e o chamador não deveria precisar conhecer as exceções das camadas de baixo.

`ExecuteRaw` existe para quem prefere ramificar sem exceção: devolve o nó `rkError` em vez de
levantar. É o que o `Ping` usa — um health check que levanta por erro de servidor é um health
check que não serve para decidir nada.

O pipeline **nunca** levanta por erro de servidor. Num lote de dez comandos, o servidor
executou os dez de qualquer jeito; abortar no primeiro erro esconderia o resultado dos outros
nove, e saber *qual* falhou é justamente o que interessa. Cada item da resposta pode ser um
`rkError`.

---

## 18. Conexão suja é um estado, não um erro (M2)

Lidas todas as respostas esperadas, `TRedisReader.Buffered` tem que ser zero. Se sobrou byte,
o que sobrou é resposta de um comando anterior — o caso clássico: um comando sofreu timeout,
o cliente desistiu, e a resposta chegou depois. A resposta que acabou de ser entregue está
correta, então levantar exceção seria mentira; mas o próximo comando nessa conexão leria a
sobra achando que é a resposta dele, e a partir daí todo valor sai deslocado por um.

Por isso a conexão marca `IsDirty` em vez de levantar, e `IsUsable` (aberta, inteira e limpa)
é o que o pool do M3 consulta no checkin para destruir em vez de devolver. É o bug clássico
de cliente Redis, e ele merece detecção explícita porque em produção se manifesta como "o
sistema começou a devolver o valor da chave errada", muito longe da causa.

---

## 19. `Abort` não pega o lock — de propósito (M2)

`Close` é ordenado: pega o lock e espera o comando em voo terminar. Só que é exatamente por
isso que ele não serve para o caso em que a conexão está pendurada num `Receive` que nunca
volta — ele ficaria esperando no lock, junto com a thread travada.

`Abort` derruba o socket **sem** pegar o lock. A leitura pendurada devolve zero, vira
`ERedisConnectionLost` na thread que estava esperando, e essa thread faz a faxina (que é
sempre sob o lock). O `Abort` em si só marca e fecha o handle: não libera reader nem stream,
que a outra thread pode estar usando naquele instante. É a mesma manobra do `Close` do
`TRedisTcpSocket` herdado da lib AMQP, e é o que o watchdog de timeout do M3 vai chamar.

---

## 20. Em RESP2 não se emite `HELLO` (M2)

Seria cômodo mandar `HELLO 2` em toda conexão só para saber a versão do servidor e o id da
conexão. Mas `HELLO` só existe a partir do Redis 6.0: fazer isso exigiria Redis 6 de quem
pediu RESP2, que é justamente o modo que existe para funcionar em qualquer servidor.

Então em RESP2 o handshake é `AUTH` (quando há senha), `CLIENT SETNAME` (quando há nome) e
`SELECT` (quando o banco difere de 0) — nessa ordem, porque sem autenticar antes o servidor
recusaria os outros dois com `NOAUTH`. `ServerVersion` e `ServerId` ficam vazios; quem
precisa deles em RESP2 usa `INFO server`.

Em RESP3 o `HELLO 3` faz autenticação e troca de protocolo num round-trip só. Se o servidor
não conhecer o comando, a lib traduz o `unknown command 'HELLO'` para uma mensagem que
explica o que aconteceu de verdade — repassar o erro cru mandaria o usuário caçar um comando
que ele não escreveu.

---

## 21. A conexão inteira é testável sem rede (M2)

`TRedisConnection.CreateOnStream` adota um `TStream` já conectado no lugar de abrir socket.
Com isso as suítes unitárias sobem a conexão inteira sobre um servidor falso em memória e
conferem byte a byte o que vai para o fio (handshake, unified request protocol, lote de
pipeline) — e, mais importante, exercitam os caminhos de falha que são caros de reproduzir
contra um servidor de verdade: fim de fluxo no meio da resposta, `send` parcial, `recv` de um
byte por vez e resposta órfã sobrando no buffer.

O servidor falso entrega as respostas em **pacotes**, e uma leitura nunca atravessa a fronteira
entre dois pacotes. Isso não é detalhe: um fake que despeja tudo de uma vez faria o leitor
encher o buffer com respostas futuras, e a conexão se declararia suja (decisão 18) sem ter
culpa. Duas respostas no mesmo pacote é como se simula, de propósito, a resposta órfã.

O mesmo construtor serve ao pool do M3 quando ele quiser injetar um transporte já pronto.

---

## 22. Timeout não é fim de stream (M3)

O transporte tinha `Connect/Receive/Send/Close` e mais nada — no AMQP isso bastava, porque o
heartbeat detectava a morte do outro lado. O Redis não tem heartbeat: se o servidor emudecer
no meio de uma resposta, a leitura fica parada para sempre, prendendo a conexão **e a thread
que chamou** (que, pela decisão 2, é a thread da aplicação).

Daí `SetReceiveTimeout`/`SetSendTimeout` (`SO_RCVTIMEO`/`SO_SNDTIMEO`). O detalhe que
custaria uma tarde: o Windows quer um `DWORD` de milissegundos e o Unix quer um `timeval` —
e passar a forma errada **não dá erro**, o `setsockopt` aceita e o timeout simplesmente não
acontece, que é o pior desfecho possível para este código.

Estourado o prazo, o socket levanta `ERedisTransportTimeout` em vez de devolver 0. A
diferença importa: fim de stream significa "o servidor encerrou", e timeout significa "o
servidor ainda pode responder, mas eu desisti". No segundo caso há uma resposta a caminho, e
é por isso que a conexão é destruída em vez de reciclada.

Os dois backends TLS rebaixam qualquer exceção a EOF (para o parser tratar como conexão
encerrada); os dois foram ajustados para deixar **só** o timeout passar. Sem isso, um
timeout sob TLS chegaria ao chamador disfarçado de "servidor encerrou".

E há uma segunda armadilha, esta na RTL do Delphi: `TSocket.ReceiveTimeout` atribuído
**antes** do `Connect` não chega ao socket. O `Connect` faz `FSocket := CreateSocket`, e é
dentro do `CreateSocket` que a RTL aplica a opção — sobre `FSocket`, que ainda é
`InvalidSocket` naquele ponto. Falha, o resultado é descartado, e como o setter já guardou o
valor, reatribuí-lo depois não faz nada. O `ApplyTimeouts` só escreve a propriedade com a
conexão de pé. O sintoma é traiçoeiro justamente porque o código compila igual nos dois
compiladores e só o Delphi fica esperando o comando inteiro — foi assim que apareceu.

---

## 23. O pool descarta; não conserta (M3)

Na devolução, o pool destrói a conexão em três casos: invalidada (erro de I/O, timeout,
fluxo malformado), **suja** (sobrou byte no buffer — decisão 18) ou com o banco corrente
diferente do configurado.

O terceiro é o menos óbvio e o mais traiçoeiro. `SELECT` vive na conexão, não no servidor:
reciclar uma conexão que ficou no banco 3 entregaria à próxima thread um banco diferente do
que ela pediu, e o estrago — ler e gravar no lugar errado — apareceria muito longe da causa.

Não existe "retomar" uma conexão morta no Redis: o socket foi embora com o comando em voo.
O que o pool chama de reconexão é abrir uma conexão **nova**, e o handshake dela já replaya
todo o estado que existia (`HELLO`/`AUTH`, `CLIENT SETNAME`, `SELECT`) — não há topologia no
servidor para restaurar, ao contrário do AMQP. O comando que estava em voo continua não
sendo repetido (decisão 4).

---

## 24. O que é rede não acontece sob o lock (M3)

Sob o lock do pool só andam contador e lista. Abrir conexão, dar o PING de saúde e fechar
socket acontecem fora dele.

O motivo é direto: uma ida e volta de rede segurando o lock congelaria todas as outras
threads pelo mesmo tempo — e o PING de saúde acontece justamente quando a rede está ruim,
que é quando congelar todo mundo é pior.

Isso obriga a uma sutileza: a vaga no `MaxSize` é reservada **antes** de soltar o lock, e
devolvida se a conexão não abrir. Sem a reserva, N threads veriam o mesmo "ainda cabe uma" e
o pool estouraria o teto; sem a devolução, cada falha de conexão encolheria o pool para
sempre, e a aplicação só voltaria ao normal com restart.

O health check em si é opt-out por tempo (`HealthCheckAfterIdleMs`): conexão que acabou de
voltar não precisa de PING, conexão parada há meia hora precisa — quem derruba conexão
ociosa é o servidor (timeout do lado de lá, failover, `CLIENT KILL`) e o cliente não é
avisado.

---

## 25. O pool é testável sem rede, e o teste de conexão-suja é de integração (M3)

`TRedisPool.CreateConnection` é `virtual` de propósito: as suítes unitárias sobrescrevem
para devolver conexões sobre o servidor falso em memória. Com isso dá para testar teto,
reuso, descarte, poda por ociosidade e health check reprovando — sem servidor e sem espera.

Duas coisas, porém, **não** se testam com fake, e é por isso que existe `tests/Integration`:

1. **Que o `SO_RCVTIMEO` foi mesmo aplicado.** Um servidor falso não tem socket, logo não
   tem como estourar timeout de socket. O teste real manda um `BLPOP` de 2 s numa conexão
   com timeout de 300 ms e exige que a desistência venha antes.
2. **Que a conexão contaminada não volta.** Depois desse timeout, a resposta do `BLPOP` está
   a caminho de verdade. Com `MaxSize = 1`, o pool é obrigado a reusar a única conexão que
   tem — então, se ele não a destruísse, o `GET` seguinte leria a resposta atrasada do
   `BLPOP` e a aplicação receberia, calada, o valor errado. É o bug clássico de cliente
   Redis, reproduzido de propósito.

Um detalhe que os testes de pool ensinaram: **não dá para verificar troca de conexão
comparando ponteiros**. O gerenciador de memória costuma devolver exatamente o endereço do
bloco recém-liberado, então `velha <> nova` falha mesmo quando a troca aconteceu — e ainda
por cima lê um ponteiro morto. Quem atesta a troca são os contadores do pool
(`CreatedCount`/`DiscardedCount`).

## 26. As famílias penduram num executor abstrato, não na conexão (M4)

As fachadas por família precisam de alguém que execute comandos; quem as reúne
(`TRedisClient`) precisa das famílias. Ligar as duas pontas direto fecharia um ciclo de
units, que nem Delphi nem FPC aceitam.

A solução é uma classe abstrata mínima, `TRedisCommandExecutor`, na `Redis.Commands`, com
três métodos: `Execute`, `ExecuteRaw` e `ExecuteBlocking`. Com ela a seta aponta num sentido
só — `Redis.Commands` ← `Redis.Commands.Strings` ← `Redis.Client` — e as famílias ficam
ignorantes de pool, socket e protocolo.

Duas alternativas foram descartadas:

- **Class helper por família.** Só um helper de uma classe fica visível por escopo, e com
  seis famílias em seis units o compilador escolheria uma em silêncio. Bug de importação,
  não de código.
- **`TRedisConnection` implementando o executor.** Obrigaria a conexão a conhecer a camada
  de comandos e a mudar de ancestral para satisfazer uma camada acima dela. A conexão saiu
  do M4 sem uma linha alterada, que é como uma peça de kernel deve atravessar um milestone
  de conveniência.

Quem quer as famílias sobre uma conexão específica usa `TRedisClient.CreateOnConnection`,
que é o mesmo caminho de quem precisa de afinidade de conexão para `SELECT`, `MULTI` ou
`WATCH`.

## 27. Na fachada, nulo não vira `''` nem `0` (M4)

A regra do M1 (decisão 14) valia para a árvore de respostas. Na fachada tipada ela volta a
ser uma escolha, porque a assinatura mais confortável — `function GetString(chave): string`
— é justamente a que apaga a diferença entre "a chave não existe" e "a chave existe e vale
`''`". Num cache, essas duas coisas levam a decisões opostas.

A convenção, válida nas seis famílias:

1. Escalar vira tipo nativo: `Boolean`, `Int64`, `Double`, `string`.
2. Resposta que **pode ser nula** devolve `IRedisReply` **ou** ganha um par `TryXxx` com
   `out` e retorno `Boolean` — `TryGet`, `HTryGet`, `ZTryScore`, `ZTryRank`.
3. Lista de valores vira `TRedisStringArray`, com item nulo virando `''`. Vale só onde o
   servidor **não** produz nulo no meio da lista (`LRANGE`, `SMEMBERS`, `HKEYS`); onde
   produz — `MGET`, `HMGET` — o retorno é `IRedisReply`, de propósito.

`GetString` continua existindo, porque na maior parte dos casos o `''` serve; o que não
existe é *só* ele.

## 28. Um argumento é `TRedisArg`, não duas sobrecargas (M4)

Chaves, campos e valores do Redis são binários. A saída óbvia seria duplicar cada método:
uma sobrecarga `string` e uma `TBytes`. Com ~150 métodos, isso dobraria a superfície da API
e a chance de as duas versões divergirem.

Como `TRedisArg` já tem `class operator Implicit` de `string` e de `TBytes`, uma assinatura
única `const AKey: TRedisArg` aceita as duas formas na chamada, sem conversão explícita e
sem perder o contrato binário. Foi verificado nos dois compiladores antes de o M4 começar:
a conversão implícita vale para parâmetro, não só para elemento de array constructor.

O retorno é o lado que não dá para unificar — `string` e `TBytes` são tipos distintos — e aí
existe um par `...Bytes` só onde o valor pode não ser texto (`GetBytes`, `HGetBytes`,
`BLPopBytes`, `SMembersBytes`).

## 29. Comando bloqueante sai por um pool separado (M4)

A decisão 6 já dizia que `BLPOP` precisa de conexão fora do pool comum. O M4 precisou
escolher **qual** conexão, e a resposta é um segundo `TRedisPool`, criado sob demanda na
primeira chamada bloqueante.

As alternativas eram piores:

- **Uma conexão detached por chamada** custa um TCP + handshake a cada `BLPOP`. Num worker
  em laço, é um handshake por tarefa.
- **Uma conexão detached em cache** serializaria dois workers do mesmo cliente num socket
  só — exatamente o que o pool existe para evitar.

O prazo do socket é esticado por chamada (`SetReceiveTimeout`) para o timeout do comando
mais `REDIS_BLOCKING_MARGIN_MS` (2 s), e **restaurado antes da devolução**: uma conexão
voltando ao pool com 32 s de read timeout faria o health check de um `PING` esperar 32 s por
um servidor que já morreu. Timeout zero (esperar para sempre) vira read timeout zero — a
única situação em que a lib deixa uma conexão sem prazo, e só porque o comando também não
tem.

No modo conexão única (`CreateOnConnection`) não há pool nenhum: a conexão já é dedicada a
quem chamou, então o bloqueante estica e restaura o prazo dela mesma.

## 30. `WITHSCORES` muda de forma entre RESP2 e RESP3 — a fachada absorve (M4)

O M1 resolveu o `HGETALL` guardando o mapa do RESP3 achatado (decisão 13), o que fez array
e mapa terem a mesma forma. O sorted set traz o caso inverso, e mais desagradável: em RESP2
o `ZRANGE ... WITHSCORES` responde uma lista **achatada** (membro, score, membro, score); em
RESP3, uma lista de **pares** `[membro, score]`, com o score como double nativo.

Achatar o par no leitor resolveria este comando e estragaria os outros — nem todo array de
arrays é um par membro/score. Então a conversão fica na família, em
`RedisReplyToScoreMembers`, que decide pela forma do primeiro item: se ele é agregado,
chegaram pares; se é escalar, chegou lista achatada. Não há ambiguidade, porque um membro é
sempre escalar.

O resultado é `TRedisScoreMemberArray` nos dois protocolos — que é o ponto: trocar
`Protocol` não pode obrigar a aplicação a reescrever o comando mais usado do tipo. Há teste
disso nas duas suítes, e o de integração roda o mesmo comando por dois clientes, um em RESP2
e outro em RESP3, contra o servidor de verdade.

## 31. TLS não é upgrade em banda — é outra porta (M5)

O AMQP negocia TLS na mesma porta em alguns cenários, e muitos protocolos têm
`STARTTLS`. **O Redis não tem nenhum dos dois:** o servidor abre um listener
separado (`--tls-port`), e a conexão nasce cifrada ou não nasce.

Isso tem duas consequências que a lib assume explicitamente:

- **`UseTls` quase nunca vem sozinho.** Ligar o TLS sem trocar a porta manda um
  ClientHello para o listener de texto claro, que o lê como comando inline e
  nunca responde. É por isso que existe `RedisDefaultTlsParams`: ele muda as
  duas coisas juntas, que é o único jeito de acertar.
- **O sintoma desse erro é um timeout, não um erro de criptografia.** Quem
  desata o nó é o `SO_RCVTIMEO` que o M3 instalou — sem ele a thread ficaria
  pendurada para sempre no handshake. A `Redis.Connection` traduz esse
  `ERedisTransportTimeout` para `ERedisTimeout` com uma mensagem que **nomeia a
  troca de porta como causa provável**, porque a pista natural leva ao lugar
  errado: o usuário vai investigar certificado quando o que errou foi o número
  da porta.

O caso simétrico — conexão em texto claro contra o listener TLS — **não** é
detectado no `Open`, e isso é uma escolha. Em RESP2 sem senha, sem `CLIENT
SETNAME` e no banco 0 o handshake não emite um byte sequer, então não há nada
que possa dar errado ainda; a falha aparece no primeiro comando, como
`ERedisConnectionLost`. Detectar antes exigiria um `PING` em **toda** conexão
aberta pelo pool, e um round-trip por conexão é caro demais para diagnosticar um
erro de configuração que acontece uma vez.

## 32. A escolha insegura precisa de uma linha explícita (M5)

`TlsVerifyPeer := False` aceita qualquer certificado: mantém a cifragem do canal
e joga fora a defesa contra man-in-the-middle. É necessário em desenvolvimento
(o `docker-compose.tls.yml` usa self-signed) e indefensável em produção.

A tentação seria um atalho — `RedisLocalhostTlsParams`, ou um
`RedisDefaultTlsParams` que já viesse com a validação desligada, "porque é o que
todo mundo usa para testar". A lib **não** oferece isso. `RedisDefaultTlsParams`
troca a porta e liga o TLS, e mantém `TlsVerifyPeer := True`; quem precisa
baixar a validação escreve a linha:

```pascal
LParams := RedisDefaultTlsParams;
LParams.TlsVerifyPeer := False;   // só em desenvolvimento
```

O raciocínio é de revisão de código, não de ergonomia: essa linha aparece no
diff de quem a escreveu e num `grep` de quem for auditar. Escondida atrás de um
nome de função amigável, ela atravessaria para produção sem ninguém notar — é
assim que a maior parte dos clientes acaba em produção sem validar certificado.
Há um teste unitário dedicado a travar essa decisão (`DefaultTlsParams_
MantemAVerificacaoLigada`), justamente para que uma "melhoria de conveniência"
futura esbarre num teste vermelho.

## 33. O backend TLS é escolhido em compilação, e o build diz qual (M5)

`Redis.Connection` referencia **uma** unit de TLS, decidida por diretiva:
`REDIS_OPENSSL` tem precedência sobre `REDIS_WINDOWS`, e sem nenhum dos dois não
há unit alguma. Não há seleção em runtime, e é de propósito: um cliente que
"tenta OpenSSL e cai para SChannel" acaba usando um backend diferente do que foi
testado, na máquina do cliente, sem avisar.

Duas consequências:

- **Build sem backend** (fora do Windows, sem a diretiva) não cai para texto
  claro: `UseTls` levanta `ERedisTls` explicando como habilitar. Abrir em texto
  claro uma conexão que o chamador pediu cifrada seria vazar a senha do `AUTH`
  no fio.
- **`RedisTlsBackendName`/`RedisTlsBackendInfo` são parte da API**, não
  decoração. O `Info` acrescenta o detalhe de runtime que só o OpenSSL tem:
  versão e o caminho da biblioteca que realmente carregou — a informação que
  resolve o dia em que a máquina tem três OpenSSL instalados e o errado venceu.
  O SChannel não tem equivalente porque não há o que escolher: é o próprio
  Windows.

  Esse cenário deixou de ser hipotético na própria validação do M5: na mesma
  máquina, o build FPC x64 carregou **OpenSSL 3.2.4** (`libssl-3-x64.dll`, vinda
  do PATH) e o build Delphi Win32 carregou **OpenSSL 3.5.2** (`libssl-3.dll`, de
  `SysWOW64`). Os dois passaram — mas se um deles tivesse falhado, sem o `Info`
  a investigação começaria pelo lugar errado.

## 34. `MULTI`/`EXEC` não é rollback, e a lib não finge que é (M6)

A palavra "transação" carrega uma promessa que o Redis não faz. O que o
`MULTI`/`EXEC` garante é **isolamento**: nenhum outro cliente intercala comando
no meio do bloco. O que ele **não** faz:

- **Não desfaz nada.** Se o terceiro de cinco comandos falhar com `WRONGTYPE`,
  os outros quatro rodaram e ficaram gravados. Não há atomicidade no sentido do
  "A" de ACID.
- **Não deixa ler lá dentro.** Entre `MULTI` e `EXEC` os comandos só são
  enfileirados; nenhum devolve valor. Não dá para ler um saldo e decidir o que
  gravar dentro do bloco.

A tentação seria esconder isso: fazer o `Commit` levantar quando qualquer item
falha, para parecer uma transação de banco. Seria pior do que inútil — o
chamador concluiria que nada foi gravado, quando na verdade quase tudo foi.
Então `Commit` **não levanta** por erro de execução: devolve o array e o item
que falhou vem como `rkError`, exatamente como no pipeline. Quem precisa de
tudo-ou-nada de verdade usa um script Lua (decisão 36), que o servidor executa
como uma unidade.

O que `Commit` **levanta** é outra coisa: comando que o servidor se recusou a
*enfileirar* (inexistente, aridade errada). Aí o `EXEC` inteiro aborta com
`EXECABORT` e nada rodou — e a exceção cita **qual** comando estava torto, que é
a informação que o `EXECABORT` sozinho não dá.

## 35. O bloco inteiro sai num pipeline só (M6)

A implementação ingênua manda `MULTI`, espera `+OK`, manda cada comando,
espera `+QUEUED` de cada um, e só então manda `EXEC`: doze idas e voltas para
dez comandos. Aqui os comandos são acumulados **localmente** e o bloco sai numa
escrita só — uma ida e volta, reusando o `TRedisPipeline` do M2, que já sabe
escrever N comandos e ler N respostas.

Três consequências que aparecem na API:

1. **`Discard` quase nunca vai ao fio.** Se o commit não aconteceu, o `MULTI`
   também não saiu, e não há o que descartar no servidor. O que o `Discard`
   *precisa* mandar é o `UNWATCH`.
2. **Enfileirar não valida nada.** Um comando inexistente só é recusado quando o
   lote chega ao servidor. Em troca, `Queue` nunca bloqueia e nunca falha.
3. **Bloco vazio VAI ao servidor** — ao contrário do pipeline vazio, que o M2
   decidiu não enviar. Sob `WATCH`, um `MULTI`/`EXEC` sem comando nenhum é a
   pergunta legítima "alguém mexeu no que eu vigiava?", e a resposta só existe
   do outro lado.

E uma que aparece no destrutor: **`WATCH` é estado da conexão**, não do bloco.
Uma conexão devolvida ao pool com `WATCH` pendente faria o `EXEC` do *próximo*
usuário abortar sem motivo aparente — e ele não teria como descobrir por quê. É
a mesma família do bug de conexão suja da decisão 18, e a resposta é a mesma:
limpar antes de devolver. Por isso o destrutor da transação manda `UNWATCH`
quando havia vigilância e não houve commit, engolindo falha (a conexão pode já
estar morta, e o pool vai destruí-la de qualquer forma).

É também a razão de a transação segurar uma **conexão inteira** enquanto vive: o
`WATCH` não sobrevive a uma troca de conexão, e sem afinidade o check-and-set
não existiria. `TRedisClient.BeginTransaction` empresta do pool e devolve no
destrutor — daí o `try/finally` não ser opcional.

## 36. O SHA do script é calculado aqui, sobre bytes, e o cache se autocorrige (M6)

`EVALSHA` troca kilobytes de Lua por 40 bytes. Para usá-lo é preciso saber o
SHA-1 do script, e há dois jeitos: perguntar ao servidor (`SCRIPT LOAD`) ou
calcular. A lib **calcula**, e isso muda o custo do primeiro uso: sem cálculo
local, aquecer um script custaria um round-trip a mais; com ele, a primeira
chamada é um `EVAL` normal — o servidor executa **e** guarda de uma vez.

Duas escolhas dentro dessa:

- **O digest é dos BYTES UTF-8, não da `string`.** No FPC a `string` carrega
  codepage dinâmico; hashear a representação local daria um SHA diferente do que
  o servidor calcula assim que houvesse um acento no script (num comentário Lua,
  por exemplo). O sintoma seria cruel: `EVALSHA` respondendo `NOSCRIPT` para
  sempre, com o cache "funcionando" e nunca acertando. Há um teste unitário com
  vetor de referência calculado fora da lib exatamente para travar isso.
- **`NOSCRIPT` não é erro, é recado.** O cache do servidor pode sumir por
  `SCRIPT FLUSH`, restart ou failover para uma réplica que nunca viu o script.
  Quando isso acontece, `Run` reenvia o `EVAL` sozinha e quem chamou não vê erro
  nenhum. É o que torna o cache uma otimização que **se autocorrige**, e não uma
  suposição sobre o estado do servidor. Qualquer outro erro — inclusive erro do
  próprio Lua — sobe: reenviar só repetiria a falha e dobraria o tráfego.

O cache vive no **cliente**, não na conexão: todas as conexões do pool falam com
o mesmo servidor, e o cache de scripts é dele. Por isso é protegido por lock —
várias threads podem estar estreando o mesmo script ao mesmo tempo.

---

## 37. O kernel ganhou um modo full-duplex — e ele falha de outro jeito (M7)

Até o M6 a conexão só sabia fazer uma coisa: escrever um comando e ler a
resposta dele, na mesma thread, sob o lock. Pub/sub não cabe nisso — o servidor
fala sozinho —, então `TRedisConnection` ganhou duas rotinas que quebram o par
de propósito:

- **`Send`** escreve e **não** lê. Pega o lock, como o `Execute`.
- **`Receive`** lê **sem** ter escrito, e **não pega o lock**. Se pegasse, um
  canal em silêncio (o estado normal de um assinante) impediria qualquer
  `SUBSCRIBE` novo de sair, porque a thread de leitura estaria segurando o lock
  parada num `recv`.

Duas invariantes do kernel mudam **só nesse modo**, e as duas por causa da
segunda thread:

- **Sobra no buffer não é conexão suja.** No modo pergunta-resposta, byte
  sobrando depois da resposta significa contaminação (seção 18). Em pub/sub
  significa que chegaram duas mensagens numa leitura só — o funcionamento
  normal. A checagem de `IsDirty` fica desligada ali.
- **A falha não faz faxina.** `MarkBroken` libera reader, stream e socket, e
  isso é seguro quando existe uma thread só. Com duas, liberar puxaria o tapete
  de quem estivesse no meio de um `Send`. No modo full-duplex a falha apenas
  marca a conexão e **fecha o socket** — que é o que desbloqueia a leitura
  pendurada da outra thread. É a mesma escolha do `Abort` (seção 19), pelo mesmo
  motivo. A liberação de verdade acontece no `Close`/destrutor, depois que a
  thread de leitura terminou.

Misturar `Execute` com `Send`/`Receive` na mesma conexão dessincroniza o fluxo —
o `Execute` leria a próxima mensagem publicada achando que é a resposta dele. É
por isso que essas rotinas existem para uma unidade só: a `Redis.PubSub`.

---

## 38. A conexão de pub/sub não tem read timeout, e quem a desbloqueia é o `Abort` (M7)

Um canal pode ficar horas sem publicar nada. Se a conexão do assinante tivesse
`ReceiveTimeoutMs`, o silêncio viraria `ERedisTimeout` e a conexão seria
descartada — e com razão, porque no modo pergunta-resposta timeout significa
"desisti, mas a resposta ainda vem a caminho" (seção 22). Só que aqui não há
resposta a caminho: não houve pergunta. Então o assinante **zera** o read
timeout ao abrir a sua conexão.

Isso transfere o problema do relógio para o `Stop`: sem timeout, a thread fica
parada no `recv` para sempre. Quem a acorda é o `Abort`, que derruba o socket
sem pegar o lock — exatamente o caso de uso para o qual ele foi escrito no M2. A
sequência do `Stop` é sinalizar, abortar, esperar a thread morrer e só então
liberar a conexão.

**O que fica em aberto, e é limitação conhecida:** conexão que morre em silêncio
(cabo arrancado, NAT que esqueceu a sessão) não é detectada até o TCP desistir,
porque o Redis não tem heartbeat como o AMQP. Quem precisa detectar mais cedo
chama `TRedisSubscriber.Ping` de um timer da aplicação — é uma linha, e é
melhor do que a lib decidir sozinha um intervalo que serve para todo mundo.

---

## 39. O callback roda na thread de leitura, em ordem (M7)

O `OnMessage` é chamado **na thread de leitura**, uma mensagem por vez, na ordem
em que chegaram. A alternativa era despachar por `RedisPool` (o thread pool que
a lib já tem) e ganhar vazão. Não foi escolhida:

- **Ordem é o único compromisso que o pub/sub do Redis realmente cumpre.** Não
  há entrega garantida, não há confirmação, não há repetição — mas o que chega,
  chega na ordem em que foi publicado. Espalhar as mensagens por N workers
  entregaria "3, 1, 2" sem aviso nenhum, e um bug desses aparece em produção,
  nunca no teste.
- A vazão que se ganharia é a de um cliente que não dá conta de ler o próprio
  socket — cenário em que o remédio certo é a aplicação enfileirar, e não a lib
  paralelizar por ela.

O preço está documentado no contrato do `OnMessage`: **callback lento segura o
socket**. Trabalho pesado deve ir para uma fila da aplicação.

Duas consequências que a lib trata sozinha:

- **Exceção do callback não derruba a conexão.** Ela vai para o `OnError` e a
  próxima mensagem é entregue normalmente. Deixar subir transformaria todo bug
  da aplicação numa reconexão.
- **`Execute` de dentro do callback é recusado na hora**, com mensagem
  explicando. Quem leria a resposta é justamente a thread que está rodando o
  callback: sem essa guarda, a chamada travaria até o `CommandTimeoutMs` e o
  diagnóstico seria "o Redis está lento".

---

## 40. `Subscribe` espera a confirmação do servidor (M7)

`SUBSCRIBE` é assíncrono no protocolo: o comando vai, e a confirmação volta pelo
mesmo fluxo das mensagens. A lib podia devolver na hora e deixar a confirmação
chegar quando chegasse — mas aí este teste (e o código de qualquer aplicação
que publica logo depois de assinar) seria uma corrida disfarçada:

```pascal
LSub.Subscribe(['noticias']);
LClient.PubSub.Publish('noticias', 'oi');   // chega? depende do escalonador
```

Então `Subscribe` **espera** a confirmação, e o que ele espera é o **estado**
(o canal aparecer na lista confirmada), não uma contagem de mensagens — assim o
resultado não depende de quantas confirmações chegaram nem da ordem delas.

Duas exceções, as duas por impossibilidade e não por conveniência:

- **De dentro do callback não espera.** Quem confirmaria é a thread que está
  rodando o callback. A assinatura sai, e a confirmação chega adiante no laço.
- **Com a conexão caída e `AutoReconnect` ligado, registra e volta.** A
  assinatura fica na lista de desejadas e vai ao fio quando a conexão voltar.
  Com `AutoReconnect` desligado, levanta.

A lib mantém duas listas por tipo de assinatura: o que a **aplicação pediu** (é
o que a reconexão reenvia) e o que o **servidor confirmou** (é o que
`Channels`/`Patterns`/`ShardChannels` devolvem, e o que a queda zera). Uma lista
só não daria conta: ao cair, ela teria de ser esvaziada — e aí não sobraria nada
para replayar.

---

## 41. Em RESP2, a lib recusa o comando que o servidor recusaria (M7)

Numa conexão RESP2 com assinatura ativa, o Redis só aceita os comandos de
assinatura, `PING`, `RESET` e `QUIT`. Qualquer outro responde um erro que fala
do protocolo, não do que fazer a respeito. O `TRedisSubscriber.Execute` recusa
**antes de ir ao fio**, com uma mensagem que nomeia a saída: usar outra conexão
(o pool do `TRedisClient`) ou negociar RESP3.

Isso vale só quando há assinatura ativa — antes do primeiro `SUBSCRIBE` a
conexão RESP2 ainda é comum, e é o que permite ler o `CLIENT ID` do assinante
antes de assinar (o teste de reconexão faz exatamente isso).

Em **RESP3** não há restrição nenhuma: a mensagem chega com tipo próprio
(push, `>`) e a conexão continua servindo comando comum. É o ganho concreto do
`HELLO 3` aqui, e o que separa os dois mundos na thread de leitura é uma linha:
em RESP3, **só** o que vem como push é tráfego de pub/sub; o resto é resposta
de comando e vai para quem estiver esperando.

Em RESP2 não existe essa marca, e a classificação é pela forma do array. Três
perguntas, não uma: o verbo bate, a aridade bate, e — nas confirmações — o
terceiro item é mesmo um inteiro. A terceira existe por um caso real: um
`PUBSUB CHANNELS` pode devolver três canais, o primeiro chamado `subscribe`.
Sem a checagem do inteiro, isso viraria uma confirmação fantasma.

---

## 42. A reconexão replaya as assinaturas — e só elas (M7)

Assinatura é o **único** estado que o pub/sub deixa no servidor, e por isso é a
única coisa que a reconexão do assinante refaz (além do handshake:
`HELLO`/`AUTH`, `CLIENT SETNAME`, `SELECT`). Não há comando em voo para repetir
— e continua valendo a decisão do M2 de não repetir nada.

O que a reconexão **não** recupera é a mensagem publicada enquanto a conexão
esteve fora: ela se perdeu, ponto. Pub/sub é fire-and-forget, o `PUBLISH`
devolve quantos assinantes receberam **naquele instante**, e zero significa que
a mensagem evaporou — não que ficou guardada esperando alguém assinar. Quem
precisa de entrega garantida usa Streams com consumer group (M8). A lib não
tenta suavizar isso com fila local: uma fila no cliente daria a impressão de
durabilidade que o servidor não oferece.

Um assinante criado sobre uma conexão pronta (`CreateOnConnection`) **não**
reconecta, e o `AutoReconnect` dele nasce False: aquela conexão pode nem ser de
socket — as suítes de teste passam uma sobre `TStream` —, e reabrir por conta
própria sairia conectando em outro lugar.

---

## 43. Entrada sem campos é `nil`, e não dicionário vazio (M8)

`XDEL` (e o trim) tira a entrada do **stream**, mas não da **PEL** do grupo. Quem
relê a PEL — `XREADGROUP` com `'0'`, `XCLAIM`, `XAUTOCLAIM` — alcança ids que já
não existem, e o servidor responde a entrada com o id preenchido e os campos
**nulos**.

Três saídas eram possíveis, e a escolha muda o que o chamador precisa escrever:

- **Levantar.** Errado: isso não é anomalia, é o estado normal de uma pendência
  sobre entrada apagada. Levantar quebraria justamente a varredura de PEL, que é
  a rotina de recuperação de quem perdeu um worker — o pior momento para uma
  exceção nova.
- **Dicionário vazio.** Apaga a diferença entre "apagada do stream" e "gravada
  sem campo" — e a segunda o Redis nem permite, porque `XADD` sem campo é erro de
  sintaxe. Seria inventar um estado que o servidor não produz.
- **`Fields = nil`, com `IsDeleted` para lê-lo por extenso.** É a escolhida. Um
  teste só, e o mesmo `FieldValue` continua devolvendo `''` sem estourar.

É a mesma regra que o M1 fixou para `rkNull`: nulo e vazio são coisas
diferentes, e rebaixar um no outro esconde informação em silêncio.

---

## 44. `XREAD` absorve a diferença entre RESP2 e RESP3 — e a chave vem junto (M8)

Em RESP2, `XREAD`/`XREADGROUP` respondem uma lista de pares `[chave, entradas]`.
Em RESP3, respondem um **mapa** chave → entradas, que o leitor guarda achatado
(decisão 13). São duas formas para o mesmo dado, e é o mesmo problema do
`WITHSCORES` no M4 — resolvido do mesmo jeito, e pelo mesmo motivo:
`RedisReplyToStreamData` olha o primeiro item (agregado = par do RESP2; escalar =
nome de chave do mapa achatado) e devolve sempre `TRedisStreamDataArray`. A
aplicação não ramifica por protocolo.

O que **não** dava para esconder é mais importante: **chave sem novidade não
aparece na resposta**. Pedir três streams e receber um é o caso normal, não a
exceção — então a estrutura devolvida carrega o `Key` de cada bloco, e existe
`RedisFindStreamData` para procurar pelo nome. Devolver "uma posição por chave
pedida", com buracos, seria mais conveniente de indexar e mentiria sobre o que o
servidor disse; indexar pela posição da chamada lê a chave errada na primeira vez
que uma delas fica quieta.

---

## 45. `BLOCK` fica em milissegundos na API, porque é a unidade do comando (M8)

O `BLPOP` do M4 recebe o prazo em **segundos** (é o que o comando espera, com
fração desde o Redis 6). O `XREAD BLOCK` recebe em **milissegundos**. É uma
inconsistência do Redis, não da lib.

A tentação era uniformizar — expor tudo em segundos e multiplicar por mil por
baixo. Não foi feito, e a regra é: **a assinatura pública usa a unidade do
comando que ela representa**. Quem lê `XReadBlocking(..., 5000)` com a
documentação do Redis ao lado vê o mesmo número nos dois lugares. Uniformizar
faria `XReadBlocking(..., 5)` significar cinco segundos aqui e cinco
milissegundos no `redis-cli` — e um erro de fator 1000 não estoura, só espera o
tempo errado, que é o tipo de bug que ninguém encontra lendo o código.

A conversão para segundos existe num lugar só, na chamada a `ExecuteBlocking`
(que fala segundos porque nasceu para o `BLPOP`), com o comentário explicando por
quê. O teste de integração exercita o prazo de verdade contra um read timeout
menor, exatamente como o do `BLPOP`, porque um erro de unidade passaria
despercebido num teste que só olha os bytes do comando.

---

## 46. `BUSYGROUP` é recado, não falha — mas só ele (M8)

Todo worker de um consumer group precisa garantir que o grupo existe antes de
ler. O jeito honesto é `XGROUP CREATE ... MKSTREAM` na subida — e o segundo
worker recebe `BUSYGROUP`, que é o servidor dizendo "já está do jeito que você
queria".

`XGroupCreate` levanta, como qualquer erro de servidor (decisão 17). Ao lado
dela, `XGroupTryCreate` trata **`BUSYGROUP` e só ele** como resposta, devolvendo
False. É a mesma forma do `NOSCRIPT` no M6: um erro que a lib entende como estado
esperado, e cuja tradução em código do chamador seria sempre a mesma linha.

O "e só ele" é a parte que importa. Engolir qualquer erro transformaria um
`WRONGTYPE` (a chave existe e é uma string) em "o grupo já existe", e o worker
seguiria em frente para falhar adiante, longe da causa. Há teste unitário nos
dois sentidos.

---

## 47. `XAUTOCLAIM` aceita resposta de dois e de três itens (M8)

O `XAUTOCLAIM` do Redis 6.2 responde `[cursor, entradas]`. O 7.0 acrescentou um
terceiro elemento: os ids que estavam na PEL e já não existem no stream (o
servidor os remove sozinho; a lista é só o registro).

O leitor aceita os dois tamanhos, e a sobrecarga que entrega os apagados
simplesmente devolve lista vazia contra um servidor 6.2. A alternativa — exigir
três itens — quebraria a lib num servidor perfeitamente capaz de rodar tudo o
mais, e o sintoma seria um erro de tipo no meio da rotina de recuperação.

Não há detecção de versão em nenhum ponto da lib, e não é para haver: **a forma
da resposta é a informação**, e ela chega junto com o dado. Perguntar
`INFO server` para decidir como ler a próxima resposta seria uma ida a mais e um
estado a mais para envelhecer.

---

## 48. Os testes de stream voltam ao servidor falso roteirizado (M8)

O M7 introduziu o servidor falso que **responde** — interpreta o que o cliente
escreveu e mantém estado —, porque sem diálogo não havia como testar confirmação
de assinatura nem ordem de mensagens. Era natural supor que `XREADGROUP`/`XACK`
pediriam o mesmo.

Não pedem. O que há de errar numa fachada de streams é (a) a montagem do comando
— `MAXLEN ~` antes do id, `STREAMS` como último modificador, `IDLE` antes da
faixa, `BLOCK` em milissegundos — e (b) a leitura de respostas de forma incomum:
mapa do RESP3, entrada sem campos, `XAUTOCLAIM` sem a terceira parte. As duas
coisas o roteiro fixo verifica melhor, porque **o teste escolhe a resposta**,
inclusive as que um servidor real quase nunca produz sob demanda.

O que exige um servidor de verdade — a PEL trocando de dono, o grupo repartindo o
trabalho entre dois consumidores, o `XCLAIM` que não rouba trabalho recente —
está na suíte de integração, onde a semântica é do Redis e não de um fake que eu
escrevi para concordar comigo.

---

## Compatibilidade e nomenclatura

A lib fala RESP, não depende da implementação: funciona com **Redis**, **Valkey**, **KeyDB**
e **Dragonfly**. Testar contra o Valkey além do Redis é diferencial barato e isola o projeto
das mudanças de licença do servidor Redis (7.4 passou a RSALv2/SSPL; a 8.0 acrescentou
AGPLv3 como opção) — que afetam a imagem Docker usada nos testes, nunca o cliente.

"Redis" no nome do projeto identifica o protocolo, como é praxe em clientes. O README traz
o aviso de não-afiliação.
