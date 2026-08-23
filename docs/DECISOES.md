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

## Compatibilidade e nomenclatura

A lib fala RESP, não depende da implementação: funciona com **Redis**, **Valkey**, **KeyDB**
e **Dragonfly**. Testar contra o Valkey além do Redis é diferencial barato e isola o projeto
das mudanças de licença do servidor Redis (7.4 passou a RSALv2/SSPL; a 8.0 acrescentou
AGPLv3 como opção) — que afetam a imagem Docker usada nos testes, nunca o cliente.

"Redis" no nome do projeto identifica o protocolo, como é praxe em clientes. O README traz
o aviso de não-afiliação.
