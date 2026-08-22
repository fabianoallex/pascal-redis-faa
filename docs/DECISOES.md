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

## Compatibilidade e nomenclatura

A lib fala RESP, não depende da implementação: funciona com **Redis**, **Valkey**, **KeyDB**
e **Dragonfly**. Testar contra o Valkey além do Redis é diferencial barato e isola o projeto
das mudanças de licença do servidor Redis (7.4 passou a RSALv2/SSPL; a 8.0 acrescentou
AGPLv3 como opção) — que afetam a imagem Docker usada nos testes, nunca o cliente.

"Redis" no nome do projeto identifica o protocolo, como é praxe em clientes. O README traz
o aviso de não-afiliação.
