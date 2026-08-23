# pascal-redis-faa

Cliente **Redis** (protocolo RESP2/RESP3) para **Free Pascal/Lazarus e Delphi**, numa
única codebase, escrito do zero. Licença MIT.

> **Status: em construção (M4 — famílias de comandos).** Já dá para conectar,
> autenticar, executar **qualquer** comando do Redis, usar pipeline, trabalhar com um
> pool de conexões com timeout de verdade e chamar os comandos de Keys, Strings, Hashes,
> Lists, Sets e ZSets pela fachada tipada — inclusive os bloqueantes — em RESP2 ou
> RESP3. Ainda faltam TLS (M5), transações e scripting (M6), pub/sub (M7) e streams
> (M8). O roadmap completo está em `CLAUDE.md`.

Projeto irmão da [`pascal-amqp-faa`](../pascal-amqp-faa) (cliente AMQP 0-9-1) e da
`pascal-pipes-faa` (IPC), com as mesmas regras: codebase dual FPC 3.2.2 + Delphi 12,
sem dependências externas, TLS nativo em Windows (SChannel) e OpenSSL opt-in em
qualquer plataforma.

## O que vai existir no v1

| Área | Conteúdo |
|---|---|
| Núcleo | codec RESP2/RESP3, conexão, pool de conexões, timeouts, reconexão |
| Comandos | Keys, Strings, Hashes, Lists, Sets, ZSets, Server |
| Avançado | pipelining, `MULTI`/`EXEC`/`WATCH`, scripting (`EVAL`/`EVALSHA` com cache de SHA) |
| Mensageria | Pub/Sub (conexão dedicada) e Streams com consumer groups |
| Segurança | TLS via SChannel (Windows) ou OpenSSL (`-dREDIS_OPENSSL`, qualquer plataforma) |

**Fora do v1:** Redis Cluster (redirects `MOVED`/`ASK`), Sentinel e client-side
caching (`CLIENT TRACKING`).

## Compatibilidade de servidor

A lib fala RESP, não depende da implementação: funciona com **Redis**, **Valkey**,
**KeyDB** e **Dragonfly**. O `docker/docker-compose.yml` usa `redis:7.2-alpine` por
padrão.

Este projeto não é afiliado à Redis Ltd.; "Redis" é marca de seus respectivos
detentores e aparece aqui apenas para identificar o protocolo com que a lib fala.

## Servidor de desenvolvimento

```
cd docker
docker compose up -d                    # Redis em localhost:6379
docker compose exec redis redis-cli ping
```

Para o listener TLS (6380), gere os certs conforme `docker/certs/README.md` e suba
com o override:

```
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
```

## Uso

```pascal
uses
  SysUtils, Redis.Types, Redis.Connection;

var
  LParams: TRedisParams;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LRespostas: TRedisReplyArray;
begin
  LParams := RedisDefaultParams;      // localhost:6379, banco 0, RESP2
  LParams.ClientName := 'minha-app';  // aparece no CLIENT LIST do servidor

  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;

    LConn.Execute('SET', ['usuario:1:nome', 'Fabiano', 'EX', 3600]);
    WriteLn(LConn.Execute('GET', ['usuario:1:nome']).AsString);

    // Chave ausente devolve nulo — que NÃO é o mesmo que string vazia.
    if LConn.Execute('GET', ['usuario:2:nome']).IsNull then
      WriteLn('não existe');

    // Pipeline: N comandos numa ida e volta só.
    LPipe := TRedisPipeline.Create;
    try
      LPipe.Queue('INCR', ['visitas']);
      LPipe.Queue('LPUSH', ['eventos', 'login']);
      LPipe.Queue('LLEN', ['eventos']);
      LRespostas := LConn.ExecutePipeline(LPipe);
      WriteLn('visitas: ', LRespostas[0].AsInteger);
    finally
      LPipe.Free;
    end;
  finally
    LConn.Free;   // o Close também fecha; o destrutor cobre o caminho de exceção
  end;
end.
```

### Fachadas tipadas por família

`TRedisClient` é o objeto que a aplicação segura: pool por dentro, comandos tipados por
fora. Não há `Connect` — a conexão abre no primeiro comando.

```pascal
uses
  SysUtils, Redis.Types, Redis.Client, Redis.Commands, Redis.Commands.Strings;

var
  LClient: TRedisClient;
  LOpcoes: TRedisSetOptions;
  LSessao: IRedisReply;
  LTopo: TRedisStringArray;
  LJson, LToken, LValor: string;
begin
  LClient := TRedisClient.Create(RedisDefaultParams);
  try
    LClient.Strings.SetEx('cache:usuario:1', 3600, LJson);

    // Ausente e vazio são coisas diferentes: TryGet devolve a presença.
    if LClient.Strings.TryGet('cache:usuario:1', LValor) then
      WriteLn(LValor);

    // Lock distribuído: SET NX PX devolve nulo quando outro já tem a posse.
    LOpcoes := RedisDefaultSetOptions;
    LOpcoes.Condition := scNotExists;      // NX
    LOpcoes.Expiry := seMilliseconds;      // PX
    LOpcoes.ExpiryValue := 30000;
    if not LClient.Strings.SetWithOptions('lock:pedido:7', LToken, LOpcoes).IsNull then
      WriteLn('a posse é minha');

    LClient.Hashes.HSetMany('sessao:42', ['ip', '10.0.0.1', 'user', 'ana']);
    LSessao := LClient.Hashes.HGetAll('sessao:42');
    WriteLn(LSessao.ValueByKey('user').AsString);

    LClient.ZSets.ZAdd('ranking', 1500, 'fabiano');
    LTopo := LClient.ZSets.ZRevRange('ranking', 0, 9);   // top 10

    LClient.Keys.Expire('sessao:42', 900);
  finally
    LClient.Free;
  end;
end.
```

Cada comando pega uma conexão do pool e a devolve antes de retornar, o que torna o mesmo
`TRedisClient` seguro para várias threads. Quando a sequência **precisa** da mesma conexão
(`SELECT`, e mais adiante `MULTI`/`WATCH`), amarre um cliente a uma conexão emprestada:

```pascal
LConn := LClient.Acquire;
try
  LDedicado := TRedisClient.CreateOnConnection(LConn);
  try
    ...            // tudo pela MESMA conexão
  finally
    LDedicado.Free;
  end;
finally
  LClient.Release(LConn);
end;
```

Convenção de retorno, válida em todas as famílias: escalar vira tipo nativo (`Boolean`,
`Int64`, `Double`, `string`); resposta que **pode ser nula** vira `IRedisReply` ou tem um
par `TryXxx` com `out`, porque "chave ausente" e "chave que vale zero" não podem virar o
mesmo valor; lista vira `TRedisStringArray`. Chaves, campos e valores entram como
`TRedisArg`, que aceita `string` e `TBytes` na **mesma** assinatura — a API é
binário-segura sem sobrecarga duplicada.

### Comandos bloqueantes

`BLPOP`, `BRPOP` e `BLMOVE` não saem do pool comum: um worker esperando 30 s por uma
tarefa seguraria uma conexão de que as outras threads precisam e, pior, morreria de
timeout de socket antes de o comando terminar — deixando a resposta a caminho para
contaminar a próxima conexão. A fachada os manda por um pool separado, com o read timeout
esticado para além do prazo do comando.

```pascal
// False não é erro: é o worker ocioso. Com várias chaves, LChave diz de qual veio.
while not GParar do
  if LClient.Lists.BLPop(['fila:alta', 'fila:baixa'], 5, LChave, LTarefa) then
    Processa(LChave, LTarefa);
```

Prazo zero significa esperar para sempre — só faz sentido em thread dedicada, porque a
única forma de cancelar é derrubar a conexão.

### Pool de conexões

O Redis não tem canal: uma conexão processa um comando por vez. A unidade de concorrência é
a conexão, e o que a aplicação segura é o **pool**.

```pascal
uses
  Redis.Types, Redis.Connection, Redis.Pool;

var
  LPool: TRedisPool;
  LConn: TRedisConnection;
begin
  LPool := TRedisPool.Create(RedisDefaultParams);   // teto de 10 conexões
  try
    LConn := LPool.Acquire;
    try
      LConn.Execute('INCR', ['visitas']);
    finally
      LPool.Release(LConn);   // devolver SEMPRE, inclusive em exceção
    end;
  finally
    LPool.Free;
  end;
end;
```

O pool descarta (em vez de reciclar) toda conexão que voltar invalidada, **suja** — sobrou
byte no buffer, o que contaminaria o próximo comando — ou com o banco trocado por um
`SELECT`. Conexão parada há mais que `HealthCheckAfterIdleMs` leva um `PING` antes de ser
emprestada, porque quem derruba conexão ociosa é o servidor e o cliente não é avisado. No
teto, `Acquire` espera uma devolução e depois levanta `ERedisPoolExhausted`, em vez de abrir
socket sem limite até o servidor recusar todo mundo.

Não existe "retomar" uma conexão morta: o que o pool faz é abrir outra, e o handshake dela
replaya `HELLO`/`AUTH`, `CLIENT SETNAME` e `SELECT`. O comando que estava em voo **não** é
repetido — `INCR` e `LPUSH` não são idempotentes.

### Timeouts

`ReceiveTimeoutMs` e `SendTimeoutMs` (5 s por padrão) viram `SO_RCVTIMEO`/`SO_SNDTIMEO` no
socket. Estourado o prazo, o comando levanta `ERedisTimeout` **e a conexão é invalidada** —
a resposta atrasada ainda pode chegar, e reciclar essa conexão entregaria essa resposta ao
comando seguinte. Sem esse timeout, um servidor que emudece prende a conexão e a thread que
chamou para sempre: o Redis não tem heartbeat.

Comandos bloqueantes precisam de um timeout maior que o do próprio comando. A fachada já
cuida disso (seção "Comandos bloqueantes"); quem for direto na conexão usa
`TRedisConnection.SetReceiveTimeout` numa conexão fora do pool.

`Execute` alcança qualquer comando, presente ou futuro. As fachadas tipadas por família são
uma camada de conveniência por cima — nunca um pré-requisito, e nunca um limite: um comando
que a lib ainda não modelou continua a uma linha de distância.

**Binário por contrato.** Argumento `TBytes` vai byte a byte para o fio e `AsBytes`
devolve o valor cru; as sobrecargas `string` passam por UTF-8. Um valor com CRLF, zero ou
0xFF no meio sobrevive ao round-trip.

**Erros.** Erro do servidor (`WRONGTYPE`, `NOSCRIPT`, `NOAUTH`…) levanta
`ERedisReplyError`, que traz o `Code` pronto para testar, e a conexão continua utilizável.
Erro de I/O levanta `ERedisConnectionLost` e **invalida** a conexão: o comando em voo não é
reexecutado, porque `INCR`, `LPUSH` e `SETNX` não são idempotentes — a decisão de repetir é
de quem chamou. `ExecuteRaw` devolve o erro como resposta, sem levantar, e o pipeline nunca
levanta por erro de servidor (cada item pode ser um erro).

**RESP3** é opt-in: `LParams.Protocol := rpRESP3` faz o handshake com `HELLO 3`. O código da
aplicação não muda — um `HGETALL` tem a mesma forma nos dois protocolos, porque o mapa do
RESP3 é guardado achatado.

## Build

**FPC (linha de comando):**

```
fpc -Fusrc -Fisrc -FEbuild -FUbuild samples\SmokeTest\SmokeTest.dpr
```

**Lazarus:** `lazbuild packages\pascal_redis_faa.lpk` e depois os projetos; o
build mode `openssl` (`lazbuild -B --build-mode=openssl <proj>.lpi`) troca o
backend TLS.

**Delphi:** abrir `Redis.groupproj` no IDE (a Community Edition não compila por
linha de comando).

## Testes unitários

Não precisam de servidor. O codec RESP é exercitado sobre uma fonte de bytes em
memória, que entrega a resposta em pedaços de tamanho controlado para reproduzir
leituras parciais de rede; a conexão inteira (handshake, `Execute`, pipeline,
invalidação) sobe sobre um servidor falso, também em memória. A lógica do pool
(teto, reuso, descarte, poda por ociosidade, health check reprovando) roda sobre
esse mesmo servidor falso. As fachadas por família são verificadas nas duas metades que
importam: os bytes que foram para o fio (a ordem dos modificadores de `SET`, `ZADD` e
`ZRANGEBYSCORE` não é livre) e a conversão da resposta (nulo que não pode virar `''`,
`WITHSCORES` mudando de forma entre RESP2 e RESP3).

**FPC/Lazarus (FPCUnit):**

```
lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi
tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain
```

**Delphi (DUnitX):** abrir `Redis.groupproj` e compilar `Redis.UnitTests`.

## Testes de integração

Precisam do container de pé (seção "Servidor de desenvolvimento"). Cobrem o que não dá para
verificar em memória: read timeout de socket de verdade, conexão derrubada pelo servidor
(`CLIENT KILL`), conexão contaminada por resposta atrasada, várias threads dividindo o
mesmo pool e uma bateria por família de comandos contra o servidor real — incluindo um
`BLPOP` com prazo maior que o read timeout e a prova de que `HGETALL` e `ZRANGE
WITHSCORES` devolvem o mesmo resultado em RESP2 e em RESP3.

```
lazbuild -B tests\Integration\fpc\RedisIntegrationTestsFpc.lpi
tests\Integration\fpc\RedisIntegrationTestsFpc.exe --all --format=plain
```

No Delphi, abrir `Redis.groupproj` e compilar `Redis.IntegrationSuite`.

Sem parâmetros os executáveis abrem a interface gráfica com a árvore de testes.

As duas suítes têm a mesma cobertura e o corpo dos testes é idêntico — o
`tests\Unit\Redis.DUnitXCompat.pas` existe para isso. Toda mudança em uma vai
para a outra na mesma sessão.

## Smoke test

Precisa do container de pé (seção anterior).

```
cd samples\SmokeTest
lazbuild SmokeTest.lpi
SmokeTest.exe
```

Sai com código 0 se todos os passos passarem.

## Licença

MIT — Copyright (c) 2026 Fabiano Arndt. Ver `LICENSE`.
