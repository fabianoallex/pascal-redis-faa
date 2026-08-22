# pascal-redis-faa

Cliente **Redis** (protocolo RESP2/RESP3) para **Free Pascal/Lazarus e Delphi**, numa
única codebase, escrito do zero. Licença MIT.

> **Status: em construção (M3 — pool, timeouts e reconexão).** Já dá para conectar,
> autenticar, executar **qualquer** comando do Redis, usar pipeline e trabalhar com um
> pool de conexões com timeout de verdade, em RESP2 ou RESP3. Ainda faltam as fachadas
> tipadas por família (M4), TLS (M5), transações (M6), pub/sub (M7) e streams (M8). O
> roadmap completo está em `CLAUDE.md`.

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

Comandos bloqueantes (`BLPOP`, `BRPOP`, `XREAD BLOCK`) precisam de um timeout maior que o do
próprio comando — use `TRedisConnection.SetReceiveTimeout` numa conexão fora do pool.

`Execute` alcança qualquer comando desde já — as fachadas tipadas por família (M4) serão
uma camada por cima, nunca um pré-requisito.

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
esse mesmo servidor falso.

**FPC/Lazarus (FPCUnit):**

```
lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi
tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain
```

**Delphi (DUnitX):** abrir `Redis.groupproj` e compilar `Redis.UnitTests`.

## Testes de integração

Precisam do container de pé (seção "Servidor de desenvolvimento"). Cobrem o que não dá para
verificar em memória: read timeout de socket de verdade, conexão derrubada pelo servidor
(`CLIENT KILL`), conexão contaminada por resposta atrasada e várias threads dividindo o
mesmo pool.

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
