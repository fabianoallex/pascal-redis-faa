# pascal-redis-faa

A **Redis** client (RESP2/RESP3 protocol) for **Free Pascal/Lazarus and Delphi**,
from a single codebase, written from scratch. MIT licensed.

> **Status: under construction (M3 — pool, timeouts and reconnection).** You can already
> connect, authenticate, run **any** Redis command, use pipelining and work through a
> connection pool with real timeouts, over RESP2 or RESP3. Still missing: the typed
> per-family facades (M4), TLS (M5), transactions (M6), pub/sub (M7) and streams (M8).
> The full roadmap lives in `CLAUDE.md` (Portuguese).

Sibling of [`pascal-amqp-faa`](../pascal-amqp-faa) (AMQP 0-9-1 client) and
`pascal-pipes-faa` (IPC), under the same rules: dual codebase for FPC 3.2.2 and
Delphi 12, no external dependencies, native TLS on Windows (SChannel) and opt-in
OpenSSL on any platform.

## What v1 will contain

| Area | Contents |
|---|---|
| Core | RESP2/RESP3 codec, connection, connection pool, timeouts, reconnection |
| Commands | Keys, Strings, Hashes, Lists, Sets, ZSets, Server |
| Advanced | pipelining, `MULTI`/`EXEC`/`WATCH`, scripting (`EVAL`/`EVALSHA` with SHA cache) |
| Messaging | Pub/Sub (dedicated connection) and Streams with consumer groups |
| Security | TLS via SChannel (Windows) or OpenSSL (`-dREDIS_OPENSSL`, any platform) |

**Out of scope for v1:** Redis Cluster (`MOVED`/`ASK` redirects), Sentinel and
client-side caching (`CLIENT TRACKING`).

## Server compatibility

The library speaks RESP and does not depend on the implementation: it works with
**Redis**, **Valkey**, **KeyDB** and **Dragonfly**. `docker/docker-compose.yml`
uses `redis:7.2-alpine` by default.

This project is not affiliated with Redis Ltd.; "Redis" is a trademark of its
respective holders and is used here only to identify the protocol the library
speaks.

## Development server

```
cd docker
docker compose up -d                    # Redis on localhost:6379
docker compose exec redis redis-cli ping
```

For the TLS listener (6380), generate the certificates as described in
`docker/certs/README.md` and bring it up with the override:

```
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
```

## Usage

```pascal
uses
  SysUtils, Redis.Types, Redis.Connection;

var
  LParams: TRedisParams;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
begin
  LParams := RedisDefaultParams;      // localhost:6379, database 0, RESP2
  LParams.ClientName := 'my-app';     // shows up in the server's CLIENT LIST

  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;

    LConn.Execute('SET', ['user:1:name', 'Fabiano', 'EX', 3600]);
    WriteLn(LConn.Execute('GET', ['user:1:name']).AsString);

    // A missing key returns null — which is NOT the same as an empty string.
    if LConn.Execute('GET', ['user:2:name']).IsNull then
      WriteLn('does not exist');

    // Pipeline: N commands in a single round-trip.
    LPipe := TRedisPipeline.Create;
    try
      LPipe.Queue('INCR', ['visits']);
      LPipe.Queue('LPUSH', ['events', 'login']);
      LPipe.Queue('LLEN', ['events']);
      LReplies := LConn.ExecutePipeline(LPipe);
      WriteLn('visits: ', LReplies[0].AsInteger);
    finally
      LPipe.Free;
    end;
  finally
    LConn.Free;   // Close also closes it; the destructor covers the exception path
  end;
end.
```

### Connection pool

Redis has no channel: one connection processes one command at a time. The unit of
concurrency is the connection, and what the application holds is the **pool**.

```pascal
uses
  Redis.Types, Redis.Connection, Redis.Pool;

var
  LPool: TRedisPool;
  LConn: TRedisConnection;
begin
  LPool := TRedisPool.Create(RedisDefaultParams);   // cap of 10 connections
  try
    LConn := LPool.Acquire;
    try
      LConn.Execute('INCR', ['visits']);
    finally
      LPool.Release(LConn);   // ALWAYS release, exceptions included
    end;
  finally
    LPool.Free;
  end;
end;
```

The pool discards (rather than recycles) any connection that comes back invalidated,
**dirty** — bytes left over in the buffer, which would contaminate the next command — or
with the database switched by a `SELECT`. A connection idle for longer than
`HealthCheckAfterIdleMs` gets a `PING` before being handed out, because the one who drops
idle connections is the server, and the client is never told. At the cap, `Acquire` waits
for a release and then raises `ERedisPoolExhausted`, instead of opening sockets without
limit until the server turns everyone away.

There is no "resuming" a dead connection: what the pool does is open another one, and its
handshake replays `HELLO`/`AUTH`, `CLIENT SETNAME` and `SELECT`. The in-flight command is
**not** retried — `INCR` and `LPUSH` are not idempotent.

### Timeouts

`ReceiveTimeoutMs` and `SendTimeoutMs` (5 s by default) become `SO_RCVTIMEO`/`SO_SNDTIMEO`
on the socket. Once the deadline passes, the command raises `ERedisTimeout` **and the
connection is invalidated** — the late reply may still arrive, and recycling that connection
would hand that reply to the next command. Without this timeout, a server that goes silent
holds the connection and the calling thread forever: Redis has no heartbeat.

Blocking commands (`BLPOP`, `BRPOP`, `XREAD BLOCK`) need a timeout larger than the command's
own — use `TRedisConnection.SetReceiveTimeout` on a connection kept outside the pool.

`Execute` reaches any command already — the typed per-family facades (M4) will be a layer
on top, never a prerequisite.

**Binary-safe by contract.** A `TBytes` argument goes to the wire byte for byte and
`AsBytes` gives the raw value back; the `string` overloads go through UTF-8. A value with
CRLF, a zero byte or 0xFF in the middle survives the round-trip.

**Errors.** A server error (`WRONGTYPE`, `NOSCRIPT`, `NOAUTH`…) raises `ERedisReplyError`,
which carries a `Code` ready to be tested, and the connection stays usable. An I/O error
raises `ERedisConnectionLost` and **invalidates** the connection: the in-flight command is
not re-executed, because `INCR`, `LPUSH` and `SETNX` are not idempotent — retrying is the
caller's call. `ExecuteRaw` returns the error as a reply instead of raising, and a pipeline
never raises on a server error (any item may be an error).

**RESP3** is opt-in: `LParams.Protocol := rpRESP3` performs the handshake with `HELLO 3`.
Application code does not change — an `HGETALL` has the same shape under both protocols,
because the RESP3 map is stored flattened.

## Build

**FPC (command line):**

```
fpc -Fusrc -Fisrc -FEbuild -FUbuild samples\SmokeTest\SmokeTest.dpr
```

**Lazarus:** `lazbuild packages\pascal_redis_faa.lpk`, then the projects; the
`openssl` build mode (`lazbuild -B --build-mode=openssl <proj>.lpi`) switches the
TLS backend.

**Delphi:** open `Redis.groupproj` in the IDE (the Community Edition cannot
compile from the command line).

## Unit tests

No server needed. The RESP codec is exercised over an in-memory byte source that
hands the reply over in chunks of a controlled size, reproducing partial network
reads; the whole connection (handshake, `Execute`, pipelining, invalidation) runs
against a fake server, also in memory. The pool logic (cap, reuse, discard, idle
pruning, failing health check) runs against that same fake server.

**FPC/Lazarus (FPCUnit):**

```
lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi
tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain
```

**Delphi (DUnitX):** open `Redis.groupproj` and build `Redis.UnitTests`.

## Integration tests

These need the container up (see "Development server"). They cover what cannot be checked
in memory: a real socket read timeout, a connection dropped by the server (`CLIENT KILL`),
a connection contaminated by a late reply, and several threads sharing one pool.

```
lazbuild -B tests\Integration\fpc\RedisIntegrationTestsFpc.lpi
tests\Integration\fpc\RedisIntegrationTestsFpc.exe --all --format=plain
```

On Delphi, open `Redis.groupproj` and build `Redis.IntegrationSuite`.

With no parameters the executables open the GUI with the test tree.

Both suites have the same coverage and the test bodies are identical — that is
what `tests\Unit\Redis.DUnitXCompat.pas` is for. Every change to one goes into
the other in the same session.

## Smoke test

Needs the container up (previous section).

```
cd samples\SmokeTest
lazbuild SmokeTest.lpi
SmokeTest.exe
```

Exits with code 0 if every step passes.

## License

MIT — Copyright (c) 2026 Fabiano Arndt. See `LICENSE`.
