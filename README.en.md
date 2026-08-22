# pascal-redis-faa

A **Redis** client (RESP2/RESP3 protocol) for **Free Pascal/Lazarus and Delphi**,
from a single codebase, written from scratch. MIT licensed.

> **Status: under construction (M0 — skeleton).** The library currently ships only
> the transport and concurrency layers. The full roadmap lives in `CLAUDE.md`
> (Portuguese).

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

No server needed: the RESP codec is exercised over an in-memory byte source that
hands the reply over in chunks of a controlled size, reproducing partial network
reads.

**FPC/Lazarus (FPCUnit):**

```
lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi
tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain
```

With no parameters the executable opens the GUI with the test tree.

**Delphi (DUnitX):** open `Redis.groupproj` and build `Redis.UnitTests`.

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
