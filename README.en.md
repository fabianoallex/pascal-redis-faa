# pascal-redis-faa

A **Redis** client (RESP2/RESP3 protocol) for **Free Pascal/Lazarus and Delphi**,
from a single codebase, written from scratch. MIT licensed.

> **Status: under construction (M6 — transactions and scripting).** You can already
> connect, authenticate, run **any** Redis command, use pipelining, work through a
> connection pool with real timeouts, call the Keys, Strings, Hashes, Lists, Sets and ZSets
> commands through the typed facade — blocking ones included — over RESP2 or RESP3, encrypt
> all of it with TLS, and use `MULTI`/`EXEC`/`WATCH` and Lua scripts with SHA caching.
> Still missing: pub/sub (M7) and streams (M8). The full roadmap lives in `CLAUDE.md`
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

### Typed per-family facades

`TRedisClient` is the object your application holds: a pool on the inside, typed commands
on the outside. There is no `Connect` — the connection opens on the first command.

```pascal
uses
  SysUtils, Redis.Types, Redis.Client, Redis.Commands, Redis.Commands.Strings;

var
  LClient: TRedisClient;
  LOptions: TRedisSetOptions;
  LSession: IRedisReply;
  LTop: TRedisStringArray;
  LJson, LToken, LValue: string;
begin
  LClient := TRedisClient.Create(RedisDefaultParams);
  try
    LClient.Strings.SetEx('cache:user:1', 3600, LJson);

    // Missing and empty are different things: TryGet reports presence.
    if LClient.Strings.TryGet('cache:user:1', LValue) then
      WriteLn(LValue);

    // Distributed lock: SET NX PX returns null when someone else owns it.
    LOptions := RedisDefaultSetOptions;
    LOptions.Condition := scNotExists;      // NX
    LOptions.Expiry := seMilliseconds;      // PX
    LOptions.ExpiryValue := 30000;
    if not LClient.Strings.SetWithOptions('lock:order:7', LToken, LOptions).IsNull then
      WriteLn('the lock is mine');

    LClient.Hashes.HSetMany('session:42', ['ip', '10.0.0.1', 'user', 'ana']);
    LSession := LClient.Hashes.HGetAll('session:42');
    WriteLn(LSession.ValueByKey('user').AsString);

    LClient.ZSets.ZAdd('leaderboard', 1500, 'fabiano');
    LTop := LClient.ZSets.ZRevRange('leaderboard', 0, 9);   // top 10

    LClient.Keys.Expire('session:42', 900);
  finally
    LClient.Free;
  end;
end.
```

Every command takes a connection from the pool and gives it back before returning, which
makes a single `TRedisClient` safe to share across threads. When a sequence **needs** the
same connection (`SELECT`, and later `MULTI`/`WATCH`), bind a client to a borrowed one:

```pascal
LConn := LClient.Acquire;
try
  LDedicated := TRedisClient.CreateOnConnection(LConn);
  try
    ...            // everything over the SAME connection
  finally
    LDedicated.Free;
  end;
finally
  LClient.Release(LConn);
end;
```

Return convention, the same across every family: a scalar becomes a native type
(`Boolean`, `Int64`, `Double`, `string`); a reply that **can be null** becomes an
`IRedisReply` or gets a `TryXxx` pair with an `out`, because "missing key" and "key whose
value is zero" must not collapse into the same value; a list becomes a
`TRedisStringArray`. Keys, fields and values come in as `TRedisArg`, which accepts both
`string` and `TBytes` in the **same** signature — the API is binary-safe without duplicated
overloads.

### Blocking commands

`BLPOP`, `BRPOP` and `BLMOVE` never leave through the shared pool: a worker waiting 30 s
for a task would hold a connection the other threads need and, worse, would die of a socket
timeout before the command finished — leaving the reply in flight to poison the next
connection. The facade routes them through a separate pool, with the read timeout stretched
past the command's own deadline.

```pascal
// False is not an error: it is the idle worker. With several keys, LKey says which one.
while not GStop do
  if LClient.Lists.BLPop(['queue:high', 'queue:low'], 5, LKey, LTask) then
    Process(LKey, LTask);
```

A zero timeout means waiting forever — only sensible on a dedicated thread, since the only
way to cancel it is to tear the connection down.

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

Blocking commands need a timeout larger than the command's own. The facade already handles
that (see "Blocking commands"); going straight at the connection means calling
`TRedisConnection.SetReceiveTimeout` on a connection kept outside the pool.

`Execute` reaches any command, present or future. The typed per-family facades are a
convenience layer on top — never a prerequisite, and never a ceiling: a command the library
has not modelled yet is still one line away.

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

### TLS

The backend is picked at **compile time**, never at runtime — a client that tries one
engine and falls back to another ends up running, on the user's machine, a backend that
was never the one under test:

| Build | Backend | Where |
|---|---|---|
| default | SChannel (SSPI) | Windows only, no DLL to ship |
| `-dREDIS_OPENSSL` | OpenSSL (libssl/libcrypto) | any platform, loaded dynamically |
| neither | — | `UseTls` raises `ERedisTls` explaining how to enable one |

`RedisTlsBackendName` tells you which engine this build carries;
`RedisTlsBackendInfo` adds the runtime detail only OpenSSL can give — version and the
path of the library it actually loaded, which is what settles the day a machine has three
OpenSSL installs and the wrong one won.

```pascal
uses
  Redis.Types, Redis.Client;

var
  LParams: TRedisParams;
begin
  LParams := RedisDefaultTlsParams;   // port 6380 AND UseTls, in one go
  LParams.TlsVerifyPeer := False;     // development only (self-signed cert)

  LClient := TRedisClient.Create(LParams);
```

**Redis TLS is not an in-band upgrade.** There is no `STARTTLS`: the server opens a
separate listener (`--tls-port`), and a connection is either born encrypted or not at all.
That is why `RedisDefaultTlsParams` changes the port *together with* `UseTls` — setting
one without the other sends a ClientHello to the plaintext listener, which reads it as an
inline command and never answers. What saves you there is the read timeout, and the
library raises `ERedisTimeout` **naming the port swap** as the likely cause: the natural
hunch is to go looking at the certificate, and the mistake is the port number.

**`TlsVerifyPeer := False` accepts any certificate** — it keeps the channel encrypted and
throws away the defence against man-in-the-middle. The library offers no shortcut that
comes with verification already off: `RedisDefaultTlsParams` keeps `TlsVerifyPeer := True`,
and whoever needs to lower it writes the line above. That is deliberate — the decision then
shows up in the diff of the person who made it and in an auditor's `grep`, instead of
riding into production hidden behind a friendly function name.

To exercise it against the development server, generate the certs (see
`docker/certs/README.md`), bring the override up and run the encrypted smoke test:

```
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
SmokeTest.exe --tls
```

`--tls` is not a mode with half a dozen extra steps: the **whole** battery starts running
on top of the encryption, which is where a badly built TLS envelope shows up — a 512 KB
bulk spanning several TLS records, a pipeline in a single write, a socket timeout in the
middle of a record.

### Transactions (`MULTI`/`EXEC`/`WATCH`)

The word is misleading: a Redis transaction guarantees that **nobody interleaves a command
in the middle of the block** — and that is all. It undoes nothing when a command fails, and
it lets you read nothing inside. That second gap is what `WATCH` fills, from outside the
block:

```pascal
LTx := LClient.BeginTransaction;      // borrows a connection from the pool
try
  LTx.Watch(['balance']);                                            // watch
  LBalance := LTx.Connection.Execute('GET', ['balance']).AsInteger;  // read (outside)
  if LBalance < 100 then
    Exit;                                                            // decide
  LTx.Queue('SET', ['balance', LBalance - 100]);                     // queue
  if not LTx.TryCommit(LReplies) then
    ; // someone touched 'balance' between WATCH and EXEC: start over
finally
  LTx.Free;                            // returns the connection to the pool
end;
```

`TryCommit` returning **False is not an error** — it is how check-and-set works under
concurrency, and the right answer is almost always to repeat the cycle. (`Commit` exists
for when there is no `WATCH`: it raises `ERedisTransactionAborted` instead.)

**There is no rollback.** If one command in the block fails with `WRONGTYPE`, the others
ran and are stored; the error arrives as an `rkError` item in the reply array and `Commit`
does **not** raise for it. Raising would make you conclude nothing was written, when almost
everything was. For real all-or-nothing, use a Lua script.

The whole block goes out in a **single round-trip** (`MULTI` + commands + `EXEC` as one
pipeline), not N+2. Since `MULTI` only leaves at commit time, `Discard` need not talk to
the server at all — what it does send is `UNWATCH`. And the `try/finally` is not decoration:
while the transaction lives, that connection is out of circulation, because `WATCH` is
**connection** state.

### Scripting (`EVAL`/`EVALSHA`)

A Lua script is the only real atomicity Redis has: it reads, decides and writes in one
pass, and nothing else runs on the server meanwhile — with the obvious flip side that a
**slow script freezes the whole server**.

The example that does not exist without a script is the distributed-lock release:

```pascal
const
  RELEASE =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then' + sLineBreak +
    '  return redis.call("DEL", KEYS[1])' + sLineBreak +
    'else' + sLineBreak +
    '  return 0' + sLineBreak +
    'end';

// 1 when the lock was yours and got released; 0 when it already belonged to someone else.
LClient.Scripting.Run(RELEASE, ['lock:order:7'], [LMyToken]).AsInteger;
```

Doing that with `GET` followed by `DEL` is the classic race: in between, the lock may expire
and be taken by someone else — and the `DEL` would erase **their** lock.

`Run` handles the SHA cache on its own: it computes the SHA-1 locally (no round-trip), sends
`EVAL` on the debut — the server executes **and** stores it — and `EVALSHA` from then on,
trading kilobytes of Lua for 40 bytes. If the server has forgotten the script
(`SCRIPT FLUSH`, a restart, a failover to a replica), it answers `NOSCRIPT` and the library
resends the `EVAL` by itself: the cache is an optimisation that **heals itself**, never an
assumption about server state. `Eval`, `EvalSha`, `ScriptLoad`, `ScriptExists` and
`ScriptFlush` remain available for driving the cycle by hand.

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
pruning, failing health check) runs against that same fake server. The per-family facades
are checked on both halves that matter: the bytes that went to the wire (the order of the
`SET`, `ZADD` and `ZRANGEBYSCORE` modifiers is not free) and the reply conversion (a null
that must not become `''`, `WITHSCORES` changing shape between RESP2 and RESP3).
Transactions and scripting land here too: a null `EXEC`, a command refused while queueing
and `NOSCRIPT` are states the real server rarely produces on demand, and the fake one hands
over for free.

**FPC/Lazarus (FPCUnit):**

```
lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi
tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain
```

**Delphi (DUnitX):** open `Redis.groupproj` and build `Redis.UnitTests`.

## Integration tests

These need the container up (see "Development server"). They cover what cannot be checked
in memory: a real socket read timeout, a connection dropped by the server (`CLIENT KILL`),
a connection contaminated by a late reply, several threads sharing one pool, and a
per-family command battery against the real server — including a `BLPOP` whose deadline is
longer than the read timeout, and the proof that `HGETALL` and `ZRANGE WITHSCORES` return
the same result over RESP2 and RESP3.

**TLS is deliberately left out of this suite:** it has to pass with only
`docker-compose.yml` up, with no certificates at all. TLS is exercised by the smoke test,
via `--tls`.

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
SmokeTest.exe          # 108 steps, plaintext (6379)
SmokeTest.exe --tls    # 117 steps, all encrypted (6380) + the TLS section
```

Exits with code 0 if every step passes, and 2 on an unknown argument — a
mistyped `--tsl` would run in plaintext looking like a success, which is the
opposite of what the run was meant to prove.

`--tls` needs the override and the certs (see "TLS"). Worth running on both
backends: the normal build uses SChannel, and `lazbuild -B
--build-mode=openssl SmokeTest.lpi` switches to OpenSSL.

## License

MIT — Copyright (c) 2026 Fabiano Arndt. See `LICENSE`.
