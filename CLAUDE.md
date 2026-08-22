# pascal-redis-faa — contexto do projeto

Cliente **Redis** (protocolo RESP2/RESP3) dual-compiler (**FPC 3.2.2/Lazarus + Delphi 12**)
numa única codebase, MIT. É o terceiro da família, depois da `../pascal-amqp-faa` (cliente
AMQP 0-9-1) e da `../pascal-pipes-faa` (IPC), e segue as mesmas regras. Quando algo aqui
estiver omisso, o `CLAUDE.md` da `pascal-amqp-faa` é a referência — em especial a lista
completa de gotchas de FPC descobertos no porte.

## Regra inegociável: proveniência do código

**Nunca copiar, adaptar ou se basear em código-fonte de bibliotecas Pascal/Delphi de Redis
de terceiros.** Este projeto deriva exclusivamente da `pascal-amqp-faa` (MIT, mesmo autor)
e da especificação pública do protocolo RESP
(`redis.io/docs/latest/develop/reference/protocol-spec`) + a documentação de comandos
(`redis.io/commands`). Consultar outras implementações é permitido apenas como conhecimento
de protocolo, nunca como fonte a transcrever.

## Decisões arquiteturais (fechadas — não rediscutir sem o usuário)

Todas saíram da avaliação de viabilidade (2026-08-22). Racional detalhado em
`docs/DECISOES.md`.

- **Não existe canal.** O AMQP multiplexa canais sobre um socket; o Redis processa um
  comando por vez, em ordem estrita. O análogo de `TAMQPChannel` NÃO existe. A unidade de
  concorrência é a conexão, e o que o app segura é o **pool**:
  `TRedisConnection` (1 socket) → `TRedisPool` (N conexões) → `TRedisClient` (fachada).
- **Sem thread de leitura nas conexões de comando.** Fora do pub/sub o servidor só fala
  quando perguntado, então a própria thread chamadora escreve e lê, sob o lock da conexão.
  Zero handoff entre threads. Thread de leitura + `RedisPool` (o thread pool) existem
  APENAS para pub/sub e comandos bloqueantes.
- **Conexão que sofreu timeout ou erro de I/O é DESTRUÍDA, nunca devolvida ao pool.** Pode
  haver resposta órfã no buffer, e devolvê-la contamina o próximo comando com a resposta do
  anterior. É o bug clássico de cliente Redis; merece teste de integração dedicado.
- **Comando em voo NÃO é re-executado na reconexão.** `INCR`, `LPUSH`, `SETNX` não são
  idempotentes. Falha com `ERedisConnectionLost` e a decisão fica com o chamador. É o
  oposto do `RepublishUnconfirmedOnReconnect` da lib AMQP. O que o recovery replaya é só
  `HELLO`/`AUTH`, `SELECT <db>` e as assinaturas de pub/sub — não há topologia no servidor.
- **Pub/Sub em conexão dedicada, fora do pool.** Em RESP2 o `SUBSCRIBE` sequestra a
  conexão. Em RESP3 (`HELLO 3`) o push vem por tipo `>` e a conexão continua utilizável;
  suportar os dois, RESP2 como padrão e RESP3 opt-in.
- **Comandos bloqueantes (`BLPOP`, `BRPOP`, `XREAD BLOCK`, `WAIT`) usam conexão *detached***,
  com read timeout maior que o timeout do comando. Nunca saem do pool comum.
- **API binário-segura por contrato.** O núcleo trabalha com `TBytes`; as sobrecargas
  `string` passam por `RedisUtf8Encode/Decode` (equivalente ao de `AMQP.Wire`). Errar isso
  corrompe valores silenciosamente só no FPC, por causa do codepage dinâmico.
- **Comandos sempre no "unified request protocol"** (array de bulk strings). Comando inline
  não é binário-seguro e não será emitido.
- **A árvore de respostas NÃO usa `TValue`.** O `TValue` rendeu dois erros internos do FPC
  3.2.2 e o `AmqpUnwrapValue` na lib AMQP. A árvore RESP é pequena e fechada: modelar com
  `IRedisReply` (interface, refcount — evita caçar leak nas suítes de 0 leaks) + um enum
  `TRedisReplyKind`. Sem RTTI.
- **Fora do v1:** Redis Cluster (redirects `MOVED`/`ASK`, 16384 slots, CRC16), Sentinel,
  client-side caching (`CLIENT TRACKING`) e mTLS. Não abrir esses escopos sem o usuário.

## Peças herdadas da pascal-amqp-faa

Quatro units foram copiadas e renomeadas (`AMQP.*` → `Redis.*`, `TAMQP*` → `TRedis*`,
`Amqp*` → `Redis*`, `AMQP_*` → `REDIS_*`). Cada uma carrega um bloco de PROVENIENCIA logo
após o include do `redis.inc`. **Sem dependência entre repositórios** — mesmo padrão do
`Pipes.Threading.pas` na `pascal-pipes-faa`: correção de bug de um lado deve ser portada
manualmente para o outro.

| Unit | Origem | Papel |
|---|---|---|
| `Redis.Threading` | `AMQP.Threading` | atomics, `RedisTickMs`, `TRedisMonitor`, `TRedisThreadPool`/`RedisPool` |
| `Redis.Transport` | `AMQP.Transport` | `TRedisTcpSocket`, `ERedisTransport`, `ERedisTls`, `RedisTlsBackendName/Info` |
| `Redis.Transport.Tls` | `AMQP.Transport.Tls` | TLS via SChannel — só Windows (`REDIS_WINDOWS`, automático) |
| `Redis.Transport.OpenSSL` | `AMQP.Transport.OpenSSL` | TLS via OpenSSL — qualquer plataforma, opt-in `-dREDIS_OPENSSL` |

**Mudança pendente no transporte (fazer no M3):** `TRedisTcpSocket` só tem
`Connect/Receive/Send/Close` — **não tem read timeout**. No AMQP isso bastava (o heartbeat
detectava morte); no Redis não: um comando pendurado prende a conexão E a thread do
chamador para sempre. Adicionar `SetReceiveTimeout`/`SetSendTimeout` (`SO_RCVTIMEO`/
`SO_SNDTIMEO` via `setsockopt`, nos dois compiladores) e propagar o timeout até os dois
backends TLS.

Aviso conhecido e **pré-existente** (idêntico na lib AMQP, não foi introduzido pelo porte):
`Redis.Transport.Tls.pas(612)` e `Redis.Transport.OpenSSL.pas(712)` — "Function result does
not seem to be set". Não perseguir.

## Regras da codebase dual (Delphi + FPC 3.2.2)

Toda unit começa com o include do `redis.inc` (ativa o mode Delphi no FPC e define
`REDIS_WINDOWS`). O que NÃO usar, e o que usar no lugar:

| Proibido (não existe no FPC 3.2) | Usar |
|---|---|
| `reference to procedure` / métodos anônimos / `TThread.CreateAnonymousThread` | `procedure ... of object`; work items (`TRedisWorkItem`) para capturar estado |
| `System.Threading` (`TTask.Run`, `TParallel.For`) | `RedisPool.Queue(...)` de `Redis.Threading` |
| `System.TMonitor` (Enter/Wait/PulseAll) | `TRedisMonitor` de `Redis.Threading` |
| `TInterlocked.*` / `AtomicXxx` direto | `RedisAtomicInc/Dec/Get/Set/CompareExchange/Read64/Write64` |
| `TThread.GetTickCount64` | `RedisTickMs` |
| `System.Net.Socket` direto | `TRedisTcpSocket` de `Redis.Transport` |
| `TEncoding.UTF8.GetBytes/GetString` | `RedisUtf8Encode/Decode` (a criar no M1, em `Redis.Types`) |
| uses com namespace (`System.SysUtils`) | nome curto (`SysUtils`); no Delphi resolve via unit scope names `System;Winapi` |
| inline `var` em bloco | declarar no `var` da rotina |
| `TStringHelper` (`.Split` etc.) | rotinas manuais (`Pos`/`Copy`) |
| `TArray.Sort<T>` | ordenação manual (não existe no Generics.Collections do FPC) |
| `ReportMemoryLeaksOnShutdown` | envolver numa condicional de não-FPC |

Gotchas de FPC que valem aqui (lista completa no `CLAUDE.md` da `pascal-amqp-faa`):

- Fontes em UTF-8 **com BOM**: Delphi exige o BOM para ler UTF-8; o FPC com BOM trata
  literais como UTF-8 corretamente. Manter o BOM ao criar units novas. (As quatro units
  copiadas ganharam BOM no porte — duas não tinham.)
- Apps console FPC puro têm `DefaultSystemCodePage` diferente de UTF-8 — strings acentuadas
  literais saem transcodificadas errado. Chamar `SetMultiByteConversionCodePage(CP_UTF8)`
  no início do `program`. (O `SmokeTest.dpr` do M0 contorna escrevendo só ASCII.)
- FPC/Linux exige `cthreads` como **primeira unit** do `program`, senão `SyncObjs` falha em
  runtime com `Failed to create OS basic event`. Vale para qualquer programa que use a lib.
- `Format('%x')` com `LongInt` negativo imprime 16 dígitos → passar `Cardinal(valor)`.
- **Ordem de avaliação de argumentos não é da esquerda para a direita.** Chamar
  `Passo('nome', LeLinha(S, LLinha), ' -> ' + LLinha)` monta o terceiro argumento ANTES de o
  segundo preencher `LLinha` — o detalhe sai vazio, justo no caso de falha em que ele é a
  única pista. Ler para a variável numa instrução separada, antes da chamada. (Era assim no
  `SmokeTest.dpr` do M0; corrigido no M1.)
- `PWideChar(string)` não existe (string é Ansi) → campos que vão para APIs wide são
  `UnicodeString` (ver `Redis.Transport.Tls.FTargetName`).
- Enum anônimo inline como campo de classe não compila → tipo nomeado.
- Citar um diretivo de compilação dentro de um comentário de prosa entre chaves quebra a
  compilação: a chave de fechamento do texto citado fecha o comentário mais cedo. Escrever
  "condicional" em vez do diretivo literal.
- **SIGPIPE no Linux**: `send()` num socket encerrado mata o processo. Já resolvido no
  `Redis.Transport` copiado (`fpsend(..., MSG_NOSIGNAL)` sob FPC+Unix).
- `TTestCase.AssertException` do FPCUnit exige a classe **exata** da exceção (não aceita
  subclasse, diferente do `Assert.WillRaise` do DUnitX). Para "qualquer exceção", usar
  `try/except` com flag booleana + `AssertTrue` — nunca `Fail()` dentro do próprio `try`.
- Samples GUI: `TThread.Queue`/`.Synchronize` do FPC só têm o overload sem parâmetros —
  toda closure vira um objeto de "marshal" descartável. E o FPC **descarta**
  `TThread.Queue(nil, ...)` postado por thread que morre antes de a main thread bombear a
  fila → saltar por um worker do `RedisPool`. Ver `PublicadorConfiavelVcl` na lib AMQP.
- Samples GUI: `uses Windows, Messages` não compila fora do Windows → no FPC usar `LCLIntf`,
  `LCLType` e `LMessages` (`LM_VSCROLL` = `WM_VSCROLL`). LCL não tem `Font.Charset` nem
  `TForm.DesignSize` no `.lfm`.

## Estrutura de units

Existentes (M0 + M1):

```
src/redis.inc
src/Redis.Threading.pas          (cópia renomeada)
src/Redis.Transport.pas          (cópia renomeada; timeouts pendentes — M3)
src/Redis.Transport.Tls.pas      (cópia renomeada)
src/Redis.Transport.OpenSSL.pas  (cópia renomeada)
src/Redis.Types.pas              TRedisReplyKind, IRedisReply, TRedisArg, excecoes, params, Utf8Encode/Decode
src/Redis.Resp.pas               codec RESP2/RESP3: RedisEncodeCommand, TRedisReader, IRedisByteSource
```

Planejadas (ao criar cada uma, adicionar ao `packages/pascal_redis_faa.lpk` **e** ao
`packages/pascal_redis_faa.pas`):

```
src/Redis.Connection.pas          1 socket, HELLO/AUTH/SELECT, Execute, pipeline
src/Redis.Pool.pas                checkout/checkin, invalidacao, health check
src/Redis.Client.pas              fachada + Execute generico
src/Redis.Commands.Keys.pas       DEL EXPIRE TTL SCAN TYPE RENAME
src/Redis.Commands.Strings.pas    GET SET INCR SETEX GETSET MSET
src/Redis.Commands.Hashes.pas     HSET HGET HGETALL HDEL HSCAN
src/Redis.Commands.Lists.pas      LPUSH RPOP LRANGE + BLPOP/BRPOP
src/Redis.Commands.Sets.pas       SADD SMEMBERS SINTER
src/Redis.Commands.ZSets.pas      ZADD ZRANGE ZRANGEBYSCORE ZINCRBY
src/Redis.Commands.Scripting.pas  EVAL/EVALSHA + cache de SHA
src/Redis.Commands.Streams.pas    XADD XREADGROUP XACK XAUTOCLAIM
src/Redis.Commands.Server.pas     PING INFO CONFIG DBSIZE FLUSHDB
src/Redis.PubSub.pas              conexao dedicada + thread + callbacks
src/Redis.Transaction.pas         MULTI/EXEC/WATCH
```

Princípio: **kernel genérico primeiro**. `Execute('SET', ['k','v'])` alcança qualquer
comando desde o M2; as fachadas tipadas por família são camadas por cima. Assim o escopo
nunca bloqueia o usuário da lib.

## Build e testes

- **FPC direto:** `fpc -Fusrc -Fisrc -FEbuild -FUbuild samples\SmokeTest\SmokeTest.dpr`
  (somar `-dREDIS_OPENSSL` para o backend OpenSSL). Compilador em
  `C:\lazarus4.0\fpc\3.2.2\bin\x86_64-win64\fpc.exe`.
- **Pacote Lazarus:** `lazbuild packages\pascal_redis_faa.lpk`. Se o lazbuild não conhecer o
  pacote, registrar antes com `lazbuild --add-package-link packages\pascal_redis_faa.lpk`.
- **Build mode `openssl`:** `lazbuild -B --build-mode=openssl <proj>.lpi`. O define vai numa
  `SharedMatrixOptions` amarrada ao mode — é o único jeito de alcançar as units do
  **pacote** (Custom Options do projeto não recompilam o pacote). Trocar de mode recompila
  o pacote inteiro (cache de `.ppu` único).
- **Delphi:** Community Edition **não compila por linha de comando** — validar abrindo
  `Redis.groupproj` no IDE.
- **Servidor de teste:** `docker/docker-compose.yml` (redis:7.2-alpine, porta 6379) e o
  override `docker-compose.tls.yml` (6380, precisa dos certs de `docker/certs`).
  **Rode o SmokeTest após qualquer mudança na lib.**
- **Suítes unitárias (M1, prontas):** `tests/Unit` (DUnitX/Delphi) + `tests/Unit/fpc`
  (FPCUnit). Não precisam de servidor. Rodar as do FPC com
  `lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi` e depois
  `tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain` (sem parâmetros abre a GUI).
  As do Delphi só pelo IDE, via `Redis.groupproj`.
  **O corpo dos testes é IDÊNTICO nos dois lados** — só a declaração das fixtures muda
  (atributos `[Test]` contra seção `published`). Quem viabiliza isso é o
  `tests/Unit/Redis.DUnitXCompat.pas`, que expõe a API de asserts do FPCUnit por cima do
  `Assert` do DUnitX (e de quebra torna a comparação de strings sensível a maiúsculas — o
  `Assert.AreEqual(string, string)` do DUnitX ignora caixa por padrão, o que enfraqueceria
  os testes em silêncio e só do lado Delphi). Ao mexer numa suíte, portar para a outra na
  mesma sessão; conferir com um `diff` das seções de implementation.
- `tests/Integration` (a criar no M3): mesma estrutura dual, contra o Redis real.

## Convenções gerais

Mesmo padrão dos projetos irmãos: licença MIT com copyright de Fabiano Arndt, commits em
português, **sem pushes/commits automáticos sem confirmação explícita do usuário**.

O README tem duas versões: `README.md` (português, **canônico**) e `README.en.md` (inglês,
com identificadores/comentários dos exemplos traduzidos). **Toda mudança no README.md deve
ser replicada no README.en.md na mesma sessão.**

## Roadmap

O v1 fecha no **M8** (decidido em 2026-08-22): kernel + comandos + TLS + pipeline/transações
+ pub/sub + streams.

0. ~~**M0 — Esqueleto.**~~ **Concluído em 2026-08-22.** Repo criado, `redis.inc`, as quatro
   units copiadas e renomeadas (com a prosa de conceitos AMQP corrigida e bloco de
   proveniência), `packages/pascal_redis_faa.lpk`, `Redis.groupproj` (Delphi) e `Redis.lpg`
   (Lazarus), `docker/` com Redis plain + override TLS, `.gitignore`, READMEs pt/en,
   `docs/DECISOES.md` e um `samples/SmokeTest` mínimo (monta RESP na mão: PING → `+PONG`,
   ECHO → bulk string). **Validado:** `fpc` compila o SmokeTest e as duas units de TLS
   (plain e `-dREDIS_OPENSSL`); `lazbuild` compila o pacote e o projeto nos dois build
   modes; **`SmokeTest.exe` roda PASS contra o container** nos dois compiladores — FPC e
   Delphi 12, este último pelo `Redis.groupproj` no IDE (2026-08-22). Sem pendências.
1. ~~**M1 — `Redis.Types` + `Redis.Resp` + suíte unitária dual.**~~ **Concluído em
   2026-08-22.** Codec RESP2 (`+ - : $ *`) e RESP3 (`_ , # ! = ( % ~ | >`), `TRedisArg` com
   operadores `Implicit`, `RedisEncodeCommand`, `TRedisReader` sobre `IRedisByteSource`, e
   `RedisEncodeReply` (só para testes e servidores falsos; rebaixa os tipos do RESP3 como um
   servidor RESP2 faria, o que permite rodar o mesmo teste nos dois protocolos).
   Build limpo nos dois compiladores. Os testes cobrem leituras parciais (a mesma resposta
   relida com chunks de 1, 2, 3, 5, 7 e 13 bytes), aninhamento, nulo ≠ vazio, payloads de
   100 KB, binário com CRLF no meio e fluxo malformado. **As duas suítes passam: 133/133 no
   FPCUnit e 133/133 no DUnitX, com `Tests Leaked: 0`** — a contagem idêntica confirma a
   paridade de cobertura, e o zero de leaks confirma que a árvore por interface não fecha
   ciclo de referência (era o risco de trocar `TValue` por refcount). Sem pendências.

   Decisões de modelagem tomadas aqui (não rediscutir sem o usuário):
   - **Mapa RESP3 é guardado ACHATADO** (`Count` = 2×pares, chave em 0/2/4…). Assim
     `HGETALL`, `CONFIG GET` e `XPENDING` têm a mesma forma em RESP2 (array) e RESP3 (mapa),
     e o código da aplicação não ramifica por protocolo. `ValueByKey` funciona nos dois.
   - **Atributo (`|`) não é um kind.** É metadado que precede a resposta real; o leitor
     anexa em `IRedisReply.Attributes` da resposta seguinte.
   - **`AsInteger`/`AsDouble` num `rkNull` levantam** `ERedisTypeError` em vez de devolver 0
     — devolver 0 confundiria "chave ausente" com "chave que vale zero". `AsString` num nulo
     devolve `''` (string tem vazio natural; inteiro não), e `AsBoolean` devolve False (é
     como o RESP2 nega um `SET NX`).
   - **`TRedisReader.Buffered`** é a mecânica de detecção de conexão suja do M3: depois de
     ler a resposta de um comando isolado tem que ser zero.
   - Tipos em streaming (`$?`, `*?`) levantam erro claro: o servidor Redis não os emite.
2. **M2 — `Redis.Connection` + `Execute` genérico.** `HELLO`/`AUTH`/`SELECT`,
   escrita+leitura na thread chamadora sob lock. O SmokeTest cresce para SET/GET/DEL/INFO.
   Fecha com PASS no Win64.
3. **M3 — Pool + timeouts + reconexão.** Inclui a adição de `SetReceiveTimeout` no
   transporte (ver "Peças herdadas") e a invalidação de conexão suja. Fecha com a suíte de
   integração contra o Redis real, incluindo o teste de conexão-suja.
4. **M4 — Famílias Strings/Keys/Hashes/Lists/Sets/ZSets.** Integração por família.
5. **M5 — TLS.** `UseTls`/`TlsVerifyPeer` nos params, SmokeTest `--tls` PASS nos dois
   backends (SChannel e OpenSSL).
6. **M6 — Pipeline + `MULTI`/`EXEC`/`WATCH` + `EVAL` com cache de SHA.**
7. **M7 — Pub/Sub (RESP2) + RESP3 opt-in via `HELLO 3`.**
8. **M8 — Streams + consumer groups.** Fecha o v1.
9. **M9 — 3 samples GUI duais VCL/LCL:** `CacheAsideVcl` (GET/SETEX/DEL, hit/miss),
   `LockDistribuidoVcl` (`SET NX PX` + token de posse, release por Lua — mostra o erro
   clássico de liberar lock alheio) e `FilaTarefasVcl` (Streams + consumer groups, dois
   workers concorrentes). Candidatos posteriores: `PubSubVcl`, `RankingZSetVcl`,
   `RateLimiterVcl`.
10. **M10 — Validação Linux x86_64 + ARM64** (mesma receita de container da lib AMQP) e
    READMEs completos com exemplos.
