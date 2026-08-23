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

**Mudança feita no transporte (M3, 2026-08-22):** `TRedisTcpSocket` ganhou
`SetReceiveTimeout`/`SetSendTimeout` (`SO_RCVTIMEO`/`SO_SNDTIMEO`). No FPC via
`fpsetsockopt` — DWORD de ms no Windows, `timeval` no Unix, que é a pegadinha da API; no
Delphi pelas propriedades `ReceiveTimeout`/`SendTimeout` do `TSocket`, que já resolvem a
diferença. Estourado o prazo, `Receive`/`Send` levantam **`ERedisTransportTimeout`** (nova,
subclasse de `ERedisTransport`), que a `Redis.Connection` traduz para `ERedisTimeout`.
Os dois backends TLS foram ajustados para **deixar essa exceção passar**: eles rebaixam
qualquer erro a EOF, e rebaixar um timeout diria "o servidor encerrou" quando o que houve
foi "eu desisti" — com a resposta ainda a caminho.

**Armadilha do Delphi (custou uma rodada de teste):** `TSocket.ReceiveTimeout`/`SendTimeout`
só valem se atribuídos com o socket **já conectado**. O `TSocket.Connect` faz
`FSocket := CreateSocket`, e é dentro do `CreateSocket` que a RTL empurra os timeouts para o
socket — usando o campo `FSocket`, que naquele instante ainda é `InvalidSocket`, porque só
recebe o handle quando o `CreateSocket` retorna. O `setsockopt` vai para um handle inválido,
falha, e a RTL descarta o resultado. Pior: o setter guarda o valor, então reatribuir o mesmo
número depois vira no-op (`if FReceiveTimeout <> Value`) e não há segunda chance. Por isso o
`ApplyTimeouts` só escreve a propriedade depois que `TSocketState.Connected` entra em
`FSock.State`. **Sintoma:** FPC estoura o timeout certinho e o Delphi espera o comando
inteiro (apareceu num `BLPOP` de 2 s com timeout de 300 ms).

**Divergência introduzida no M5 (2026-08-22):** as oito mensagens de erro com
acentuação nas duas units de TLS viraram ASCII (`validacao`, `conexao`, `nao`,
`renegociacao`...). Motivo: app console FPC puro imprime UTF-8 num console
cp850, e a mensagem que o usuário mais precisa ler — "validação do certificado
falhou" — saía ilegível justo no momento da falha. É o mesmo motivo pelo qual as
units escritas neste projeto já evitam acento em literal. **Se for portar
correção para a `pascal-amqp-faa`, esta diferença é intencional.**

Aviso conhecido e **pré-existente** (idêntico na lib AMQP, não foi introduzido pelo porte):
`Redis.Transport.Tls.pas(617)` e `Redis.Transport.OpenSSL.pas(719)` — "Function result does
not seem to be set". Não perseguir. (As linhas andam quando as units são editadas; o que
identifica o aviso é a mensagem, não o número.)

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
- **`TEncoding.UTF8.GetString` do Delphi LEVANTA em bytes que não são UTF-8 válido**, e o
  FPC não. O `TUTF8Encoding` nasce com `MB_ERR_INVALID_CHARS`, e o `TEncoding.GetString`
  tem um `if (ByteCount > 0) and (Len = 0) then raise EEncodingError`. Como o Redis guarda
  **bytes**, `AsString` num valor binário estourava — só no Delphi, e com exceção da RTL.
  Por isso o `RedisUtf8Decode` usa um `TMBCSEncoding.Create(CP_UTF8, 0, 0)` próprio
  (tolerante: byte inválido vira U+FFFD). Achado na validação do M7 no Delphi, onde
  derrubava calado a mensagem binária de pub/sub — o callback levantava, a lib mandava
  para o `OnError` e a mensagem sumia. Travado por teste nas duas suítes
  (`Decode_BytesInvalidos_NaoLevanta`, `AsString_EmBulkBinario_NaoLevanta`).
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

Existentes (M0 a M8):

```
src/redis.inc
src/Redis.Threading.pas          (cópia renomeada)
src/Redis.Transport.pas          (cópia renomeada; timeouts de socket no M3)
src/Redis.Transport.Tls.pas      (cópia renomeada)
src/Redis.Transport.OpenSSL.pas  (cópia renomeada)
src/Redis.Types.pas              TRedisReplyKind, IRedisReply, TRedisArg, excecoes, params, Utf8Encode/Decode
src/Redis.Resp.pas               codec RESP2/RESP3: RedisEncodeCommand, TRedisReader, IRedisByteSource
src/Redis.Connection.pas         TRedisConnection (1 socket, handshake, Execute/ExecuteRaw, Send/Receive full-duplex), TRedisPipeline, TRedisSocketStream
src/Redis.Pool.pas               TRedisPool (Acquire/Release, descarte, health check, poda), TRedisPoolParams
src/Redis.Commands.pas           TRedisCommandExecutor (abstrato), TRedisCommandFamily, RedisArgs, conversores
src/Redis.Commands.Keys.pas      DEL UNLINK EXISTS EXPIRE TTL TYPE RENAME COPY SCAN
src/Redis.Commands.Strings.pas   GET SET (NX/XX/EX/KEEPTTL/GET) INCR MSET MGET GETRANGE
src/Redis.Commands.Hashes.pas    HSET HGET HGETALL HMGET HDEL HINCRBY HSCAN
src/Redis.Commands.Lists.pas     LPUSH RPOP LRANGE LMOVE + BLPOP/BRPOP/BLMOVE
src/Redis.Commands.Sets.pas      SADD SMEMBERS SMISMEMBER SINTER/SUNION/SDIFF SSCAN
src/Redis.Commands.ZSets.pas     ZADD ZRANGE ZRANGEBYSCORE ZINCRBY ZPOPMIN ZSCAN
src/Redis.Commands.Streams.pas   XADD XRANGE XREAD XGROUP XREADGROUP XACK XPENDING XCLAIM XAUTOCLAIM XINFO
src/Redis.Commands.Scripting.pas EVAL/EVALSHA com cache de SHA, SCRIPT LOAD/EXISTS/FLUSH
src/Redis.Commands.PubSub.pas    PUBLISH SPUBLISH PUBSUB CHANNELS/NUMSUB/NUMPAT (quem publica)
src/Redis.Transaction.pas        TRedisTransaction: MULTI/EXEC/WATCH em um pipeline só
src/Redis.PubSub.pas             TRedisSubscriber: conexão dedicada + thread de leitura + callbacks
src/Redis.Client.pas             TRedisClient: pool + famílias + Execute genérico + CreateSubscriber
```

Planejadas (ao criar cada uma, adicionar ao `packages/pascal_redis_faa.lpk` **e** ao
`packages/pascal_redis_faa.pas`):

```
src/Redis.Commands.Server.pas     PING INFO CONFIG DBSIZE FLUSHDB
```

A `Redis.Commands.Server` é a **única** peça da lista original que o v1 não entregou.
Não bloqueia ninguém: `Execute('INFO', [])` alcança qualquer comando desde o M2, e a
`TRedisConnection` já expõe `Ping` e `Select`. Fica como primeiro candidato pós-v1.

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
- **Delphi com OpenSSL:** os três `.dproj` já trazem a build configuration **`OpenSSL`**
  (`Cfg_3`, filha de Debug, com `REDIS_OPENSSL` no `DCC_Define`) — é a irmã do build mode
  `openssl` do Lazarus. Selecione-a no Project Manager e compile; **não** acrescente a
  diretiva à mão em Conditional defines, porque ela cairia na configuração *Debug* e a
  partir daí todo build "normal" usaria OpenSSL em silêncio. Em projeto Win32 a lista de
  sonames tenta `libcrypto-3-x64.dll` primeiro, falha (processo 32-bit não carrega DLL x64)
  e cai para `libcrypto-3.dll` — que o Windows acha em `SysWOW64`. O fallback é o que faz
  isso funcionar sem instalar nada.
- **O IDE do Delphi reescreve o `.dproj` ao trocar de configuração**, gravando a
  configuração ativa em `<Config Condition="'$(Config)'==''">`. Isso é estado local: não
  commite. Antes de fechar, volte a configuração para `Debug`, ou descarte o arquivo com
  `git checkout -- <proj>.dproj`.
- **Servidor de teste:** `docker/docker-compose.yml` (redis:7.2-alpine, porta 6379) e o
  override `docker-compose.tls.yml` (6380, precisa dos certs de `docker/certs`).
  **Rode o SmokeTest após qualquer mudança na lib.**
- **SmokeTest com TLS (M5):** `SmokeTest.exe --tls` roda a bateria INTEIRA contra o
  listener cifrado (6380) em vez do de texto claro, e acrescenta a seção de TLS —
  160 passos contra os 151 do modo plain. Desde o M7 ele vale ainda mais: pub/sub é o
  único caso em que uma thread lê e outra escreve no mesmo envelope TLS ao mesmo tempo.
  Precisa dos certs e do override de pé:
  `docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d`. Rode nos
  **dois backends**: build normal (SChannel) e `--build-mode=openssl` (OpenSSL).
  Qualquer argumento que não seja `--tls` é recusado com exit code 2 — um `--tsl`
  digitado errado rodaria em texto claro com cara de sucesso.
- **TLS fica FORA da suíte de integração**, de propósito: ela precisa valer com só o
  `docker-compose.yml` de pé, sem certs. Quem exercita TLS é o SmokeTest.
- **Suítes unitárias (M1–M8, prontas):** `tests/Unit` (DUnitX/Delphi) + `tests/Unit/fpc`
  (FPCUnit). Não precisam de servidor — a `Redis.ConnectionTests` sobe a conexão inteira
  sobre um servidor falso em memória (`TRedisConnection.CreateOnStream`), e a
  `Redis.CommandsTests` amarra um `TRedisClient` a esse mesmo servidor falso para conferir
  **os bytes que foram para o fio** e a conversão da resposta. A `Redis.PubSubTests` vai
  além: o servidor falso dela **responde** (interpreta o `SUBSCRIBE` e devolve a
  confirmação), porque sem diálogo não dá para testar confirmação, ordem de mensagens nem
  queda de conexão. Ele também desiste da leitura depois de 10 s — trava de segurança para
  que um teste mal escrito não pendure a suíte na thread de leitura. **Esse é o padrão a
  reusar sempre que o teste depender de diálogo** e não de resposta roteirizada: o fake
  interpreta o que o cliente escreveu, mantém o estado mínimo do servidor e responde — e a
  leitura BLOQUEIA quando não há nada, como um socket em silêncio. O M8 avaliou usá-lo e
  **não** precisou: streams não têm thread nem diálogo, e o que interessa ali (ordem dos
  modificadores, mapa do RESP3, entrada sem campos) o roteiro fixo verifica melhor, porque
  é o teste que escolhe a resposta. Ver a seção 48 de `docs/DECISOES.md`. Rodar as do FPC com
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
  **A tradução é mecânica, então faça por script em vez de à mão** (foi assim que a
  `Redis.PubSubTests` nasceu nos dois lados, no M7): escreva o arquivo DUnitX e derive o
  gêmeo FPCUnit trocando (a) o cabeçalho, que cita o caminho da suíte irmã, (b) o `uses`
  — `fpcunit, testregistry, SysUtils, Classes, ...` no lugar de
  `DUnitX.TestFramework, System.*, ..., Redis.DUnitXCompat` —, (c) `[TestFixture] X = class`
  + `public` por `X = class(TTestCase)` + `published`, removendo os prefixos `[Test] `, e
  (d) `TDUnitX.RegisterTestFixture(X)` por `RegisterTest(X)`. Acrescente `{$mode delphi}{$H+}`
  antes do `interface`. Depois um `diff` dos dois arquivos tem de mostrar **só** essas
  quatro coisas — é a prova barata de que a cobertura não divergiu.
- **Suíte de integração (M3–M8, pronta):** `tests/Integration` (DUnitX/Delphi, projeto
  `Redis.IntegrationSuite.dproj`) + `tests/Integration/fpc` (FPCUnit). **Precisa do
  container de pé.** O M4 acrescentou uma fixture por família, mais a do
  `TRedisClient`; o M7, a de pub/sub — que inclui o teste de **reconexão** (derruba a
  conexão do assinante com `CLIENT KILL ID` e confere que as assinaturas voltaram, pelo
  `PUBSUB NUMSUB` do servidor, não pela contabilidade do cliente); o M8, a de streams,
  com o ciclo inteiro de uma fila de trabalho (dois consumidores repartindo as entradas,
  pendência sobrevivendo ao worker morto e voltando por `XAUTOCLAIM`, `XCLAIM` que não
  rouba trabalho recente). Mesma regra de paridade das unitárias — corpo idêntico, só as
  fixtures mudam. Rodar as do FPC com
  `lazbuild -B tests\Integration\fpc\RedisIntegrationTestsFpc.lpi` e depois
  `tests\Integration\fpc\RedisIntegrationTestsFpc.exe --all --format=plain`.
  O programa Delphi se chama `Redis.IntegrationSuite` porque a **unit** já se chama
  `Redis.IntegrationTests` e o Delphi não aceita projeto e unit homônimos.

## Convenções gerais

Mesmo padrão dos projetos irmãos: licença MIT com copyright de Fabiano Arndt, commits em
português, **sem pushes/commits automáticos sem confirmação explícita do usuário**.

O README tem duas versões: `README.md` (português, **canônico**) e `README.en.md` (inglês,
com identificadores/comentários dos exemplos traduzidos). **Toda mudança no README.md deve
ser replicada no README.en.md na mesma sessão.**

**Fim de linha: LF**, no working tree e no repositório, em tudo — fontes, `.lpi`/`.lpk`,
`.dproj`, markdown. O `core.autocrlf` desta máquina está em `true` e o git avisa que
"LF will be replaced by CRLF"; o aviso é inofensivo e **não é convite para converter
nada**. Ferramenta que reescreve arquivo (script de edição em massa, sobretudo) tem de
preservar LF: converter para CRLF marca o arquivo inteiro como alterado para quem
comparar working trees, e não traz benefício nenhum — tanto o FPC quanto o Delphi 12 leem
LF sem reclamar.

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
   - **`TRedisReader.Buffered`** é a mecânica de detecção de conexão suja (usada pelo M2
     em `IsDirty`): depois de ler a resposta de um comando isolado tem que ser zero.
   - Tipos em streaming (`$?`, `*?`) levantam erro claro: o servidor Redis não os emite.
2. ~~**M2 — `Redis.Connection` + `Execute` genérico.**~~ **Concluído em 2026-08-22.**
   `TRedisConnection` (1 socket, lock, `TRedisReader`), handshake (`HELLO 3` ou
   `AUTH` + `CLIENT SETNAME` + `SELECT`), `Execute`/`ExecuteRaw`/`ExecuteArgs`, `Ping`,
   `Select`, `Abort` e `TRedisPipeline`. Escrita e leitura acontecem na thread chamadora,
   sob o lock — não há thread de leitura. **Validado nos DOIS compiladores** contra o
   container (2026-08-22): SmokeTest **PASS nos 50 passos** no FPC (fpc direto e lazbuild
   nos dois build modes) **e no Delphi 12** (pelo `Redis.groupproj`), cobrindo
   SET/GET/DEL/EXISTS/INCR/TTL/INFO/DBSIZE, binário com CRLF, UTF-8, bulk de 200 KB,
   pipeline (inclusive erro no meio do lote), `WRONGTYPE`, `SELECT` entre bancos, RESP3 via
   `HELLO 3` (versão, id, mapa e double nativos) e invalidação de conexão. As duas suítes
   unitárias passam com **178/178** (133 do M1 + 45 novos) — FPCUnit e DUnitX, este com
   `Tests Leaked: 0`. A contagem idêntica dos dois lados é o que confirma a paridade de
   cobertura; o zero de leaks confirma que a conexão não deixa reader, stream nem árvore de
   resposta para trás (nem no caminho de invalidação, que libera tudo no meio do erro).

   Decisões tomadas aqui (racional nas seções 17–21 de `docs/DECISOES.md`):
   - **Erro de servidor levanta `ERedisReplyError` e NÃO invalida a conexão**; erro de I/O,
     timeout ou fluxo malformado invalidam e viram `ERedisConnectionLost` — inclusive o que
     sobe do transporte, que é traduzido para não vazar exceção de camada de baixo.
     `ExecuteRaw` devolve o `rkError` sem levantar, e **pipeline nunca levanta**.
   - **`IsDirty`/`IsUsable`**: sobrou byte depois da última resposta = conexão contaminada.
     Não é erro (a resposta entregue está certa), é estado — e é o que o pool do M3 checa.
   - **`Abort` não pega o lock**, para conseguir desbloquear uma leitura pendurada em outra
     thread; a faxina fica para quem estava lendo.
   - **Em RESP2 não se emite `HELLO`** (só existe no Redis 6+), então `ServerVersion` e
     `ServerId` ficam vazios nesse modo.
   - **`CreateOnStream`** adota um `TStream` pronto: é o que torna a conexão inteira
     testável sem rede (e o que o pool usará para injetar transporte).
3. ~~**M3 — Pool + timeouts + reconexão.**~~ **Concluído em 2026-08-22.**
   `SetReceiveTimeout`/`SetSendTimeout` no transporte (ver "Peças herdadas"),
   `ERedisTransportTimeout` → `ERedisTimeout` na conexão, `TRedisConnection.SetReceiveTimeout`
   para os comandos bloqueantes do M4/M8, e `Redis.Pool` com `Acquire`/`Release`, descarte
   de conexão inutilizável, health check por PING, poda por ociosidade e teto com espera.
   **Validado nos DOIS compiladores** contra o container (2026-08-22): suíte de integração
   **11/11**, SmokeTest **PASS nos 60 passos** e unitárias **196/196** (178 + 18 do pool),
   com `Tests Leaked: 0` nas duas suítes DUnitX — inclusive na de integração, onde vazar
   significaria conexão que ninguém fechou. O read timeout do Delphi só passou depois de
   corrigir a armadilha do `TSocket` descrita em "Peças herdadas": na primeira rodada o FPC
   estourava em 300 ms e o Delphi esperava os 2 s inteiros do `BLPOP`.

   Decisões tomadas aqui (racional nas seções 22–25 de `docs/DECISOES.md`):
   - **Timeout tem exceção própria** (`ERedisTransportTimeout` no transporte,
     `ERedisTimeout` na conexão) e NÃO é rebaixado a fim de fluxo: "o servidor encerrou" e
     "eu desisti de esperar" levam a decisões diferentes, e no segundo caso a resposta
     ainda pode chegar.
   - **O pool descarta em vez de consertar.** Conexão invalidada, suja ou com o banco
     trocado é destruída no `Release`. Não existe "retomar" conexão no Redis: o que o pool
     faz é abrir outra, e o handshake dela replaya `HELLO`/`AUTH`, `CLIENT SETNAME` e
     `SELECT` — o comando em voo continua não sendo repetido.
   - **Health check e criação acontecem FORA do lock do pool**; sob o lock só anda
     contador e lista. Uma ida e volta de rede segurando o lock congelaria todas as
     threads.
   - **A vaga é reservada antes de soltar o lock** (e devolvida se a conexão não abrir),
     senão N threads veriam o mesmo "cabe mais uma" e estourariam o `MaxSize`.
   - **`CreateConnection` é `virtual`**: é o que torna a lógica do pool testável sem rede
     (as suítes sobrescrevem para devolver conexões sobre o servidor falso).
4. ~~**M4 — Famílias Strings/Keys/Hashes/Lists/Sets/ZSets.**~~ **Concluído em
   2026-08-22.** `Redis.Commands` (executor abstrato + base das famílias), as seis units de
   família e o `Redis.Client` com `TRedisClient`. **Validado nos DOIS compiladores** contra
   o container (2026-08-22): SmokeTest **PASS nos 90 passos** no FPC (fpc direto e lazbuild
   nos dois build modes) **e no Delphi 12** (pelo `Redis.groupproj`), unitárias
   **301/301** (196 + 105 do M4) e integração **34/34** (11 + 23 do M4), com
   `Tests Leaked: 0` nas duas suítes DUnitX. A contagem idêntica dos dois lados é o que
   confirma a paridade; o zero de leaks na de integração confirma que as famílias não
   deixam conexão nem árvore de resposta para trás — inclusive o pool separado dos
   bloqueantes, que nasce sob demanda e é destruído com o cliente. Nada no kernel foi
   alterado no M4: `Redis.Connection` e `Redis.Pool` saíram sem uma linha mudada.

   Decisões tomadas aqui (racional nas seções 26–30 de `docs/DECISOES.md`):
   - **A família pendura num `TRedisCommandExecutor` abstrato**, não na conexão. É o que
     quebra o ciclo `família → quem executa → quem reúne as famílias`, e o que deixou a
     conexão intocada. Class helper por família foi descartado: só um helper por classe
     fica visível num escopo, e seis units escolheriam um em silêncio.
   - **Nulo não vira `''` nem `0` na fachada.** Escalar vira tipo nativo; resposta que pode
     ser nula devolve `IRedisReply` ou tem par `TryXxx`; lista vira `TRedisStringArray`
     apenas onde o servidor não produz nulo no meio (`MGET` e `HMGET` devolvem
     `IRedisReply` de propósito).
   - **Um argumento é `TRedisArg`, não duas sobrecargas.** Os operadores `Implicit` fazem a
     mesma assinatura aceitar `string` e `TBytes`, o que mantém o contrato binário sem
     dobrar ~150 métodos. A conversão implícita vale para **parâmetro**, não só para
     elemento de array constructor — conferido no FPC 3.2.2 com um programa-sonda antes
     de escrever as units, porque é a premissa em que a API inteira se apoia.
   - **Bloqueante sai por um pool SEPARADO**, criado sob demanda, com o read timeout
     esticado por chamada (timeout do comando + `REDIS_BLOCKING_MARGIN_MS`) e **restaurado
     antes da devolução** — senão o health check do pool esperaria o prazo esticado.
   - **`WITHSCORES` muda de forma entre RESP2 (lista achatada) e RESP3 (lista de pares)**,
     e quem absorve é `RedisReplyToScoreMembers`, decidindo pela forma do primeiro item.
     Achatar isso no leitor estragaria os outros arrays de arrays.
5. ~~**M5 — TLS.**~~ **Concluído em 2026-08-23.** `UseTls`/`TlsVerifyPeer` ligados de
   verdade na `Redis.Connection` (o socket ganha um envelope `TRedisSchannelStream` ou
   `TRedisOpenSslStream` e o codec continua lendo de um `TStream`), `RedisDefaultTlsParams`
   e o argumento `--tls` do SmokeTest. **Validado nas QUATRO combinações** de compilador x
   backend contra o container com o override TLS (2026-08-22/23): SmokeTest **PASS nos 99
   passos com `--tls`** e **90 sem**, em FPC+SChannel, FPC+OpenSSL 3.2.4, Delphi+SChannel e
   Delphi+OpenSSL 3.5.2; unitárias **305/305** (301 + 4 do M5) e integração **34/34**, com
   `Tests Leaked: 0` nas duas suítes DUnitX.

   As duas units de transporte TLS estavam no repo desde o M0 sem nunca terem aberto um
   handshake — e a `Redis.Transport.OpenSSL` nunca tinha sido **compilada** pelo Delphi,
   porque nenhum projeto daqui definia a diretiva. O M5 é onde as quatro células da matriz
   ficaram verdes pela primeira vez.

   Decisões tomadas aqui (racional nas seções 31–33 de `docs/DECISOES.md`):
   - **TLS não é upgrade em banda: é outra porta.** Não existe `STARTTLS` no Redis, então
     `UseTls` anda junto com a porta — daí o `RedisDefaultTlsParams`, que muda as duas
     coisas de uma vez.
   - **Handshake TLS contra porta plain vira `ERedisTimeout`, não erro de cripto.** O
     servidor lê o ClientHello como comando inline e nunca responde; quem desata o nó é o
     `SO_RCVTIMEO` do M3. A mensagem **nomeia a troca de porta** como causa provável,
     porque a pista natural (certificado) leva ao lugar errado.
   - **O caso simétrico — plain contra porta TLS — NÃO é detectado no `Open`**, e é
     escolha: em RESP2 sem senha/nome/banco o handshake não emite byte nenhum. Detectar
     custaria um `PING` em toda conexão do pool.
   - **A escolha insegura precisa de uma linha explícita.** `RedisDefaultTlsParams` mantém
     `TlsVerifyPeer := True`; não há atalho que já venha com a validação desligada. Tem
     teste unitário dedicado a travar isso.
   - **Backend escolhido em COMPILAÇÃO** (`REDIS_OPENSSL` > `REDIS_WINDOWS`), nunca em
     runtime; build sem backend levanta `ERedisTls` em vez de cair para texto claro.
   - **Stream adotado (`CreateOnStream`) ignora `UseTls`**: ele já É o transporte pronto, e
     cifrar por cima seria TLS dentro de TLS. É o que mantém as suítes unitárias rodando
     sem rede mesmo com `UseTls` nos parâmetros.
6. ~~**M6 — `MULTI`/`EXEC`/`WATCH` + `EVAL` com cache de SHA**~~ **Concluído em
   2026-08-23.** `Redis.Transaction` (`TRedisTransaction` com `Watch`/`Queue`/`TryCommit`/
   `Discard`) e `Redis.Commands.Scripting` (`Eval`/`EvalSha`/`Run` com cache, `SCRIPT
   LOAD`/`EXISTS`/`FLUSH`), mais `BeginTransaction` e `Scripting` no `TRedisClient`.
   **Validado nos DOIS compiladores** contra o container (2026-08-23): SmokeTest **PASS
   nos 108 passos** e **117 com `--tls`**, no FPC (SChannel/OpenSSL x fpc direto/lazbuild)
   e no Delphi 12 (configurações Debug e OpenSSL); unitárias **339/339** (305 + 34 do M6)
   e integração **45/45** (34 + 11), com `Tests Leaked: 0` — no Delphi as duas suítes
   rodaram nas **duas** configurações, o que fecha a matriz compilador x backend também
   para os testes, e não só para o SmokeTest.

   Decisões tomadas aqui (racional nas seções 34–36 de `docs/DECISOES.md`):
   - **`MULTI`/`EXEC` não é rollback.** Erro de execução de um comando vem como item
     `rkError` no array e os outros ficam gravados; `Commit` **não** levanta por isso —
     levantar faria o chamador concluir que nada foi gravado. O que levanta é comando
     recusado no *enfileiramento*, e a mensagem cita qual.
   - **O bloco inteiro sai num pipeline só** (`MULTI` + comandos + `EXEC`), reusando o
     `TRedisPipeline` do M2: uma ida e volta em vez de N+2. Daí o `Discard` não precisar
     ir ao fio e o bloco VAZIO precisar (sob `WATCH` ele é uma pergunta legítima).
   - **`WATCH` é estado da CONEXÃO**, então a transação segura uma conexão inteira e o
     destrutor manda `UNWATCH` se houve vigilância sem commit — senão o `EXEC` do próximo
     usuário do pool abortaria sem motivo aparente.
   - **O SHA é calculado localmente, sobre os BYTES UTF-8.** Hashear a representação da
     `string` daria digest diferente do servidor com qualquer acento no script, e o
     `EVALSHA` responderia `NOSCRIPT` para sempre com o cache "funcionando".
   - **`NOSCRIPT` não é erro, é recado:** `Run` reenvia o `EVAL` sozinha e quem chamou não
     vê nada. Erro do próprio Lua sobe — reenviar só repetiria a falha.
7. ~~**M7 — Pub/Sub (RESP2) + RESP3 opt-in via `HELLO 3`.**~~ **Concluído em 2026-08-23.**
   `Redis.PubSub` (`TRedisSubscriber`: conexão dedicada, thread de leitura, callbacks,
   reconexão com replay das assinaturas) e `Redis.Commands.PubSub`
   (`PUBLISH`/`SPUBLISH`/`PUBSUB`), mais `PubSub` e `CreateSubscriber` no `TRedisClient`.
   O kernel ganhou o modo **full-duplex** (`TRedisConnection.Send`/`Receive`) — a primeira
   mudança na `Redis.Connection` desde o M5.
   **Validado nos DOIS compiladores** contra o container (2026-08-23): SmokeTest **PASS
   nos 126 passos** e **135 com `--tls`** — no FPC nas quatro combinações de
   SChannel/OpenSSL x fpc direto/lazbuild, e no Delphi 12 nas configurações Debug
   (SChannel) e OpenSSL; unitárias **376/376** (339 + 37) e integração **55/55**
   (45 + 10), com `Tests Leaked: 0` nas duas configurações do Delphi. O TLS importa mais
   aqui do que nos milestones anteriores: pub/sub é o único caso em que uma thread lê e
   outra escreve no MESMO envelope TLS ao mesmo tempo, e é o `--tls` que exercita isso —
   passou nos dois backends.

   A primeira rodada no **Delphi 12** derrubou um bug **anterior ao M7**: o
   `RedisUtf8Decode` levantava `EEncodingError` em bytes que não são UTF-8 válido, porque
   o `TEncoding.UTF8` do Delphi usa `MB_ERR_INVALID_CHARS` (ver os gotchas da codebase
   dual). Só apareceu agora porque o pub/sub é o primeiro caminho que chama `AsString`
   num payload binário — e o sintoma era cruel: a mensagem binária sumia calada, porque
   a exceção do callback ia para o `OnError` e a thread de leitura seguia viva. O decode
   passou a usar um `TMBCSEncoding` tolerante, com teste nas duas suítes
   (`Decode_BytesInvalidos_NaoLevanta`, `AsString_EmBulkBinario_NaoLevanta`). Vale como
   lembrete de para que serve a matriz: o FPC dava PASS nos 126 passos com o bug de pé.

   Decisões tomadas aqui (racional nas seções 37–42 de `docs/DECISOES.md`):
   - **O kernel ganhou `Send` (escreve e não lê) e `Receive` (lê sem ter escrito).**
     `Receive` **não pega o lock**, senão um canal em silêncio — o estado normal de um
     assinante — impediria qualquer `SUBSCRIBE` novo de sair. Nesse modo, sobra no buffer
     NÃO é conexão suja (é a próxima mensagem) e a falha **não libera** reader e stream,
     só derruba o socket: há duas threads na conexão, e a que falhou não sabe onde a
     outra está. É a mesma regra do `Abort`.
   - **A conexão do assinante não tem read timeout**, porque silêncio não é falha. Quem
     desbloqueia a leitura no `Stop` é o `Abort`. Limitação conhecida: conexão que morre
     em silêncio só aparece quando o TCP desiste — o Redis não tem heartbeat. Quem
     precisa detectar antes chama `Ping` de um timer da aplicação.
   - **O callback roda NA THREAD DE LEITURA, em ordem.** Ordem é o único compromisso que
     o pub/sub do Redis cumpre; despachar pelo `RedisPool` daria vazão e a embaralharia.
     O preço está no contrato: callback lento segura o socket. Exceção de callback vai
     para o `OnError` e não derruba a conexão, e `Execute` de dentro do callback é
     recusado na hora (quem leria a resposta é a thread que está no callback).
   - **`Subscribe` espera a confirmação do servidor**, e espera o ESTADO (o canal na lista
     confirmada), não uma contagem de mensagens. Sem isso, publicar logo depois de assinar
     seria uma corrida. Não espera em dois casos: chamado de dentro do callback, e com a
     conexão caída sob `AutoReconnect` (a assinatura fica registrada e vai no fio depois).
   - **Duas listas por tipo de assinatura**: o que a aplicação pediu (a reconexão reenvia)
     e o que o servidor confirmou (a queda zera). Uma só não daria conta.
   - **Em RESP2 a lib recusa antes do servidor** o comando que ele recusaria em modo de
     assinatura, nomeando a saída; em RESP3 não há restrição. Na thread de leitura, RESP3
     separa os mundos pelo tipo push; RESP2, pela forma do array — verbo, aridade **e**
     terceiro item inteiro nas confirmações (sem essa terceira checagem, um
     `PUBSUB CHANNELS` com um canal chamado `subscribe` viraria confirmação fantasma).
   - **Mensagem publicada com o assinante fora do ar está perdida**, e a lib não finge o
     contrário com fila local. `PUBLISH` devolve quantos receberam NAQUELE instante.
8. ~~**M8 — Streams + consumer groups.**~~ **Concluído em 2026-08-23.** Fecha o v1.
   `Redis.Commands.Streams` (`TRedisStreamsCommands`: XADD/XRANGE/XREVRANGE/XTRIM,
   XREAD com e sem BLOCK, XGROUP CREATE/DESTROY/SETID/CREATECONSUMER/DELCONSUMER,
   XREADGROUP com e sem BLOCK, XACK, XPENDING nas duas formas, XCLAIM/XAUTOCLAIM e
   XINFO), mais a propriedade `Streams` no `TRedisClient`. **Nada no kernel foi
   alterado**: `Redis.Connection`, `Redis.Pool` e `Redis.Commands` saíram sem uma linha
   mudada — o `ExecuteBlocking` do M4 já servia aos dois bloqueantes novos.
   **Validado nos DOIS compiladores** contra o container (2026-08-23): SmokeTest **PASS
   nos 151 passos** e **160 com `--tls`**, no FPC nas quatro combinações de
   SChannel/OpenSSL x fpc direto/lazbuild e no Delphi 12 nas configurações Debug e
   OpenSSL; unitárias **436/436** (376 + 60) e integração **69/69** (55 + 14), com
   `Tests Leaked: 0` nas duas configurações do Delphi — inclusive na de integração, onde
   vazar significaria conexão que ninguém fechou. As contagens IDÊNTICAS dos dois lados
   são o que confirma a paridade de cobertura das suítes gêmeas.

   Ao contrário do M7, a rodada no Delphi não derrubou nada: nenhum ajuste foi preciso
   depois dela. Os quatro arquivos de projeto do Delphi ganharam a unit nova
   (`Redis.UnitTests.dpr`/`.dproj` e `Redis.IntegrationSuite.dpr`/`.dproj`); o
   `SmokeTest.dproj` alcança a `src` pelo search path e não precisou mudar.

   Decisões tomadas aqui (racional nas seções 43–48 de `docs/DECISOES.md`):
   - **Entrada sem campos é `nil`, não dicionário vazio.** `XDEL` tira do stream mas não
     da PEL: reler a PEL alcança ids que já não existem, e o servidor manda os campos
     nulos. `IsDeleted` lê isso por extenso. Levantar quebraria justo a rotina de
     recuperação de quem perdeu um worker.
   - **`XREAD`/`XREADGROUP` mudam de forma entre RESP2 (lista de pares) e RESP3 (mapa)**,
     e quem absorve é `RedisReplyToStreamData`, decidindo pela forma do primeiro item —
     a mesma solução do `WITHSCORES` no M4. E **chave sem novidade não aparece na
     resposta**: o nome vem junto de cada bloco, e `RedisFindStreamData` procura por ele.
     Indexar pela posição da chamada lê a chave errada na primeira vez que uma fica quieta.
   - **`BLOCK` fica em MILISSEGUNDOS na API pública**, porque é a unidade do comando —
     ao contrário do timeout do `BLPOP`, que é em segundos. Uniformizar esconderia um
     erro de fator 1000, que não estoura: só espera o tempo errado. A conversão para
     `ExecuteBlocking` acontece num lugar só.
   - **`BUSYGROUP` é recado, não falha — mas só ele.** `XGroupTryCreate` devolve False
     quando o grupo já existia (é o que todo worker vê ao subir depois do primeiro);
     qualquer outro erro sobe. Mesma forma do `NOSCRIPT` no M6.
   - **`XAUTOCLAIM` aceita resposta de dois e de três itens** (Redis 6.2 x 7.0). Não há
     detecção de versão em lugar nenhum da lib: a forma da resposta é a informação, e ela
     chega junto com o dado.
9. **M9 — Samples GUI duais VCL/LCL (Onda A: 5 samples).** Escopo fechado em 2026-08-23;
   catálogo completo, racional e lista de descartados na **seção 49 de `docs/DECISOES.md`**.
   Regra de curadoria que decide o desenho de cada um: **um mecanismo por sample**, e as
   **variações do padrão são controles DENTRO do sample** (combo, checkbox, botão que
   quebra a demonstração de propósito), nunca um app novo.

   1. `CacheAsideVcl` — cache-aside com hit/miss e TTL na tela. Exercita `Strings` e
      `Keys.Ttl`. Armadilha: **`SET` sem `KEEPTTL` apaga o TTL** — regravar o valor
      cacheado com `SetValue` transforma o cache em vazamento permanente (está
      documentado em `TRedisSetOptions`; aqui vira botão). Variações internas: TTL fixo
      × com jitter (stampede), `DEL` × `UNLINK`.
   2. `LockDistribuidoVcl` — lock com token de posse. Exercita `SET NX PX` e
      `Scripting.Run` (release compare-and-delete em Lua, com o cache de SHA do M6).
      Armadilha: liberar lock alheio com `DEL` depois de o próprio lock ter expirado.
      Variações: sem token (errado) / com token / com renovação por `PEXPIRE` em Lua.
      Precisa dizer na tela que é lock de instância única, não Redlock.
   3. `FilaTarefasVcl` — fila de trabalho durável. Exercita Streams + consumer groups
      (`XAdd`, `XGroupTryCreate`, `XReadGroupBlocking`, `XAck`, `XPendingIdle`,
      `XAutoClaim`). Armadilhas: pendência sobrevivendo ao worker morto; entrada
      apagada (`IsDeleted`). Variações: um × dois consumidores.
   4. `PubSubVcl` — notificação efêmera. Exercita o `TRedisSubscriber` inteiro: conexão
      dedicada, thread de leitura, reconexão com replay, padrões, RESP2 × RESP3.
      Armadilhas: **mensagem publicada com o assinante fora do ar está perdida** (a lib
      não finge fila local); callback lento segura o socket; `Execute` de dentro do
      callback é recusado na hora.
   5. `ReservaOtimistaVcl` — concorrência otimista. Exercita `Watch`/`Queue`/`TryCommit`
      com laço de repetição. Armadilha: **`EXEC` não é rollback** — erro de execução de
      um comando não desfaz os outros. Variações: sem `WATCH` (perde atualização, e isso
      é visível com duas threads) / com `WATCH` + retry / a mesma operação em Lua,
      atômica e sem retry.

   **Ordem: o `CacheAsideVcl` primeiro**, porque é ele que estabelece o esqueleto dual
   (form, marshal, worker, par `.dfm`/`.lfm`) que os outros quatro copiam.

   **Sample 1 concluído em 2026-08-23** — `samples/CacheAsideVcl`, validado nos **dois
   compiladores** contra o container. No FPC/LCL: `lazbuild` limpo e a bateria inteira
   dirigida por UI Automation, em texto claro e sobre TLS, conferindo **no servidor** (não
   só no log da app) que o `SET` simples zera o TTL (`-1`), que o `KEEPTTL` preserva, que
   o `DEL` some com a chave (`-2`), e que o lote com TTL fixo fica com **um** valor de TTL
   para as 20 chaves enquanto o lote com jitter espalha por sete. No Delphi 12, pela IDE,
   nas **duas** configurações — `Debug` (SChannel) em texto claro e sobre TLS, e `OpenSSL`
   sobre TLS —, incluindo as cinco consultas concorrentes e os dois lotes. Nenhum ajuste
   foi preciso depois da rodada Delphi (ao contrário do M7).

   **Sample 2 concluído em 2026-08-23** (validação FPC) — `samples/LockDistribuidoVcl`,
   copiado do esqueleto do `CacheAsideVcl`. Exercita `SET NX PX` (`Strings.SetWithOptions`
   com `Condition := scNotExists`), release seguro por `Scripting.Run` com um Lua
   compare-and-delete, renovação por outro Lua compare-and-`PEXPIRE`, e a armadilha do
   `DEL` direto — um "concorrente" simulado (checkbox + `TTimer` de 1 s, tentando `SET NX
   PX` com o próprio token sempre que a chave está livre) é quem torna a armadilha
   reproduzível: sem um segundo dono de verdade, "liberar lock alheio" seria só teoria.
   **Validado no FPC/LCL** com `lazbuild` limpo e uma bateria dirigida por mensagens Win32
   cruas (`EnumChildWindows` + `BM_CLICK`/`WM_GETTEXT`/`WM_SETTEXT` via P/Invoke do
   PowerShell — mesma ideia do `NativeWindowHandle` citado abaixo, só que dirigida de fora
   do processo) contra o container, conferindo **no servidor** via `redis-cli`: adquirir
   grava o token com o TTL certo (`PTTL` bate); liberar com o script apaga e some (`GET`
   retorna nil); com o concorrente ligado e o TTL vencendo, o concorrente assume a chave
   com o próprio token, e "Liberar com DEL direto" apaga exatamente o lock do concorrente,
   logando a armadilha com o token certo; com "Gerar token de posse" desmarcado, o botão
   "Liberar com script" fica desabilitado (`IsWindowEnabled` = False), forçando a via
   insegura; e a renovação automática mantém o lock vivo muito além do TTL original (2 s)
   por 5 s seguidos de `PEXPIRE`s bem-sucedidos. As legendas acentuadas (Conexão, Aquisição,
   Concorrência, Liberação, Renovação) renderizaram corretas a partir do `.lfm` em UTF-8,
   confirmando o par de escapes `.dfm` (`#231`, `#227`...)/`.lfm` (UTF-8 direto) descrito nas
   convenções gerais. **Validação no Delphi 12 pela IDE ainda não foi feita** — fica para o
   usuário, como de praxe (a Community Edition não compila por linha de comando).

   **Sample 3 concluído em 2026-08-23** (validação FPC) — `samples/FilaTarefasVcl`, também
   copiado do esqueleto do `CacheAsideVcl`. Exercita `XAdd`, `XGroupTryCreate`,
   `XReadGroupBlocking`, `XAck`, `XAutoClaim` e `XReadGroup` com `'0'`. A novidade de
   threading: cada consumidor é um **laço persistente** num worker do `RedisPool` (fica
   bloqueado em `XReadGroupBlocking`, acorda com tarefa ou timeout, repete até ser
   desligado), não uma operação avulsa — e cada ida ao servidor tem seu PRÓPRIO
   `UsarCliente`/`SoltarCliente`, nunca um só cobrindo o laço inteiro. É o que faz
   desconectar com um consumidor ligado esperar só a chamada corrente (no máximo
   `BLOCK_MS`), e não o laço inteiro.

   **Achado ao validar contra o servidor de verdade, que mudou o desenho da armadilha 2:**
   o plano original era mostrar `TRedisStreamEntry.IsDeleted` reivindicando pelo
   `XAUTOCLAIM` uma entrada apagada do stream por `XDEL`. Testado contra o container
   (Redis 7.2.16) via `redis-cli` isolado antes de mexer no sample, isso **não acontece**:
   no Redis 7+, `XAUTOCLAIM`/`XCLAIM` **purgam sozinhos** da PEL a entrada que já não
   existe mais no stream, e a reportam na TERCEIRA parte da resposta (`ADeletedIds`) —
   nunca no array principal com campos nulos. `IsDeleted` só aparece pra quem rele' a
   própria PEL direto (`XREADGROUP` com `'0'`, sem passar por `XCLAIM`/`XAUTOCLAIM`), que é
   exatamente o que o teste de integração do M8 (`EntradaApagada_ContinuaNaPelSemCampos`)
   já fazia — só que ele nunca chama `XAutoClaim`, e por isso a divergência não apareceu antes.
   Por causa disso o sample ganhou **dois** botões de recuperação, e não um: "Reivindicar
   (XAUTOCLAIM)" (usa o overload de 3 saídas, mostra a purga automática do Redis 7+ e
   dispensa `XACK` na entrada purgada) e "Retomar minha PEL" (mesmo consumidor, `XREADGROUP
   '0'`, é quem de fato mostra `IsDeleted = True`). Vale como lembrete do valor de testar
   contra o servidor ANTES de escrever a UI em vez de só depois: a suposição errada teria
   virado uma armadilha documentada errado no README.

   **Validado no FPC/LCL** com `lazbuild` limpo e a mesma bateria de mensagens Win32 cruas
   do sample 2, conferindo **no servidor** via `redis-cli`/`XPENDING`/`XINFO GROUPS`: `XADD`
   grava e os workers dividem o trabalho quando os dois estão ligados (`entries-read` bate
   com o total, `pending` volta a 0); "Matar" deixa exatamente uma pendência na PEL do
   consumidor certo; "Reivindicar" confirma a pendência normal e, na apagada, purga sem
   `XACK`; "Retomar" mostra a entrada apagada chegando `IsDeleted` de verdade; e a variação
   um × dois consumidores foi validada com os dois ligados ao mesmo tempo dividindo seis
   tarefas. **Validação no Delphi 12 pela IDE ainda não foi feita** — fica para o usuário.

   **Caixa de vazamento é o `Tests Leaked: 0` dos samples.** O `.dpr` liga
   `ReportMemoryLeaksOnShutdown` só no Delphi, então fechar a janela em silêncio é a prova
   de que cliente, marshals e work items foram todos liberados — inclusive no caminho de
   desconexão, que espera as operações em voo. Não apareceu caixa em nenhuma das duas
   configurações. Manter essa condicional em todos os samples do M9.

   O `--tls` do sample precisa do listener de pé, e o override **não sobe sozinho**
   (não define imagem): `docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d`.
   Sem ele, marcar TLS falha com `ERedisConnectionLost` e `10061 (recusou ativamente)` —
   que é diferente do sintoma de apontar TLS para porta plain (`ERedisTimeout` nomeando a
   troca de porta).

   Dois achados do esqueleto que valem para os próximos quatro samples: (a) o `.lpi`
   precisa de `GraphicApplication` em `Linking/Options/Win32`, senão o FPC linka o app
   como **console** e abre uma janela preta atrás da form — os samples `*Vcl` da
   `pascal-amqp-faa` têm esse defeito, então a correção não veio de lá; (b) a LCL não
   expõe provider de UI Automation, então os controles aparecem como `Pane` sem pattern
   nenhum: para dirigir a GUI por script, use o `NativeWindowHandle` com `BM_CLICK`, e
   leia o `TMemo` pela propriedade `Name` do elemento (`TLabel` não tem HWND e é
   invisível para a automação — por isso o log da app precisa dizer tudo que a tela diz).

   **Molde de cada sample** (herdado dos 9 samples `*Vcl` da `pascal-amqp-faa`):

   ```
   samples/<Nome>Vcl/
     <Nome>Vcl.dpr     IFDEF FPC: cthreads (UNIX) + Interfaces; ReportMemoryLeaksOnShutdown só no Delphi
     <Nome>Vcl.dproj   (Delphi)
     <Nome>Vcl.lpi     RequiredPackages: pascal_redis_faa + LCL; SyntaxMode Delphi;
                       UnitOutputDirectory lib\$(TargetCPU)-$(TargetOS)-$(LCLWidgetType)
     <Nome>Vcl.res
     u<Nome>Main.pas   fonte ÚNICO para os dois mundos
     u<Nome>Main.dfm + u<Nome>Main.lfm   dois arquivos de form mantidos em paralelo, à mão
   ```

   **Regras que valem em todo sample GUI deste projeto** (as quatro primeiras vêm dos
   gotchas da codebase dual; as duas últimas são específicas do Redis):

   - **Nada de rede na main thread.** O Redis é request/response: fora do pub/sub não há
     callback de entrega, e quem chama `Strings.Get` **bloqueia a thread chamadora**.
     Toda operação vai para um worker do `RedisPool` e volta para a UI por marshal
     descartável + `TThread.Queue` (o FPC não tem o overload de closure anônima).
     Isto é mais regra aqui do que na lib AMQP, não menos.
   - Evento de conexão postado por thread que morre logo depois precisa **saltar por um
     worker do `RedisPool`** — o FPC descarta o `TThread.Queue(nil, ...)` do postador
     morto.
   - Sob FPC, `uses LCLIntf, LCLType, LMessages` no lugar de `Windows, Messages`.
   - O `.lfm` não leva `Font.Charset` nem `TForm.DesignSize`; o resto é idêntico ao `.dfm`.
   - **`TRedisClient` é compartilhável entre threads** (ele É o pool) — é o que permite
     dois workers concorrentes no `FilaTarefasVcl`. Já uma `TRedisConnection` avulsa não é.
   - Comando bloqueante sai pelo **pool separado** (`ExecuteBlocking`), num worker
     dedicado; nunca da main thread.

   **Checklist de conclusão de cada sample:** `lazbuild <Nome>Vcl.lpi` limpo **e**
   compilação no Delphi 12 pela IDE (a CE não compila por linha de comando); rodado de
   fato contra o container; registrado em `Redis.lpg` **e** `Redis.groupproj`; descrito
   no `README.md` **e** no `README.en.md`; LF em tudo, inclusive `.lfm`/`.dfm`/`.lpi`;
   decisão nova (se houver) vira seção em `docs/DECISOES.md`. Todos os samples trazem
   host/porta/senha/db e um **checkbox TLS** (o `RedisDefaultTlsParams` troca porta e
   `UseTls` de uma vez) — TLS não é sample, é campo de tela.
10. **M9.1 — segunda rodada de samples (catálogo, não escopo aberto).** Mesma forma dual
    VCL/LCL e as mesmas regras de curadoria. Só abrir depois que a Onda A fechar, e
    escolhendo por lacuna de cobertura, não por vontade de ter mais exemplos.

    - **Onda B (padrões de alto valor):** `RateLimiterVcl` (janela fixa × deslizante ×
      token bucket num combo, mostrando a borda da janela fixa deixar passar 2× o limite);
      `RankingZSetVcl` (leaderboard: `ZIncrBy`, top-N, posição, empates);
      `SessaoHashVcl` (TTL é da chave e não do campo; `HSET` não renova prazo);
      `FilaListaVcl` (o **contraste** com Streams: `BLPop` sem ack perde a tarefa se o
      worker morre; `RPopLPush` + lista de processamento como remendo); `LoteVsRttVcl`
      (N comandos soltos × pipeline × `MULTI`, cronometrado).
    - **Onda C (menores, ferramentas e contratos da lib):** `NavegadorChavesVcl`
      (`Scan`/`KeyType`/`Ttl` + `HScan`/`SScan`/`ZScan`; por que `KEYS` trava o servidor e
      por que `SCAN` pode repetir chave); `BinarioVcl` (contrato binário: imagem em
      `TBytes`, CRLF no meio, `AsString` em bytes inválidos não levanta — o bug do M7);
      `ResilienciaVcl` (painel do pool: `CLIENT KILL`, descarte × devolução, timeout, e a
      decisão central — **comando em voo não é reexecutado**); `TagsSetsVcl`
      (`SInter`/`SDiff`/`SInterCard`); `AgendadosZSetVcl` (jobs por timestamp;
      `ZRangeByScore`+`ZRem` sem atomicidade entrega o job duas vezes).
    - **Avaliados e descartados** (com motivo, na seção 49 de `docs/DECISOES.md`):
      write-through/write-behind, contador de visitas, autocomplete por `ZRANGEBYLEX`,
      HyperLogLog/Bitmaps/Geo, TLS como sample, Cluster/Sentinel.
11. **M10 — Validação Linux x86_64 + ARM64** (mesma receita de container da lib AMQP) e
    READMEs completos com exemplos.
