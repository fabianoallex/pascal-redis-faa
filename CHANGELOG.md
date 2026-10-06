# Changelog

O formato segue o [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/), e as versões
seguem o [Versionamento Semântico](https://semver.org/lang/pt-BR/). Enquanto a versão for 0.x,
uma versão minor pode mudar a API; toda mudança desse tipo aparece aqui.

## [Unreleased]

## [0.1.2] - 2026-10-05

### Alterado

- A versão mínima exigida da pascal-common-faa sobe de `1.0.0` para `1.2.0` (checagem na
  `Redis.Types` e `MinVersion` do `.lpk`), alinhada à versão que as suítes e os samples
  validam. Quem fornece uma cópia mais velha passa a ver a mensagem de erro na compilação.

## [0.1.1] - 2026-10-05

### Alterado

- Submódulo `external/pascal-common-faa` (testes, samples e scripts) sobe de `v1.1.0` para
  `v1.2.0`. A lib não usa o `PcProcessorCount` novo, então o mínimo exigido continua `1.0.0`
  (`Redis.Types` e `MinVersion` do `.lpk` sem mudança). No Linux o `PcPool` pode chegar a
  4 x núcleos threads em vez de 16; só os samples GUI o usam.

## [0.1.0] - 2026-10-04

Primeira versão publicada. Cliente Redis (RESP2/RESP3) para Delphi 12 e FPC 3.2.2/Lazarus numa
codebase só, escrito do zero a partir da especificação do protocolo. Fecha o v1 (M0–M8) e
traz os três primeiros samples GUI do M9.

### Adicionado

- **Kernel:** codec RESP2 e RESP3 binário-seguro (`IRedisReply`, sem `TValue`), conexão com
  handshake (`HELLO 3` ou `AUTH` + `CLIENT SETNAME` + `SELECT`), `Execute` genérico que
  alcança qualquer comando, pipeline, e conexão que sofreu timeout ou erro de I/O destruída
  em vez de devolvida.
- **Pool de conexões** (`TRedisPool`): teto com espera, descarte de conexão suja ou
  invalidada, health check por `PING`, poda por ociosidade; read/write timeout de socket, e
  pool separado para comandos bloqueantes.
- **Fachadas por família:** keys, strings, hashes, lists (com `BLPOP`/`BRPOP`/`BLMOVE`),
  sets, sorted sets, streams e consumer groups (`XADD` a `XAUTOCLAIM` e `XINFO`), scripting
  (`EVAL`/`EVALSHA` com cache de SHA e reenvio transparente no `NOSCRIPT`) e transações
  (`MULTI`/`EXEC`/`WATCH` num pipeline só).
- **Pub/Sub** (`TRedisSubscriber`): conexão dedicada, thread de leitura, callbacks em ordem,
  reconexão com replay das assinaturas; RESP2 por padrão e RESP3 opt-in.
- **TLS:** SChannel no Windows; OpenSSL em qualquer plataforma com `-dREDIS_OPENSSL`.
- Samples GUI duais VCL/LCL `CacheAsideVcl`, `LockDistribuidoVcl` e `FilaTarefasVcl`, o
  `SmokeTest` (151 passos; 160 com `--tls`), e suítes unitária (436) e de integração (69)
  espelhadas em DUnitX e FPCUnit.
- `tools/test_fpc.sh` (Windows) e `tools/test_fpc_docker.sh` (Linux, FPC 3.2.2 em
  container, um Redis próprio por execução): rodam as duas suítes com heaptrc e falham com
  qualquer bloco não liberado.

### Mudado

- **Dependência nova: [pascal-common-faa](https://github.com/fabianoallex/pascal-common-faa)
  1.0 ou mais nova** (fase F8 do plano dela). A `Redis.Threading` foi apagada; o que ela tinha
  vem de lá, sem alias: `RedisAtomic*` → `PcAtomic*`, `RedisTickMs` → `PcTickMs`
  (`PascalCommon.Threading`); `TRedisMonitor` → `TPcMonitor`, `TRedisWorkItem` →
  `TPcWorkItem`, `TRedisThreadPool` → `TPcThreadPool`, `RedisPool` → `PcPool`,
  `REDIS_WAIT_INFINITE` → `PC_WAIT_INFINITE` (`PascalCommon.ThreadPool`). `TRedisPool`, o pool
  de conexões, não mudou. O `pascal_redis_faa.lpk` exige `pascal_common_faa` pelo nome: a
  aplicação fornece a cópia, uma só para todas as libs `*-faa`. Uma cópia velha demais para o
  build com a mensagem da checagem em `Redis.Types`.
- O `PcPool` é **um pool para o processo inteiro**, dividido com as outras libs que o usam, e
  é criado na inicialização da pascal-common-faa (o `RedisPool` era criado no primeiro uso).
  A lib em si não enfileira nada nele.
- Os `.lpi` de teste do FPC passaram a ligar o heaptrc. Antes disso o FPC nunca tinha medido
  vazamento neste repositório (só o DUnitX); a medição deu 0 blocos nas duas suítes, no
  Windows e no Linux.

### Corrigido

- **Samples GUI: work item rodando depois de a form ser liberada no fechamento.** A LCL e a
  VCL liberam a form num exit proc, antes de qualquer finalização de unit, e o pool de
  threads só é liberado numa delas. O `FormCloseQuery` esperava apenas as operações em
  andamento no servidor; um item na fila, ou o consumidor do `FilaTarefasVcl` dormindo no
  processamento, rodava depois e usava a form liberada (medido: 2,6 s depois do
  `FormDestroy`, em silêncio). Agora todo work item é contado desde o enfileiramento até o
  destrutor, e o fechamento espera o contador zerar rodando os marshals pendentes. Anterior à
  migração: o `RedisPool` tinha o mesmo comportamento. Ver a seção 50 de `docs/DECISOES.md`.
- `CacheAsideVcl`: o atraso da fonte era lido do `TEdit` de dentro do worker; agora é lido na
  thread da UI, ao enfileirar.
- `LockDistribuidoVcl`: os timers do concorrente e da renovação enfileiravam trabalho mesmo
  com a janela fechando.

[Unreleased]: https://github.com/fabianoallex/pascal-redis-faa/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/fabianoallex/pascal-redis-faa/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/fabianoallex/pascal-redis-faa/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/fabianoallex/pascal-redis-faa/releases/tag/v0.1.0
