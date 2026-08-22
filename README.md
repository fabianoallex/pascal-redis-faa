# pascal-redis-faa

Cliente **Redis** (protocolo RESP2/RESP3) para **Free Pascal/Lazarus e Delphi**, numa
única codebase, escrito do zero. Licença MIT.

> **Status: em construção (M0 — esqueleto).** A lib ainda tem apenas as camadas de
> transporte e concorrência. O roadmap completo está em `CLAUDE.md`.

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

Não precisam de servidor: o codec RESP é exercitado sobre uma fonte de bytes em
memória, que entrega a resposta em pedaços de tamanho controlado para reproduzir
leituras parciais de rede.

**FPC/Lazarus (FPCUnit):**

```
lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi
tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain
```

Sem parâmetros o executável abre a interface gráfica com a árvore de testes.

**Delphi (DUnitX):** abrir `Redis.groupproj` e compilar `Redis.UnitTests`.

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
