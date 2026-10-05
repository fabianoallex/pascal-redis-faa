#!/bin/sh
# Compila e roda as suites FPCUnit (unitaria e de integracao) no FPC 3.2.2 do
# Linux, dentro de um container Docker, com heaptrc. Criterio: 0 erros,
# 0 falhas e "0 unfreed memory blocks" em TODAS as rodadas.
#
# O repositorio e' montado somente-leitura e copiado dentro do container: nada
# e' escrito na arvore de trabalho. Sem Lazarus: o runner FPCUnit so' puxa a
# LCL no Windows. A pascal-common-faa vem do submodulo external/ (checkout sem
# --recursive basta; ela nao precisa do submodulo dela).
#
# A suite de integracao procura o Redis em localhost:6379. Cada execucao deste
# script sobe um Redis PROPRIO (redis:7.2-alpine, o mesmo do
# docker/docker-compose.yml) e roda o FPC dentro da rede dele (--network
# container:...), entao o localhost dos testes e' esse Redis. Assim varios
# scripts ao mesmo tempo nao disputam o mesmo servidor -- a suite faz
# CLIENT KILL e conta assinantes com PUBSUB NUMSUB, e uma rodada vizinha
# mudaria as contas.
#
# Variaveis:
#   FPC_IMAGE  imagem com FPC 3.2.2 no PATH (padrao: fpc322-bookworm).
#   SUITES     "unit", "integration" ou "unit integration" (padrao: as duas).
#   RUNS       quantas vezes rodar cada suite no mesmo container (padrao: 1).
#   FPCOPT     opcoes extras do fpc, ex.: -dREDIS_OPENSSL.
#   CPUS       limite de CPU do container do FPC (docker --cpus), ex.: 1. Com
#              varios containers ao mesmo tempo e CPUS=1 e' como as corridas
#              de concorrencia aparecem (mesma pratica da pascal-common-faa).
#
# Nota: no Debian o heaptrc so' escreve o resumo com um arquivo de log
# (HEAPTRC=log=...); sem ele nao imprime nada, nem com vazamento
# (gotcha 5 da pascal-common-faa).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${FPC_IMAGE:-fpc322-bookworm}"
SUITES="${SUITES:-unit integration}"
RUNS="${RUNS:-1}"
MOUNT="$ROOT"
command -v cygpath >/dev/null 2>&1 && MOUNT="$(cygpath -w "$ROOT")"
CPUFLAG=""
[ -n "$CPUS" ] && CPUFLAG="--cpus=$CPUS"

[ -f "$ROOT/external/pascal-common-faa/src/PascalCommon.ThreadPool.pas" ] \
  || { echo "external/pascal-common-faa vazio: git submodule update --init external/pascal-common-faa"; exit 1; }

REDIS="pascal-redis-faa-ci-$$-$(date +%s)"
trap 'docker rm -f "$REDIS" >/dev/null 2>&1 || true' EXIT
docker run -d --rm --name "$REDIS" redis:7.2-alpine \
  redis-server --appendonly no --save "" >/dev/null
until docker exec "$REDIS" redis-cli ping 2>/dev/null | grep -q PONG; do sleep 1; done

MSYS_NO_PATHCONV=1 docker run --rm $CPUFLAG --network "container:$REDIS" \
  -e SUITES="$SUITES" -e RUNS="$RUNS" -e FPCOPT="$FPCOPT" \
  -v "$MOUNT:/src:ro" "$IMAGE" sh -c '
  set -e
  mkdir -p /t/external /t/u /t/i && cp -r /src/src /src/tests /t/
  cp -r /src/external/pascal-common-faa /t/external/
  C=/t/external/pascal-common-faa/src
  FAIL=0
  for S in $SUITES; do
    case $S in
      unit) D=/t/tests/Unit/fpc; P=RedisUnitTestsFpc; U=/t/u ;;
      integration) D=/t/tests/Integration/fpc; P=RedisIntegrationTestsFpc; U=/t/i ;;
      *) echo "suite desconhecida: $S"; exit 2 ;;
    esac
    cd $D
    fpc -v0 -Mdelphi $FPCOPT -Fu/t/src -Fi/t/src -Fu$C -Fi$C -FU$U -gh -gl -o$U/runner $P.lpr > $U/build.log 2>&1 \
      || { grep -iE "error|fatal" $U/build.log | head -30; exit 1; }
    N=1
    while [ $N -le $RUNS ]; do
      rm -f $U/heap.txt
      HEAPTRC="log=$U/heap.txt" $U/runner --all --format=plain > $U/run.log 2>&1 || true
      T=$(grep -E "^Number of run tests" $U/run.log | grep -oE "[0-9]+" || echo "?")
      OK=1
      grep -qE "^Number of errors: +0$" $U/run.log || OK=0
      grep -qE "^Number of failures: +0$" $U/run.log || OK=0
      grep -qE "^0 unfreed memory blocks" $U/heap.txt || OK=0
      if [ $OK = 1 ]; then
        echo "$S rodada $N: ok ($T testes, 0 vazamentos)"
      else
        FAIL=1
        echo "$S rodada $N: FALHOU"
        grep -E "^Number of" $U/run.log || true
        grep "unfreed" $U/heap.txt || echo "heap.txt sem resumo do heaptrc"
        grep -B1 -A3 "Message:" $U/run.log | head -40 || true
      fi
      N=$((N + 1))
    done
  done
  exit $FAIL'
