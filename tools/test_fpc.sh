#!/bin/sh
# Compila (lazbuild) e roda as suites FPCUnit no Windows. Criterio: 0 erros,
# 0 falhas e "0 unfreed memory blocks" no arquivo do heaptrc (ligado nos .lpi
# de teste desde a migracao para a pascal-common-faa; antes dela o FPC nunca
# mediu vazamento neste repo, so' o DUnitX). No Linux: tools/test_fpc_docker.sh.
#
# A suite de integracao precisa do Redis de pe' em localhost:6379
# (docker compose -f docker/docker-compose.yml up -d); a unitaria nao.
#
# Os .lpi de teste pegam a pascal-common-faa do submodulo external/ (Prefer),
# nao de um pacote registrado no IDE; confira no build.log em caso de duvida.
#
# O lado Delphi nao tem equivalente por linha de comando (o Delphi Community
# Edition nao compila fora do IDE): rode tests\Unit\Redis.UnitTests.dproj e
# tests\Integration\Redis.IntegrationSuite.dproj pelo IDE.
#
# Variaveis:
#   SUITES     "unit", "integration" ou as duas (padrao).
#   BUILDMODE  build mode do lazbuild, ex.: openssl (padrao: o Default).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LAZBUILD="${LAZBUILD:-lazbuild}"
command -v "$LAZBUILD" >/dev/null 2>&1 || LAZBUILD=/c/lazarus4.0/lazbuild.exe
SUITES="${SUITES:-unit integration}"
MODE=""
[ -n "$BUILDMODE" ] && MODE="--build-mode=$BUILDMODE"

[ -f "$ROOT/external/pascal-common-faa/src/PascalCommon.ThreadPool.pas" ] \
  || { echo "external/pascal-common-faa vazio: git submodule update --init external/pascal-common-faa"; exit 1; }

FAIL=0
for S in $SUITES; do
  case $S in
    unit) D="$ROOT/tests/Unit/fpc"; P=RedisUnitTestsFpc ;;
    integration) D="$ROOT/tests/Integration/fpc"; P=RedisIntegrationTestsFpc ;;
    *) echo "suite desconhecida: $S"; exit 2 ;;
  esac
  cd "$D"
  if ! "$LAZBUILD" -B $MODE $P.lpi > build.log 2>&1; then
    grep -E "Error|Fatal" build.log | grep -v "generics\." | head -30
    exit 1
  fi
  rm -f heap.txt
  HEAPTRC="log=heap.txt" ./$P.exe --all --format=plain > run.log 2>&1 || true
  echo "== $S"
  grep -E "^Number of" run.log
  grep "unfreed" heap.txt 2>/dev/null || echo "heap.txt sem resumo do heaptrc"
  grep -qE "^Number of errors: +0$" run.log && grep -qE "^Number of failures: +0$" run.log \
    && grep -qE "^0 unfreed memory blocks" heap.txt || FAIL=1
done
exit $FAIL
