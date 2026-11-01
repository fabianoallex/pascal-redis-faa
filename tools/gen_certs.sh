#!/bin/sh
# Gera a PKI de desenvolvimento que o docker/docker-compose.tls.yml usa, em
# docker/certs (nao versionado: chave privada nao vai para o repositorio, nem
# de dev). Roda o openssl num container, entao nao precisa de openssl na
# maquina. Sobrescreve o que houver.
#
#   ca.crt / ca.key            CA privada ("pascal-redis-faa dev CA")
#   server.crt / server.key    servidor, CN=localhost, SAN DNS:localhost e
#                              IP:127.0.0.1, assinado pela CA
#   client.crt / client.key    cliente (mTLS), CN=pascal-redis-faa-client,
#                              EKU clientAuth, assinado pela CA — o par PEM
#                              que o backend OpenSSL usa
#   client.pfx                 o mesmo cliente em PFX, senha "redistest" —
#                              o formato que o backend SChannel usa (T7)
#   rogue.crt / .key / .pfx    cliente self-signed, fora da CA: o servidor
#                              tem de recusar (prova que o certificado do
#                              cliente vai mesmo no handshake)
#
# Por que uma CA e nao mais um self-signed: desde a 0.4.0 a lib valida o
# servidor contra uma CA privada (TRedisParams.TlsCaFile) e apresenta
# certificado de cliente; o SmokeTest --tls exercita as duas coisas.
#
# Uso (a partir da raiz do repo):   sh tools/gen_certs.sh
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CERTS="$ROOT/docker/certs"
mkdir -p "$CERTS"
# Git for Windows converteria o "/certs" do -v num caminho do Windows.
export MSYS_NO_PATHCONV=1

docker run --rm --entrypoint sh -v "$CERTS:/certs" -w /certs alpine/openssl -c '
set -e
cat > server.ext <<EOF
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
cat > client.ext <<EOF
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout ca.key -out ca.crt -subj "/CN=pascal-redis-faa dev CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 -extfile server.ext
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
  -subj "/CN=pascal-redis-faa-client"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 3650 -extfile client.ext
openssl pkcs12 -export -in client.crt -inkey client.key -out client.pfx \
  -passout pass:redistest
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout rogue.key -out rogue.crt -subj "/CN=pascal-redis-faa-rogue" \
  -addext "extendedKeyUsage=clientAuth"
openssl pkcs12 -export -in rogue.crt -inkey rogue.key -out rogue.pfx \
  -passout pass:redistest
rm -f server.csr client.csr server.ext client.ext ca.srl
# O redis roda como uid 999 e o openssl grava a chave como 600/root: sem
# isto o servidor aborta ao ler o keyfile.
chmod 644 server.key client.key client.pfx rogue.key rogue.pfx
'
ls "$CERTS"
