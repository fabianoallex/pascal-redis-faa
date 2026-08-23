# Certificado do Redis TLS de desenvolvimento

O `docker-compose.tls.yml` espera `server.crt` e `server.key` neste diretório.
Eles **não são versionados** (chave privada não vai para o repositório, mesmo
sendo de dev). Gere um par self-signed local com:

```
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout server.key -out server.crt \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost"
```

No Windows sem openssl instalado, use a imagem do compose (a partir de
`docker/`). O `chmod 644` é **essencial**: o openssl gera a chave como 600/root
e o redis roda como uid 999 — sem ele o servidor aborta ao ler o keyfile.

```
docker run --rm -v "${PWD}/certs:/certs" alpine/openssl req -x509 -newkey rsa:2048 -nodes -keyout /certs/server.key -out /certs/server.crt -days 3650 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost"
docker run --rm -v "${PWD}/certs:/certs" alpine chmod 644 /certs/server.key
```

## Do lado do cliente

Use `RedisDefaultTlsParams` (já vem com a porta 6380 e `UseTls`) e desligue a
validação **numa linha explícita**, porque o certificado é self-signed:

```pascal
LParams := RedisDefaultTlsParams;
LParams.TlsVerifyPeer := False;   // só em desenvolvimento
```

A lib **não** oferece um atalho que já venha com a validação desligada, e isso é
proposital: aceitar qualquer certificado anula a defesa contra
man-in-the-middle e deixa só a cifragem do canal. Essa escolha tem de aparecer
no diff de quem a fez, não vir escondida num nome de função.

O `--tls-auth-clients no` do compose desliga o mTLS: o cliente não apresenta
certificado. mTLS está fora do escopo do v1.
