# Certificado do Redis TLS de desenvolvimento

O `docker-compose.tls.yml` espera `server.crt` e `server.key` neste diretório.
Eles **não são versionados** (chave privada não vai para o repositório, mesmo
sendo de dev). Gere um par self-signed local com:

```
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout server.key -out server.crt \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost"
```

Como o certificado é self-signed, use `TlsVerifyPeer := False` no cliente
(é o que `TRedisConnectionParams.LocalhostTls` fará a partir do M5).

O `--tls-auth-clients no` do compose desliga o mTLS: o cliente não apresenta
certificado. mTLS está fora do escopo do v1.
