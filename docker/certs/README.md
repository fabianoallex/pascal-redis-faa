# PKI do Redis TLS de desenvolvimento

O `docker-compose.tls.yml` espera aqui uma CA privada e os certificados assinados por ela.
Eles **não são versionados** (chave privada não vai para o repositório, mesmo sendo de dev).
Gere tudo, a partir da raiz do repo, com:

```
sh tools/gen_certs.sh
```

O script roda o openssl num container (não precisa de openssl na máquina) e grava:

| Arquivo | O que é |
|---|---|
| `ca.crt` / `ca.key` | CA privada, `CN=pascal-redis-faa dev CA` |
| `server.crt` / `server.key` | servidor, `CN=localhost`, SAN `DNS:localhost` e `IP:127.0.0.1` |
| `client.crt` / `client.key` | cliente para mTLS (par PEM, backend OpenSSL) |
| `client.pfx` | o mesmo cliente em PFX, senha `redistest` (backend SChannel) |
| `rogue.crt` / `rogue.key` / `rogue.pfx` | cliente self-signed, fora da CA: o servidor recusa |

O servidor usa `tls-auth-clients optional`: cliente sem certificado entra, e quem apresenta
um tem de apresentar um da CA.

## Do lado do cliente

Use `RedisDefaultTlsParams` (já vem com a porta 6380 e `UseTls`) e aponte a CA, deixando a
validação ligada:

```pascal
LParams := RedisDefaultTlsParams;
LParams.TlsCaFile := 'docker/certs/ca.crt';
```

Com mTLS, acrescente o certificado do cliente no formato do backend:

```pascal
// OpenSSL: par PEM
LParams.TlsCertFile := 'docker/certs/client.crt';
LParams.TlsKeyFile := 'docker/certs/client.key';
// SChannel: PFX com senha
LParams.TlsCertFile := 'docker/certs/client.pfx';
LParams.TlsCertPassword := 'redistest';
```

Desligar a validação (`TlsVerifyPeer := False`) continua possível, mas não é mais preciso
contra este servidor. A lib **não** oferece um atalho que já venha com a validação desligada:
aceitar qualquer certificado anula a defesa contra man-in-the-middle, e essa escolha tem de
aparecer no diff de quem a fez.
