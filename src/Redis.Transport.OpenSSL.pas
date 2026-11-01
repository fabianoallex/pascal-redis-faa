unit Redis.Transport.OpenSSL;

{$I redis.inc}

{ OBSOLETA desde a 0.4.0; sai na proxima versao minor.

  O backend OpenSSL passou para a pascal-common-faa
  (PascalCommon.Transport.OpenSSL), junto com o resto do TLS (passo 3 do
  docs/transport-design.md de la'; secao 53 de docs/DECISOES.md aqui). Esta
  unit ficou so' para nao quebrar quem a lista num uses ou num .dproj (ela so'
  puxa a unit nova). A classe que existia aqui (TRedisOpenSslStream) nao tem
  equivalente publico: a Redis.Connection usa PcTlsWrap. Para escolher o
  backend use TRedisParams.TlsBackend := tbOpenSsl — no Windows nao e' mais
  preciso compilar com -dREDIS_OPENSSL para isso; a diretiva so' muda o
  padrao (RedisDefaultTlsBackend). }

interface

uses
  PascalCommon.Transport.OpenSSL;

implementation

end.
