unit Redis.Transport.Tls;

{$I redis.inc}

{ OBSOLETA desde a 0.4.0; sai na proxima versao minor.

  O backend SChannel passou para a pascal-common-faa
  (PascalCommon.Transport.SChannel), junto com o resto do TLS (passo 3 do
  docs/transport-design.md de la'; secao 53 de docs/DECISOES.md aqui). Esta
  unit ficou so' para nao quebrar quem a lista num uses ou num .dproj (ela so'
  puxa a unit nova; Pascal nao re-exporta simbolos de um uses). A classe que
  existia aqui (TRedisSchannelStream) nao tem equivalente publico: a
  Redis.Connection usa PcTlsWrap. Para escolher o backend use
  TRedisParams.TlsBackend := tbSChannel. }

interface

{$IFDEF REDIS_WINDOWS}
uses
  PascalCommon.Transport.SChannel;
{$ENDIF}

implementation

end.
