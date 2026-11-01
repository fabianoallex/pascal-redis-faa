unit Redis.Transport;

{$I redis.inc}

{ O transporte da lib: o que sobrou aqui depois que TCP e TLS passaram a vir
  da pascal-common-faa (0.4.0; passo 3 do docs/transport-design.md de la',
  racional na secao 53 de docs/DECISOES.md).

  Ate' a 0.3.x esta unit era uma copia renomeada da AMQP.Transport (socket
  ssockets/System.Net.Socket com SO_RCVTIMEO/SO_SNDTIMEO), e as duas units de
  TLS eram copias das da lib AMQP. Hoje:

  - O socket e o TLS sao PcTcpConnect/PcTlsWrap (PascalCommon.Transport.Tcp e
    .Tls), e a conexao le e escreve por um TPcEndpointStream. Quem chama e'
    a Redis.Connection, que e' tambem a FRONTEIRA das excecoes (decisao T6):
    nenhuma EPcTransport* sai da lib; cada uma vira a ERedis* que ja' saia
    antes, com a mensagem da common (em ingles).
  - Os prazos de leitura e de escrita sao os do endpoint (SetReadDeadline/
    SetWriteDeadline), um por direcao, como eram SO_RCVTIMEO e SO_SNDTIMEO.
    Um so' para as duas direcoes nao serviria ao pub/sub: o leitor do
    assinante espera sem prazo (silencio e' o estado normal ali) enquanto quem
    manda um SUBSCRIBE precisa desistir se o servidor parar de ler.
  - O backend TLS e' escolhido em RUNTIME (TRedisParams.TlsBackend; decisao
    T3). REDIS_OPENSSL so' muda o padrao para OpenSSL.
  - O backoff de reconexao continua aqui na lib (T5): PcTcpConnect tenta cada
    endereco uma vez, e quem insiste e' o TRedisSubscriber (pub/sub) ou o
    chamador, nunca o transporte.

  Esta unit ficou com as excecoes publicas (que os chamadores ja' capturam),
  a traducao TRedisParams -> opcoes da common e o nome do backend. }

interface

uses
  SysUtils,
  PascalCommon.Transport,
  Redis.Types;

type
  /// Erro de socket plain (conectar, enviar, receber) fora do TLS.
  ///
  /// Desde a 0.4.0 a lib nao a levanta mais: falha de transporte sai da
  /// Redis.Connection como ERedisConnectionLost (ou ERedisTimeout), como ja'
  /// saia antes para quem usava a conexao, o pool ou o cliente. Fica
  /// declarada para nao quebrar quem a captura.
  ERedisTransport = class(Exception);

  /// Estourou o prazo de leitura/escrita. Mesma situacao da ERedisTransport:
  /// o que sai da lib e' ERedisTimeout.
  ERedisTransportTimeout = class(ERedisTransport);

  { Erros da camada TLS: handshake, certificado recusado (TlsVerifyPeer=True,
    TlsCaFile, mTLS), libssl/libcrypto nao encontrada em runtime, tbSChannel
    fora do Windows. E' a EPcTls da pascal-common-faa com a mesma mensagem.
    Sobe intacta do Open, sem virar ERedisConnectionLost: "o socket abriu e a
    criptografia nao fechou" leva a outra correcao (certificado, CA, porta). }
  ERedisTls = class(Exception);

/// O backend que tbDefault significa NESTE build: tbOpenSsl com
/// -dREDIS_OPENSSL; senao o padrao da plataforma (SChannel no Windows,
/// OpenSSL fora dele), ja' resolvido.
function RedisDefaultTlsBackend: TPcTlsBackend;

/// Opcoes de TCP que a conexao usa: ConnectTimeoutMs dos parametros, e
/// TCP_NODELAY (o padrao da common; pipeline e request/reply pagariam o
/// atraso do Nagle).
function RedisTcpOptions(const AParams: TRedisParams): TPcTcpOptions;

/// Opcoes de TLS que a conexao usa: CA, certificado do cliente, validacao,
/// backend, e o prazo do handshake = ReceiveTimeoutMs (zero = o padrao da
/// common, 15 s: um handshake sem prazo prenderia a thread para sempre num
/// listener que nunca responde, inclusive o do assinante de pub/sub, cuja
/// leitura nao tem prazo).
function RedisTlsOptions(const AParams: TRedisParams): TPcTlsOptions;

/// Nome do backend TLS padrao deste build: 'SChannel' ou 'OpenSSL'. Util para
/// exibir em UI/log qual motor um build usa.
function RedisTlsBackendName: string; overload;
/// Nome de um backend em particular (tbDefault resolvido como acima).
function RedisTlsBackendName(ABackend: TPcTlsBackend): string; overload;

/// Como RedisTlsBackendName, com o detalhe de runtime: no OpenSSL, versao e
/// biblioteca carregada (depois da 1a conexao TLS); no SChannel, a descricao
/// fixa. Antes de o OpenSSL carregar, devolve so' o nome.
function RedisTlsBackendInfo: string; overload;
function RedisTlsBackendInfo(ABackend: TPcTlsBackend): string; overload;

implementation

uses
  PascalCommon.Transport.Tls;

function RedisDefaultTlsBackend: TPcTlsBackend;
begin
  {$IFDEF REDIS_OPENSSL}
  Result := tbOpenSsl;
  {$ELSE}
  Result := PcTlsResolveBackend(tbDefault);
  {$ENDIF}
end;

function ResolveBackend(ABackend: TPcTlsBackend): TPcTlsBackend;
begin
  if ABackend = tbDefault then
    Result := RedisDefaultTlsBackend
  else
    Result := ABackend;
end;

function RedisTcpOptions(const AParams: TRedisParams): TPcTcpOptions;
begin
  // Registro zerado = o recomendado pela common (TCP_NODELAY, sem
  // keep-alive). So' o prazo do connect vem dos parametros.
  FillChar(Result, SizeOf(Result), 0);
  if AParams.ConnectTimeoutMs > 0 then
    Result.ConnectTimeoutMs := Cardinal(AParams.ConnectTimeoutMs);
end;

function RedisTlsOptions(const AParams: TRedisParams): TPcTlsOptions;
begin
  Result := Default(TPcTlsOptions);
  Result.CaFile := AParams.TlsCaFile;
  Result.CertFile := AParams.TlsCertFile;
  Result.KeyFile := AParams.TlsKeyFile;
  Result.CertPassword := AParams.TlsCertPassword;
  Result.SkipServerVerification := not AParams.TlsVerifyPeer;
  if AParams.ReceiveTimeoutMs > 0 then
    Result.HandshakeTimeoutMs := Cardinal(AParams.ReceiveTimeoutMs);
  Result.Backend := ResolveBackend(AParams.TlsBackend);
end;

function RedisTlsBackendName: string;
begin
  Result := RedisTlsBackendName(tbDefault);
end;

function RedisTlsBackendName(ABackend: TPcTlsBackend): string;
begin
  Result := PcTlsBackendName(ResolveBackend(ABackend));
end;

function RedisTlsBackendInfo: string;
begin
  Result := RedisTlsBackendInfo(tbDefault);
end;

function RedisTlsBackendInfo(ABackend: TPcTlsBackend): string;
begin
  Result := PcTlsBackendInfo(ResolveBackend(ABackend));
  if Result = '' then
    Result := RedisTlsBackendName(ABackend);
end;

end.
