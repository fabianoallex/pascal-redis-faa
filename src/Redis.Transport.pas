unit Redis.Transport;

{$I redis.inc}

{ PROVENIENCIA: copia renomeada de AMQP.Transport.pas (projeto ../pascal-amqp-faa),
  prefixos TRedis*/Redis*. Sem dependencia entre repositorios (mesmo padrao
  de Pipes.Threading.pas na pascal-pipes-faa): correcao de bug de um lado
  deve ser portada manualmente para o outro. }

{ Socket TCP cliente com a mesma superficie nos dois compiladores.

  TRedisTcpSocket esconde a diferenca de RTL:
  - Delphi: System.Net.Socket (TSocket), mesma escolha da pascal-amqp-faa.
  - FPC: ssockets (TInetSocket), que resolve DNS e conecta no construtor.

  Contrato usado por Redis.Connection:
  - Receive/Send bloqueantes; Receive devolve <= 0 em fim de stream/erro (o
    parser RESP trata como conexao encerrada).
  - SetReceiveTimeout/SetSendTimeout (SO_RCVTIMEO/SO_SNDTIMEO) limitam a espera.
    Estourado o prazo, Receive/Send levantam ERedisTransportTimeout — que nao e'
    o mesmo que devolver 0: fim de stream significa "o servidor encerrou", e
    timeout significa "o servidor ainda pode responder, mas eu desisti". Quem
    desistiu tem de DESTRUIR a conexao, porque a resposta atrasada chegaria no
    meio do proximo comando. Os timeouts podem ser ajustados antes ou depois do
    Connect: ficam guardados e sao reaplicados no socket.
  - Close e' thread-safe no sentido que importa aqui: chamado por OUTRA thread
    (timeout/teardown/reconexao) para desbloquear um Receive pendente da thread
    de leitura. No FPC usamos shutdown() — o handle so e' fechado no destrutor,
    evitando corrida de reuso de FD; no Delphi TSocket.Close ja tem esse papel
    (comportamento identico ao da pascal-amqp-faa). Close pode ser chamado mais
    de uma vez. }

interface

uses
  SysUtils,
  Classes
  {$IFDEF FPC}
  , ssockets
  {$ELSE}
  , System.Net.Socket
  {$ENDIF}
  ;

type
  /// Erro de socket plain (conectar, enviar, receber) fora do TLS —
  /// tipicamente host/porta inalcançável ou conexão derrubada pelo peer.
  ERedisTransport = class(Exception);

  { Estourou SO_RCVTIMEO/SO_SNDTIMEO. Subclasse de ERedisTransport para que quem
    só quer saber "deu erro de socket" continue funcionando sem mudar.

    Existe separada porque timeout NÃO é o mesmo que conexão encerrada: a
    resposta pode estar a caminho. A Redis.Connection traduz esta exceção para
    ERedisTimeout e invalida a conexão — devolvê-la ao pool contaminaria o
    próximo comando com a resposta atrasada deste. }
  ERedisTransportTimeout = class(ERedisTransport);

  { Erros da camada TLS (handshake, cifra, validação de cert). Declarada aqui —
    e não nas units de transporte TLS — para existir em TODA plataforma/build:
    Redis.Transport.Tls só compila sob REDIS_WINDOWS e Redis.Transport.OpenSSL só
    sob REDIS_OPENSSL, mas testes e chamadores precisam capturar ERedisTls sem
    depender dessas diretivas. Também é a exceção levantada por UseTls quando
    nenhum backend TLS foi compilado (fora do Windows, sem -dREDIS_OPENSSL) —
    a mensagem explica como habilitar. Nos backends reais: falha de
    handshake, cert rejeitado (TlsVerifyPeer=True) ou libssl/libcrypto não
    encontrada em runtime. }
  ERedisTls = class(Exception);

  TRedisTcpSocket = class
  private
    {$IFDEF FPC}
    FSock: TInetSocket;
    FShutdown: Boolean;
    {$ELSE}
    FSock: TSocket;
    {$ENDIF}
    FReceiveTimeoutMs: Integer;
    FSendTimeoutMs: Integer;
    /// Empurra os timeouts guardados para o socket (no-op se ainda nao ha um).
    procedure ApplyTimeouts;
  public
    constructor Create;
    destructor Destroy; override;
    /// Resolve o host e conecta (bloqueante). Levanta excecao em falha.
    procedure Connect(const AHost: string; APort: Word);
    /// Devolve os bytes lidos; 0 (ou negativo) = conexao encerrada.
    /// Levanta ERedisTransportTimeout se estourar o receive timeout.
    function Receive(var Buffer; ACount: Integer): Integer;
    /// Levanta ERedisTransportTimeout se estourar o send timeout.
    function Send(const Buffer; ACount: Integer): Integer;
    /// Encerra a conexao, desbloqueando um Receive pendente em outra thread.
    procedure Close;

    /// Teto de espera por bytes, em milissegundos. Zero desliga (espera
    /// indefinida). Pode ser chamado antes ou depois do Connect.
    ///
    /// Sem isto, um comando pendurado prende a conexao E a thread que chamou,
    /// para sempre. No AMQP o heartbeat detectava a morte; o Redis nao tem
    /// heartbeat, entao se o servidor emudecer no meio de uma resposta so' o
    /// timeout desata o no.
    procedure SetReceiveTimeout(AMilliseconds: Integer);

    /// Teto de espera para escoar o envio. Importa quando o servidor para de
    /// ler (janela TCP fechada) e o send bloqueia com o buffer cheio.
    procedure SetSendTimeout(AMilliseconds: Integer);
  end;

/// Backend TLS deste build (decidido em compilacao): 'OpenSSL', 'SChannel'
/// ou 'nenhum'. Util para exibir em UI/log qual motor um build usa.
function RedisTlsBackendName: string;

/// Como RedisTlsBackendName, mas com o detalhe de runtime quando o backend ja
/// carregou — o OpenSSL publica versao e biblioteca na 1ª conexao TLS (ex.:
/// 'OpenSSL 3.5.2 ... (libssl-3.dll)'). Antes disso, devolve so o nome.
function RedisTlsBackendInfo: string;

/// Uso interno dos backends TLS: publica o detalhe de RedisTlsBackendInfo.
procedure RedisSetTlsBackendDetail(const ADetail: string);

implementation

{$IFDEF FPC}
uses
  sockets
  {$IFDEF UNIX}
  , BaseUnix   // ESysEAGAIN: o errno de "estourou o SO_RCVTIMEO" no Unix
  {$ENDIF}
  ;

const
  REDIS_SHUT_RDWR = 2;
{$ENDIF}

{$IFDEF REDIS_WINDOWS}
const
  /// WSAETIMEDOUT. Declarado aqui porque a unit sockets do FPC no Windows nao
  /// exporta os codigos WSA*, e puxar a winsock2 inteira por uma constante
  /// custaria mais do que a linha abaixo.
  REDIS_ERR_TIMEOUT = 10060;
{$ENDIF}

var
  // Escrito uma vez pelo backend ao carregar (antes de qualquer leitura util:
  // so ha detalhe DEPOIS de uma conexao TLS abrir).
  GTlsBackendDetail: string = '';

function RedisTlsBackendName: string;
begin
  {$IFDEF REDIS_OPENSSL}
  Result := 'OpenSSL';
  {$ELSE}
    {$IFDEF REDIS_WINDOWS}
  Result := 'SChannel';
    {$ELSE}
  Result := 'nenhum';
    {$ENDIF}
  {$ENDIF}
end;

function RedisTlsBackendInfo: string;
begin
  Result := GTlsBackendDetail;
  if Result = '' then
    Result := RedisTlsBackendName;
end;

procedure RedisSetTlsBackendDetail(const ADetail: string);
begin
  GTlsBackendDetail := ADetail;
end;

constructor TRedisTcpSocket.Create;
begin
  inherited Create;
  {$IFNDEF FPC}
  FSock := TSocket.Create(TSocketType.TCP);
  {$ENDIF}
end;

destructor TRedisTcpSocket.Destroy;
begin
  try
    Close;
  except
  end;
  FSock.Free;
  inherited;
end;

procedure TRedisTcpSocket.Connect(const AHost: string; APort: Word);
begin
  {$IFDEF FPC}
  FSock := TInetSocket.Create(AHost, APort); // conecta no construtor
  {$ELSE}
  FSock.Connect(AHost, '', '', APort);
  {$ENDIF}
  // Depois de conectar, nao antes: no FPC o socket so' existe a partir daqui
  // (o TInetSocket conecta no proprio construtor) e no Delphi a RTL perde a
  // opcao se ela for aplicada antes (ver ApplyTimeouts).
  ApplyTimeouts;
end;

procedure TRedisTcpSocket.SetReceiveTimeout(AMilliseconds: Integer);
begin
  if AMilliseconds < 0 then
    AMilliseconds := 0;
  FReceiveTimeoutMs := AMilliseconds;
  ApplyTimeouts;
end;

procedure TRedisTcpSocket.SetSendTimeout(AMilliseconds: Integer);
begin
  if AMilliseconds < 0 then
    AMilliseconds := 0;
  FSendTimeoutMs := AMilliseconds;
  ApplyTimeouts;
end;

procedure TRedisTcpSocket.ApplyTimeouts;
{$IFDEF FPC}
var
  {$IFDEF REDIS_WINDOWS}
  LValue: LongWord;
  {$ELSE}
  LTime: TTimeVal;
  {$ENDIF}
{$ENDIF}
begin
  if FSock = nil then
    Exit;  // ainda nao conectou; o Connect reaplica
  {$IFDEF FPC}
    {$IFDEF REDIS_WINDOWS}
  // Winsock espera um DWORD de milissegundos; o Unix, um timeval. Passar a
  // forma errada nao da erro — o setsockopt aceita e o timeout simplesmente
  // nao acontece, que e' o pior desfecho possivel para este codigo.
  LValue := LongWord(FReceiveTimeoutMs);
  fpsetsockopt(FSock.Handle, SOL_SOCKET, SO_RCVTIMEO, @LValue, SizeOf(LValue));
  LValue := LongWord(FSendTimeoutMs);
  fpsetsockopt(FSock.Handle, SOL_SOCKET, SO_SNDTIMEO, @LValue, SizeOf(LValue));
    {$ELSE}
  LTime.tv_sec := FReceiveTimeoutMs div 1000;
  LTime.tv_usec := (FReceiveTimeoutMs mod 1000) * 1000;
  fpsetsockopt(FSock.Handle, SOL_SOCKET, SO_RCVTIMEO, @LTime, SizeOf(LTime));
  LTime.tv_sec := FSendTimeoutMs div 1000;
  LTime.tv_usec := (FSendTimeoutMs mod 1000) * 1000;
  fpsetsockopt(FSock.Handle, SOL_SOCKET, SO_SNDTIMEO, @LTime, SizeOf(LTime));
    {$ENDIF}
  {$ELSE}
  // A RTL do Delphi resolve a diferenca Windows/POSIX sozinha, mas SO' aplica
  // a opcao com um socket ja' aberto — e nem sempre.
  //
  // Armadilha da System.Net.Socket (conferida no fonte da 23.0): o
  // TSocket.Connect faz 'FSocket := CreateSocket', e e' DENTRO do CreateSocket
  // que a RTL empurra ReceiveTimeout/SendTimeout para o socket... usando o
  // campo FSocket, que naquele instante ainda vale InvalidSocket, porque so'
  // recebe o handle QUANDO o CreateSocket retorna. O setsockopt vai para um
  // handle invalido, falha, e a RTL descarta o resultado: o timeout se perde
  // em silencio. Pior, o setter guarda o valor, entao uma segunda atribuicao
  // igual (o 'FReceiveTimeout <> Value' do TSocket) vira no-op e nao ha'
  // segunda chance.
  //
  // Por isso a propriedade so' e' escrita com a conexao ja' de pe': ai' a
  // transicao 0 -> valor acontece com handle valido. O Connect chama este
  // metodo de novo justamente para isso. Sintoma de quando isto quebra: o
  // FPC estoura o timeout certinho e o Delphi espera o comando inteiro (foi
  // como apareceu, num BLPOP de 2 s com timeout de 300 ms).
  if not (TSocketState.Connected in FSock.State) then
    Exit;
  FSock.ReceiveTimeout := FReceiveTimeoutMs;
  FSock.SendTimeout := FSendTimeoutMs;
  {$ENDIF}
end;

{$IFDEF FPC}
/// True se o codigo de erro do socket significa "estourou o timeout".
function RedisIsTimeoutError(ACode: Integer): Boolean;
begin
  {$IFDEF REDIS_WINDOWS}
  Result := ACode = REDIS_ERR_TIMEOUT;
  {$ELSE}
  // No Unix, SO_RCVTIMEO estourado devolve EAGAIN (EWOULDBLOCK e' o mesmo
  // valor em todas as plataformas que o FPC suporta).
  Result := ACode = ESysEAGAIN;
  {$ENDIF}
end;
{$ENDIF}

function TRedisTcpSocket.Receive(var Buffer; ACount: Integer): Integer;
begin
  if FSock = nil then
    Exit(0);
  {$IFDEF FPC}
  Result := FSock.Read(Buffer, ACount);
  // Negativo pode ser timeout ou erro de verdade; so' o errno distingue. O
  // erro comum continua saindo como <= 0 (fim de stream), como antes.
  if (Result < 0) and RedisIsTimeoutError(FSock.LastError) then
    raise ERedisTransportTimeout.Create('estourou o receive timeout do socket');
  {$ELSE}
  try
    Result := FSock.Receive(Buffer, ACount);
  except
    // A RTL do Delphi levanta em vez de devolver negativo, e usa a MESMA
    // classe para timeout e para conexao derrubada — o codigo e' que separa.
    on E: ESocketError do
    begin
      {$IFDEF REDIS_WINDOWS}
      if E.Code = REDIS_ERR_TIMEOUT then
        raise ERedisTransportTimeout.Create(
          'estourou o receive timeout do socket');
      {$ENDIF}
      // Delphi fora do Windows nao e' alvo validado deste projeto (a CE nem
      // compila para Linux); la' o errno de timeout muda por plataforma —
      // EAGAIN e' 11 no Linux e 35 no Darwin — e chutar um valor seria pior
      // do que deixar o erro subir como falha de transporte. No FPC, que e' o
      // compilador do alvo Linux (M10), a classificacao usa ESysEAGAIN e sai
      // certa em qualquer Unix.
      raise;
    end;
  end;
  {$ENDIF}
end;

function TRedisTcpSocket.Send(const Buffer; ACount: Integer): Integer;
begin
  if FSock = nil then
    raise ERedisTransport.Create('socket nao conectado');
  {$IF Defined(FPC) and Defined(UNIX) and Declared(MSG_NOSIGNAL)}
  // MSG_NOSIGNAL: send() num socket ja encerrado devolve erro (EPIPE) em vez
  // de matar o processo com SIGPIPE. Essencial pro TLS: o destrutor do stream
  // manda close_notify best-effort mesmo quando o socket ja foi derrubado
  // (teardown/reconexao) — no Windows isso e' so um erro de send engolido;
  // no Linux, sem esta flag, era SIGPIPE fatal (visto no smoke test --tls).
  // Vale pra qualquer Unix cuja unit sockets declare a flag (Linux, BSDs);
  // no Darwin (sem MSG_NOSIGNAL) fica o caminho comum, sujeito a SIGPIPE.
  Result := fpsend(FSock.Handle, @Buffer, ACount, MSG_NOSIGNAL);
  if (Result < 0) and RedisIsTimeoutError(SocketError) then
    raise ERedisTransportTimeout.Create('estourou o send timeout do socket');
  {$ELSE}
    {$IFDEF FPC}
  Result := FSock.Write(Buffer, ACount);
  if (Result < 0) and RedisIsTimeoutError(FSock.LastError) then
    raise ERedisTransportTimeout.Create('estourou o send timeout do socket');
    {$ELSE}
  try
    Result := FSock.Send(Buffer, ACount);
  except
    on E: ESocketError do
    begin
      {$IFDEF REDIS_WINDOWS}
      if E.Code = REDIS_ERR_TIMEOUT then
        raise ERedisTransportTimeout.Create(
          'estourou o send timeout do socket');
      {$ENDIF}
      raise;
    end;
  end;
    {$ENDIF}
  {$ENDIF}
end;

procedure TRedisTcpSocket.Close;
begin
  if FSock = nil then
    Exit;
  {$IFDEF FPC}
  if not FShutdown then
  begin
    FShutdown := True;
    fpshutdown(FSock.Handle, REDIS_SHUT_RDWR);
  end;
  {$ELSE}
  if TSocketState.Connected in FSock.State then
    FSock.Close;
  {$ENDIF}
end;

end.
