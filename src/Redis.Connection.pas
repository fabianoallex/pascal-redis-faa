unit Redis.Connection;

{ Conexao com um servidor Redis: um socket, um lock, um leitor RESP.

  E' a peca central do kernel. Acima dela vem o pool (M3) e a fachada
  TRedisClient; abaixo, o transporte e o codec.

  O modelo de execucao e' o oposto do da pascal-amqp-faa e vale repetir aqui,
  porque quase tudo nesta unit decorre disso: NAO ha thread de leitura. Fora do
  pub/sub o servidor Redis so' fala quando perguntado, e as respostas voltam na
  ordem exata dos comandos — entao a propria thread que chamou Execute escreve
  o comando e le a resposta, segurando o lock da conexao. Zero handoff entre
  threads, zero fila de correlacao, e o erro de I/O chega em quem pode decidir o
  que fazer com ele. Thread de leitura so' existira' no pub/sub (M7).

  As tres regras que a conexao aplica sozinha (ver docs/DECISOES.md):

  1. Erro de I/O, timeout ou fluxo malformado INVALIDAM a conexao. Ela nao
     tenta se recuperar nem reenviar nada: fecha o socket e passa a recusar
     comandos. Quem estiver com ela na mao (o pool, no M3) tem que destrui-la.
  2. Comando em voo nao e' re-executado. INCR, LPUSH e SETNX nao sao
     idempotentes; repetir em silencio corromperia dados. A excecao sobe.
  3. Sobrou byte no buffer depois da ultima resposta => a conexao esta' SUJA
     (IsDirty). Nao levanta erro, porque a resposta atual esta' correta, mas a
     conexao nao pode voltar para o pool: o proximo comando leria a resposta do
     anterior. E' o bug classico de cliente Redis.

  Erro DE SERVIDOR e' outra coisa: um '-WRONGTYPE' e' uma resposta valida, a
  conexao continua sa'. Execute levanta ERedisReplyError nesse caso; ExecuteRaw
  devolve o no' rkError para quem prefere ramificar sem excecao. }

{$I redis.inc}

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Redis.Types,
  Redis.Resp,
  Redis.Transport;

type
  /// Adapta TRedisTcpSocket a TStream.
  ///
  /// Existe porque os dois backends TLS (SChannel e OpenSSL) envolvem um
  /// TStream, nao um socket: com esta casca, o caminho plain e o caminho TLS
  /// tem exatamente o mesmo tipo daqui para cima. NAO e' dono do socket.
  TRedisSocketStream = class(TStream)
  private
    FSocket: TRedisTcpSocket;
  public
    constructor Create(ASocket: TRedisTcpSocket);
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  end;

  /// Adapta um TStream ao IRedisByteSource que o TRedisReader consome.
  ///
  /// E' o ponto de encontro prometido no cabecalho da Redis.Resp: o codec nao
  /// conhece socket, e quem faz a ponte e' esta unit, que sabe o que fazer
  /// quando a leitura devolve zero. NAO e' dono do stream.
  TRedisStreamSource = class(TInterfacedObject, IRedisByteSource)
  private
    FStream: TStream;
  public
    constructor Create(AStream: TStream);
    function ReadBytes(var ABuffer; ACount: Integer): Integer;
  end;

  /// Lote de comandos enviados numa tacada so'.
  ///
  /// Pipeline no Redis nao e' transacao nem bloco atomico: e' so' economia de
  /// round-trip. Dez comandos enfileirados custam UMA ida e volta em vez de
  /// dez, o que numa rede de 1 ms transforma 10 ms em 1 ms. O servidor executa
  /// na ordem, e outro cliente pode intercalar comandos no meio — quem precisa
  /// de atomicidade usa MULTI/EXEC (M6) ou um script Lua.
  ///
  /// Os comandos sao codificados JA' no Queue, e o que a classe guarda e' o
  /// buffer pronto para o send. Assim nao ha' copia de argumentos nem posse
  /// duvidosa de TBytes ate' a hora do envio.
  TRedisPipeline = class
  private
    FBuffer: TBytes;
    FLength: Integer;
    FCount: Integer;
    procedure Append(const ABytes: TBytes);
  public
    constructor Create;

    /// Enfileira um comando. Mesma forma do Execute:
    /// Queue('SET', ['chave', 'valor', 'EX', 60]).
    procedure Queue(const AName: string; const AArgs: array of TRedisArg); overload;
    procedure Queue(const AName: string); overload;

    /// Enfileira um comando ja' com o nome dentro do array.
    procedure QueueArgs(const AArgs: array of TRedisArg);

    /// Esvazia o lote para reuso.
    procedure Clear;

    /// Quantos comandos estao enfileirados — e, portanto, quantas respostas o
    /// ExecutePipeline vai devolver.
    property Count: Integer read FCount;

    /// Bytes prontos para o socket. Uso interno da conexao.
    function ToBytes: TBytes;
  end;

  /// Uma conexao com o servidor.
  ///
  /// Ciclo de vida: Create -> Open -> Execute* -> Close/Free. Depois de
  /// invalidada (IsBroken) a conexao nao volta: crie outra. Isso e'
  /// deliberado — reabrir por baixo dos panos esconderia do chamador que houve
  /// perda de comando.
  ///
  /// Thread-safety: Execute, ExecuteRaw, ExecutePipeline, Open e Close sao
  /// serializados por um lock interno, entao chamar de varias threads e'
  /// seguro, so' que serial. O uso normal e' uma conexao por thread, tirada do
  /// pool. Abort e' a excecao proposital: nao pega o lock, justamente para
  /// conseguir interromper uma leitura pendurada.
  TRedisConnection = class
  private
    FParams: TRedisParams;
    FLock: TCriticalSection;
    FSocket: TRedisTcpSocket;
    FStream: TStream;
    FAdopted: TStream;        // stream injetado ainda nao usado (ver CreateOnStream)
    FWasAdopted: Boolean;     // nasceu de CreateOnStream: nao sabe abrir socket
    FSource: IRedisByteSource;
    FReader: TRedisReader;
    FOpen: Boolean;
    FBroken: Boolean;
    FDirty: Boolean;
    FDatabase: Integer;
    FNegotiated: TRedisProtocol;
    FServerVersion: string;
    FServerId: Int64;
    procedure EstablishTransport;
    procedure Teardown;
    procedure Handshake;
    procedure MarkBroken;
    procedure EnsureUsable;
    procedure WriteAll(const ABytes: TBytes);
    /// Escreve ACount comandos ja' codificados e le ACount respostas, mapeando
    /// qualquer falha de I/O para invalidacao + excecao da lib. NAO pega o
    /// lock e NAO levanta erro de servidor — isso e' com quem chama.
    function Perform(const ABytes: TBytes; ACount: Integer): TRedisReplyArray;
    function HandshakeStep(const ABytes: TBytes): IRedisReply;
    function GetIsUsable: Boolean;
  public
    constructor Create(const AParams: TRedisParams);

    /// Adota um TStream ja' conectado no lugar de abrir socket. O Open entao
    /// so' faz o handshake.
    ///
    /// Serve para testar a conexao inteira (handshake, Execute, pipeline,
    /// deteccao de conexao suja) contra um servidor falso em memoria, sem
    /// rede — e' o que as suites unitarias usam. A conexao passa a ser dona do
    /// stream. Uma conexao adotada nao pode ser reaberta depois de Close.
    constructor CreateOnStream(AStream: TStream; const AParams: TRedisParams);

    destructor Destroy; override;

    /// Conecta (se preciso) e faz o handshake: HELLO 3 ou AUTH, CLIENT SETNAME
    /// e SELECT, conforme os parametros. Falha no handshake fecha tudo antes
    /// de propagar a excecao — nao fica socket meio aberto.
    procedure Open;

    /// Fecha a conexao ordenadamente, esperando o comando em voo terminar.
    ///
    /// Nao emite QUIT: o comando so' pede ao servidor que feche, e o socket
    /// fechando ja' faz isso — enviar QUIT custaria mais um round-trip para
    /// ter o mesmo efeito.
    procedure Close;

    /// Derruba o socket SEM pegar o lock, para desbloquear uma leitura
    /// pendurada em outra thread (timeout, shutdown da app). A conexao fica
    /// invalidada: a thread que estava lendo recebe ERedisConnectionLost.
    procedure Abort;

    /// Executa um comando e devolve a resposta. Levanta ERedisReplyError se o
    /// servidor respondeu erro (a conexao continua sa'), ou
    /// ERedisConnectionLost / ERedisProtocolError se a conexao morreu ou o
    /// fluxo dessincronizou (a conexao e' invalidada).
    function Execute(const AName: string; const AArgs: array of TRedisArg): IRedisReply; overload;
    function Execute(const AName: string): IRedisReply; overload;

    /// Como Execute, mas com o nome do comando dentro do array — util para
    /// comandos de duas palavras montados dinamicamente
    /// (['CLIENT', 'SETNAME', LNome]).
    function ExecuteArgs(const AArgs: array of TRedisArg): IRedisReply;

    /// Como Execute, mas devolve o erro do servidor como um no' rkError em vez
    /// de levantar. Erro de conexao continua levantando — esse nao e'
    /// resposta.
    function ExecuteRaw(const AName: string; const AArgs: array of TRedisArg): IRedisReply; overload;
    function ExecuteRaw(const AName: string): IRedisReply; overload;

    /// Envia o lote inteiro numa escrita e devolve as respostas na ordem em
    /// que foram enfileiradas.
    ///
    /// NAO levanta ERedisReplyError: num lote, saber QUAL comando falhou
    /// importa mais do que abortar no primeiro erro — o servidor executou
    /// todos de qualquer jeito. Cada item pode ser um rkError; use
    /// IsError/RaiseIfError item a item.
    function ExecutePipeline(APipeline: TRedisPipeline): TRedisReplyArray;

    /// PING. Devolve True se o servidor respondeu PONG. Nao levanta em erro de
    /// servidor (mas levanta se a conexao morreu) — e' o health check que o
    /// pool do M3 chama antes de emprestar uma conexao ociosa.
    function Ping: Boolean;

    /// SELECT: troca o banco corrente e memoriza o novo valor (a reconexao do
    /// M3 replaya este SELECT, ja' que o banco vive na conexao, nao no
    /// servidor).
    procedure Select(ADatabase: Integer);

    /// Parametros com que a conexao foi criada.
    property Params: TRedisParams read FParams;

    /// True entre um Open bem-sucedido e o Close.
    property IsOpen: Boolean read FOpen;

    /// True depois de erro de I/O, timeout ou fluxo malformado. Conexao
    /// invalidada nao volta ao pool: e' destruida.
    property IsBroken: Boolean read FBroken;

    /// True quando sobrou byte nao consumido depois da ultima resposta. A
    /// resposta entregue estava certa, mas a conexao esta' contaminada.
    property IsDirty: Boolean read FDirty;

    /// Atalho para "aberta, inteira e limpa" — o que o pool checa no checkin.
    property IsUsable: Boolean read GetIsUsable;

    /// Banco selecionado nesta conexao.
    property Database: Integer read FDatabase;

    /// Protocolo REALMENTE em uso. So' vira rpRESP3 depois de um HELLO 3
    /// aceito pelo servidor.
    property NegotiatedProtocol: TRedisProtocol read FNegotiated;

    /// Versao do servidor, quando ele a informou no HELLO (RESP3). Em RESP2 a
    /// lib nao emite HELLO — servidor anterior ao 6.0 nao conhece o comando —
    /// entao fica vazia; quem quiser a versao em RESP2 usa INFO server.
    property ServerVersion: string read FServerVersion;

    /// Id da conexao no servidor (o mesmo que aparece no CLIENT LIST), quando
    /// informado no HELLO. Zero em RESP2.
    property ServerId: Int64 read FServerId;
  end;

implementation

const
  /// Nome de usuario usado no HELLO quando o parametro Username esta' vazio
  /// mas ha' senha. E' o usuario que o 'requirepass' classico configura.
  REDIS_DEFAULT_USER = 'default';

{ TRedisSocketStream }

constructor TRedisSocketStream.Create(ASocket: TRedisTcpSocket);
begin
  inherited Create;
  FSocket := ASocket;
end;

function TRedisSocketStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Receive(Buffer, Count);
end;

function TRedisSocketStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Send(Buffer, Count);
end;

function TRedisSocketStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  Result := 0;  // inalcancavel; so' cala o aviso de resultado nao atribuido
  raise ERedisException.Create('TRedisSocketStream nao suporta Seek');
end;

{ TRedisStreamSource }

constructor TRedisStreamSource.Create(AStream: TStream);
begin
  inherited Create;
  FStream := AStream;
end;

function TRedisStreamSource.ReadBytes(var ABuffer; ACount: Integer): Integer;
begin
  Result := FStream.Read(ABuffer, ACount);
  // O contrato do IRedisByteSource e' o do read(2): zero significa "acabou".
  // O socket pode devolver negativo em erro; normalizar aqui evita que o
  // leitor precise conhecer essa particularidade do transporte.
  if Result < 0 then
    Result := 0;
end;

{ TRedisPipeline }

constructor TRedisPipeline.Create;
begin
  inherited Create;
  FLength := 0;
  FCount := 0;
end;

procedure TRedisPipeline.Append(const ABytes: TBytes);
var
  LNeeded: Integer;
begin
  LNeeded := FLength + Length(ABytes);
  if LNeeded > Length(FBuffer) then
  begin
    // Cresce dobrando: um lote de mil comandos nao pode custar mil
    // realocacoes de um buffer que so' cresce.
    if Length(FBuffer) = 0 then
      SetLength(FBuffer, 1024);
    while LNeeded > Length(FBuffer) do
      SetLength(FBuffer, Length(FBuffer) * 2);
  end;
  if Length(ABytes) > 0 then
    Move(ABytes[0], FBuffer[FLength], Length(ABytes));
  FLength := LNeeded;
  Inc(FCount);
end;

procedure TRedisPipeline.Queue(const AName: string; const AArgs: array of TRedisArg);
begin
  Append(RedisEncodeCommand(AName, AArgs));
end;

procedure TRedisPipeline.Queue(const AName: string);
begin
  Append(RedisEncodeCommand(AName, []));
end;

procedure TRedisPipeline.QueueArgs(const AArgs: array of TRedisArg);
begin
  Append(RedisEncodeCommand(AArgs));
end;

procedure TRedisPipeline.Clear;
begin
  FLength := 0;
  FCount := 0;
end;

function TRedisPipeline.ToBytes: TBytes;
begin
  Result := Copy(FBuffer, 0, FLength);
end;

{ TRedisConnection }

constructor TRedisConnection.Create(const AParams: TRedisParams);
begin
  inherited Create;
  FParams := AParams;
  FLock := TCriticalSection.Create;
  FDatabase := AParams.Database;
  FNegotiated := rpRESP2;
end;

constructor TRedisConnection.CreateOnStream(AStream: TStream;
  const AParams: TRedisParams);
begin
  Create(AParams);
  FAdopted := AStream;
  FWasAdopted := True;
end;

destructor TRedisConnection.Destroy;
begin
  try
    Teardown;
  except
    // Destrutor nao propaga: se o socket ja' morreu, nao ha' nada a fazer aqui.
  end;
  FAdopted.Free;   // stream adotado e nunca aberto
  FLock.Free;
  inherited;
end;

procedure TRedisConnection.EstablishTransport;
begin
  if FAdopted <> nil then
  begin
    // Stream injetado (teste / transporte ja' pronto): assume a posse e zera o
    // campo, para um segundo Open falhar em vez de reusar um stream fechado.
    FStream := FAdopted;
    FAdopted := nil;
  end
  else
  begin
    if FParams.UseTls then
      // O handshake TLS entra no M5, junto com os certs de docker/certs.
      // Melhor recusar alto e claro do que abrir em texto claro uma conexao
      // que o chamador pediu cifrada.
      raise ERedisTls.Create(
        'TLS ainda nao esta ligado na conexao (chega no M5); use UseTls := False');
    FSocket := TRedisTcpSocket.Create;
    try
      FSocket.Connect(FParams.Host, FParams.Port);
    except
      on E: Exception do
      begin
        FreeAndNil(FSocket);
        raise ERedisConnectionLost.CreateFmt('nao conectou em %s:%d: %s',
          [FParams.Host, FParams.Port, E.Message]);
      end;
    end;
    FStream := TRedisSocketStream.Create(FSocket);
  end;
  FSource := TRedisStreamSource.Create(FStream);
  FReader := TRedisReader.Create(FSource);
end;

procedure TRedisConnection.Teardown;
begin
  if FSocket <> nil then
    FSocket.Close;
  FreeAndNil(FReader);
  // A interface segura o TRedisStreamSource, que aponta para o stream: soltar
  // a referencia ANTES de liberar o stream evita um source apontando para
  // memoria morta caso alguem ainda esteja com a interface na mao.
  FSource := nil;
  FreeAndNil(FStream);
  FreeAndNil(FSocket);
  FOpen := False;
end;

procedure TRedisConnection.MarkBroken;
begin
  FBroken := True;
  // Fecha na hora: pode haver resposta orfa a caminho, e um socket meio vivo
  // so' serviria para alguem tentar reusar.
  Teardown;
end;

procedure TRedisConnection.EnsureUsable;
begin
  if FBroken then
    raise ERedisConnectionLost.Create(
      'conexao invalidada por erro anterior; crie outra');
  if not FOpen then
    raise ERedisException.Create('conexao nao esta aberta');
end;

procedure TRedisConnection.WriteAll(const ABytes: TBytes);
var
  LSent, LTotal: Integer;
begin
  LTotal := 0;
  while LTotal < Length(ABytes) do
  begin
    LSent := FStream.Write(ABytes[LTotal], Length(ABytes) - LTotal);
    // Envio parcial e' normal em socket; zero (ou negativo) e' conexao morta.
    if LSent <= 0 then
      raise ERedisConnectionLost.Create('a conexao caiu ao enviar o comando');
    Inc(LTotal, LSent);
  end;
end;

function TRedisConnection.Perform(const ABytes: TBytes;
  ACount: Integer): TRedisReplyArray;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, ACount);
  try
    WriteAll(ABytes);
    for I := 0 to ACount - 1 do
      Result[I] := FReader.ReadReply;
    // Invariante da conexao limpa: lidas todas as respostas esperadas, o
    // buffer tem que estar vazio. O que sobrar e' resposta de um comando
    // anterior (o caso classico: comando que sofreu timeout e cuja resposta
    // chegou depois), e o proximo Execute leria essa sobra achando que e' a
    // resposta dele. Nao da' para consertar — da' para nao propagar.
    if FReader.Buffered > 0 then
      FDirty := True;
  except
    on E: ERedisReplyError do
      raise;      // resposta valida do servidor; a conexao continua sa'
    on E: ERedisTypeError do
      raise;
    on E: ERedisProtocolError do
    begin
      MarkBroken;  // fluxo dessincronizado: nao ha' como reencontrar o comeco
      raise;
    end;
    on E: ERedisConnectionLost do
    begin
      MarkBroken;
      raise;
    end;
    on E: ERedisTimeout do
    begin
      MarkBroken;
      raise;
    end;
    on E: Exception do
    begin
      // Sobra o que vem do transporte (ERedisTransport, ERedisTls, erro de
      // socket da RTL). Do ponto de vista de quem chamou e' tudo a mesma
      // coisa: o comando nao completou e a conexao acabou. Traduzir aqui
      // poupa o chamador de conhecer as excecoes das camadas de baixo.
      MarkBroken;
      raise ERedisConnectionLost.CreateFmt('%s: %s', [E.ClassName, E.Message]);
    end;
  end;
end;

function TRedisConnection.HandshakeStep(const ABytes: TBytes): IRedisReply;
var
  LReplies: TRedisReplyArray;
begin
  // Roda pelo mesmo Perform dos comandos normais (mesma invalidacao, mesma
  // traducao de excecao), so' que sem o lock: o Open ja' o esta' segurando.
  LReplies := Perform(ABytes, 1);
  Result := LReplies[0];
end;

procedure TRedisConnection.Handshake;
var
  LReply: IRedisReply;
  LField: IRedisReply;
  LUser: string;
begin
  FNegotiated := rpRESP2;
  FServerVersion := '';
  FServerId := 0;

  if FParams.Protocol = rpRESP3 then
  begin
    // HELLO autentica e troca o protocolo de uma vez, e e' o unico jeito de
    // subir para RESP3. Existe a partir do Redis 6.0.
    LUser := FParams.Username;
    if LUser = '' then
      LUser := REDIS_DEFAULT_USER;
    if FParams.Password <> '' then
      LReply := HandshakeStep(RedisEncodeCommand('HELLO',
        [3, 'AUTH', LUser, FParams.Password]))
    else
      LReply := HandshakeStep(RedisEncodeCommand('HELLO', [3]));

    if LReply.IsError then
    begin
      if Pos('unknown command', LowerCase(LReply.ErrorMessage)) > 0 then
        raise ERedisException.Create(
          'o servidor nao conhece HELLO (anterior ao Redis 6.0): RESP3 nao ' +
          'esta disponivel; use Protocol := rpRESP2');
      LReply.RaiseIfError;
    end;

    // A resposta do HELLO e' um mapa. Como o M1 guarda mapa achatado, o
    // ValueByKey funciona aqui igual a como funcionaria num HGETALL de RESP2.
    LField := LReply.ValueByKey('proto');
    if (LField = nil) or (LField.AsInteger <> 3) then
      raise ERedisProtocolError.Create(
        'o servidor aceitou HELLO mas nao confirmou proto 3');
    FNegotiated := rpRESP3;
    LField := LReply.ValueByKey('version');
    if LField <> nil then
      FServerVersion := LField.AsString;
    LField := LReply.ValueByKey('id');
    if LField <> nil then
      FServerId := LField.AsInteger;
  end
  else if FParams.Password <> '' then
  begin
    // RESP2: nada de HELLO, para nao exigir Redis 6 de quem nao pediu RESP3.
    // Com Username vazio vai o AUTH de um argumento so' — a forma que o
    // requirepass entende e a unica que servidor antigo aceita.
    if FParams.Username <> '' then
      LReply := HandshakeStep(RedisEncodeCommand('AUTH',
        [FParams.Username, FParams.Password]))
    else
      LReply := HandshakeStep(RedisEncodeCommand('AUTH', [FParams.Password]));
    LReply.RaiseIfError;
  end;

  if FParams.ClientName <> '' then
  begin
    LReply := HandshakeStep(RedisEncodeCommand('CLIENT',
      ['SETNAME', FParams.ClientName]));
    LReply.RaiseIfError;
  end;

  if FParams.Database <> REDIS_DEFAULT_DATABASE then
  begin
    LReply := HandshakeStep(RedisEncodeCommand('SELECT', [FParams.Database]));
    LReply.RaiseIfError;
  end;
  FDatabase := FParams.Database;
end;

procedure TRedisConnection.Open;
begin
  FLock.Enter;
  try
    // Invalidada primeiro, aberta depois: uma conexao que o Abort derrubou
    // ainda pode estar com o socket na mao, e sair calado daqui devolveria ao
    // chamador uma conexao morta com cara de aberta.
    if FBroken then
      raise ERedisConnectionLost.Create(
        'conexao invalidada por erro anterior; crie outra');
    if FOpen then
      Exit;
    // Conexao adotada nao sabe abrir socket: o stream veio pronto de fora e,
    // uma vez fechado, nao ha' de onde tirar outro. Sem esta guarda, reabrir
    // uma conexao de teste sairia conectando no servidor de verdade.
    if FWasAdopted and (FAdopted = nil) then
      raise ERedisException.Create(
        'conexao adotada de um stream nao pode ser reaberta');
    EstablishTransport;
    try
      Handshake;
    except
      // Handshake que falha (senha errada, banco inexistente, servidor sem
      // HELLO) nao pode deixar socket aberto para tras.
      Teardown;
      raise;
    end;
    FOpen := True;
  finally
    FLock.Leave;
  end;
end;

procedure TRedisConnection.Close;
begin
  FLock.Enter;
  try
    Teardown;
  finally
    FLock.Leave;
  end;
end;

procedure TRedisConnection.Abort;
begin
  // De proposito SEM o lock: quem chama isto quer justamente interromper a
  // thread que esta' segurando o lock, parada num Receive. Fechar o socket faz
  // a leitura devolver zero, e ela sobe como ERedisConnectionLost la'.
  //
  // So' marca e derruba o socket; NAO libera reader nem stream, que a outra
  // thread pode estar usando neste instante. A faxina fica para o Perform que
  // falhar (MarkBroken), para o Close ou para o destrutor — todos sob o lock.
  FBroken := True;
  FOpen := False;
  if FSocket <> nil then
    FSocket.Close;
end;

function TRedisConnection.Execute(const AName: string;
  const AArgs: array of TRedisArg): IRedisReply;
begin
  Result := ExecuteRaw(AName, AArgs);
  Result.RaiseIfError;
end;

function TRedisConnection.Execute(const AName: string): IRedisReply;
begin
  Result := Execute(AName, []);
end;

function TRedisConnection.ExecuteArgs(const AArgs: array of TRedisArg): IRedisReply;
var
  LReplies: TRedisReplyArray;
begin
  FLock.Enter;
  try
    EnsureUsable;
    LReplies := Perform(RedisEncodeCommand(AArgs), 1);
  finally
    FLock.Leave;
  end;
  Result := LReplies[0];
  Result.RaiseIfError;
end;

function TRedisConnection.ExecuteRaw(const AName: string;
  const AArgs: array of TRedisArg): IRedisReply;
var
  LReplies: TRedisReplyArray;
begin
  FLock.Enter;
  try
    EnsureUsable;
    LReplies := Perform(RedisEncodeCommand(AName, AArgs), 1);
  finally
    FLock.Leave;
  end;
  Result := LReplies[0];
end;

function TRedisConnection.ExecuteRaw(const AName: string): IRedisReply;
begin
  Result := ExecuteRaw(AName, []);
end;

function TRedisConnection.ExecutePipeline(APipeline: TRedisPipeline): TRedisReplyArray;
begin
  Result := nil;
  if APipeline = nil then
    raise ERedisException.Create('pipeline nulo');
  // Lote vazio nao vai ao servidor: enviar zero byte e ler zero resposta seria
  // um no-op com cara de round-trip.
  if APipeline.Count = 0 then
  begin
    SetLength(Result, 0);
    Exit;
  end;
  FLock.Enter;
  try
    EnsureUsable;
    Result := Perform(APipeline.ToBytes, APipeline.Count);
  finally
    FLock.Leave;
  end;
end;

function TRedisConnection.Ping: Boolean;
var
  LReply: IRedisReply;
begin
  LReply := ExecuteRaw('PING', []);
  Result := (not LReply.IsError) and (UpperCase(LReply.AsString) = 'PONG');
end;

procedure TRedisConnection.Select(ADatabase: Integer);
begin
  Execute('SELECT', [ADatabase]);
  // So' depois do OK: se o SELECT falhar, o banco corrente continua sendo o
  // anterior, e e' esse que a reconexao do M3 tem que replayar.
  FDatabase := ADatabase;
end;

function TRedisConnection.GetIsUsable: Boolean;
begin
  Result := FOpen and (not FBroken) and (not FDirty);
end;

end.
