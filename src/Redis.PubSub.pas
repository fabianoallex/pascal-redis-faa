unit Redis.PubSub;

{ Pub/Sub: a assinatura de canais e a thread que escuta.

  Esta e' a unica parte da lib em que o servidor fala sem ser perguntado, e por
  isso a unica com thread de leitura. Todo o resto (Execute, pipeline,
  transacao) e' pergunta-resposta na propria thread do chamador — ver o
  cabecalho da Redis.Connection.

  O que o Redis entrega aqui e' fire-and-forget, e a lib nao vai fingir o
  contrario: mensagem publicada enquanto o assinante estava desconectado esta'
  PERDIDA — sem fila, sem replay, sem confirmacao. Quem precisa de entrega
  garantida usa Streams com consumer group (M8), nao pub/sub.

  Tres formas de assinar:

  - SUBSCRIBE canal      — nome exato.
  - PSUBSCRIBE padrao    — glob ('noticias.*'), casado pelo servidor.
  - SSUBSCRIBE canal     — canal "shardado" (Redis 7+), que em cluster fica
                           preso ao shard do slot em vez de ser espalhado por
                           todos os nos. Fora de cluster funciona igual ao
                           SUBSCRIBE, e e' por isso que da' para exercitar sem
                           cluster.

  RESP2 x RESP3. Em RESP2 o SUBSCRIBE SEQUESTRA a conexao: dali em diante o
  servidor so' aceita comandos de assinatura, PING, RESET e QUIT — qualquer
  outro e' recusado. Em RESP3 (HELLO 3) as mensagens vem por um tipo proprio, o
  push '>', e a conexao continua aceitando comandos normais. O TRedisSubscriber
  fala os dois: em RESP2 ele recusa na hora o comando que o servidor recusaria,
  citando o motivo; em RESP3 ele deixa passar.

  Nos dois protocolos a conexao e' DEDICADA e fica fora do pool. Em RESP2 nao
  ha' escolha; em RESP3 seria possivel compartilhar, mas ai' uma mensagem
  publicada no meio de um GET chegaria antes da resposta do GET, e o kernel
  inteiro teria de aprender a ignorar push no meio de uma leitura. O ganho nao
  paga o risco. Ver docs/DECISOES.md.

  ORDEM E CALLBACK. O OnMessage roda NA THREAD DE LEITURA, uma mensagem por
  vez, na ordem em que chegaram — que e' a mesma em que o servidor as enviou.
  E' uma escolha (secao 38 de docs/DECISOES.md): despachar por um pool de
  workers daria vazao maior e embaralharia a ordem, e ordem e' o unico
  compromisso que o pub/sub do Redis realmente cumpre. A consequencia esta' no
  contrato do OnMessage: callback lento segura o socket, e trabalho pesado deve
  ir para uma fila da aplicacao.

  Numa aplicacao GUI o callback NAO pode tocar a interface direto, porque nao
  esta' na main thread: use TThread.Queue/Synchronize (ver os samples do M9).

  Uso tipico:

      LSub := TRedisSubscriber.Create(LParams);
      try
        LSub.OnMessage := ChegouMensagem;   // procedure ... of object
        LSub.Start;
        LSub.Subscribe(['noticias', 'alertas']);
        ...                                  // a app segue a vida
      finally
        LSub.Free;                           // para a thread e fecha a conexao
      end;

  Publicar nao precisa de nada disso: PUBLISH e' um comando comum, sai por
  qualquer conexao do pool e mora na Redis.Commands.PubSub. }

{$I redis.inc}

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  Redis.Types,
  Redis.Resp,
  Redis.Commands,
  Redis.Connection,
  Redis.Threading;

const
  /// Quanto o Subscribe espera pela confirmacao do servidor antes de desistir.
  REDIS_SUBSCRIBE_TIMEOUT_MS = 5000;

  /// Primeira espera entre tentativas de reconexao. Dobra a cada falha ate'
  /// REDIS_RECONNECT_MAX_DELAY_MS.
  REDIS_RECONNECT_DELAY_MS = 500;
  REDIS_RECONNECT_MAX_DELAY_MS = 30000;

type
  /// Uso indevido do assinante: comando que o servidor recusaria em RESP2,
  /// chamada que nao pode partir de dentro do callback, assinante parado.
  /// Indica bug de quem chama, nao problema de rede.
  ERedisPubSubError = class(ERedisException);

  /// Por qual das tres formas de assinatura a mensagem chegou.
  TRedisPubSubKind = (pkChannel, pkPattern, pkShard);

  /// Verbo da mensagem de controle que o servidor envia. pvNone = nao e'
  /// trafego de pub/sub (e' resposta de um comando comum).
  TRedisPubSubVerb = (pvNone,
    pvMessage, pvPMessage, pvSMessage,
    pvSubscribe, pvUnsubscribe,
    pvPSubscribe, pvPUnsubscribe,
    pvSSubscribe, pvSUnsubscribe);

  /// Uma mensagem publicada.
  ///
  /// Payload em TBytes porque o Redis publica bytes: quem publica um JPEG tem
  /// o JPEG de volta. Text faz a decodificacao UTF-8 quando o conteudo e'
  /// texto — o caso comum, mas nao o contrato.
  TRedisPubSubMessage = record
    Kind: TRedisPubSubKind;
    /// Canal em que a mensagem foi publicada. No pkPattern e' o canal REAL,
    /// nao o padrao.
    Channel: string;
    /// Padrao que casou; vazio fora do pkPattern.
    Pattern: string;
    Payload: TBytes;
    /// Payload decodificado como UTF-8.
    function Text: string;
  end;

  /// Chegou mensagem. Roda na thread de leitura (ver o cabecalho da unit).
  TRedisMessageEvent = procedure(ASender: TObject;
    const AMessage: TRedisPubSubMessage) of object;

  /// O servidor confirmou uma assinatura (ou o seu cancelamento). ACount e' o
  /// total de assinaturas que a CONEXAO passou a ter, como o servidor conta.
  TRedisSubscriptionEvent = procedure(ASender: TObject; AKind: TRedisPubSubKind;
    const AName: string; ACount: Integer) of object;

  /// A conexao caiu, ou o callback da aplicacao levantou. AError vale so'
  /// durante a chamada — copie a mensagem se precisar dela depois.
  TRedisPubSubErrorEvent = procedure(ASender: TObject;
    AError: Exception) of object;

  /// Reconectou e reenviou as assinaturas.
  TRedisPubSubNotifyEvent = procedure(ASender: TObject) of object;

  TRedisSubscriber = class;

  /// A thread de leitura. Nao tem logica propria: o laco inteiro vive no
  /// assinante, que e' quem tem o estado.
  TRedisSubscriberThread = class(TThread)
  private
    FOwner: TRedisSubscriber;
  protected
    procedure Execute; override;
  public
    constructor Create(AOwner: TRedisSubscriber);
  end;

  /// Assinante de pub/sub: uma conexao dedicada, uma thread de leitura e
  /// callbacks.
  ///
  /// Thread-safety: Subscribe/Unsubscribe/Execute/Ping podem ser chamados de
  /// qualquer thread (sao serializados internamente). Start e Stop, nao —
  /// chame-os de uma thread so', normalmente a que criou o objeto.
  TRedisSubscriber = class
  private
    FParams: TRedisParams;
    FConnection: TRedisConnection;
    FOwnsConnection: Boolean;
    FAdopted: Boolean;          // nasceu de CreateOnConnection: nao reconecta
    FThread: TRedisSubscriberThread;
    FRunning: Boolean;
    { Protege o PONTEIRO FConnection e o uso dele por quem nao e' a thread de
      leitura. So' a thread de leitura cria e destroi a conexao (fora o Start,
      antes de a thread existir, e o Stop, depois de ela morrer). }
    FConnLock: TCriticalSection;
    { Serializa Execute/Ping: so' pode haver UMA resposta pendente por vez, ja'
      que nao ha' como correlacionar duas. }
    FCmdLock: TCriticalSection;
    { Guarda os conjuntos confirmados, a geracao e a resposta pendente. }
    FMon: TRedisMonitor;
    FStop: TEvent;
    { O que a APLICACAO pediu. E' isto que a reconexao reenvia. }
    FWantChannels: TStringList;
    FWantPatterns: TStringList;
    FWantShards: TStringList;
    { O que o SERVIDOR confirmou. Zerado a cada queda. }
    FLiveChannels: TStringList;
    FLivePatterns: TStringList;
    FLiveShards: TStringList;
    FGeneration: Integer;       // +1 a cada queda: desperta quem espera
    FReply: IRedisReply;        // resposta de comando comum, aguardando dono
    FReplyReady: Boolean;
    FSubscribeTimeoutMs: Integer;
    FCommandTimeoutMs: Integer;
    FAutoReconnect: Boolean;
    FReconnectDelayMs: Integer;
    FMaxReconnectDelayMs: Integer;
    FLastError: string;
    FOnMessage: TRedisMessageEvent;
    FOnSubscribed: TRedisSubscriptionEvent;
    FOnUnsubscribed: TRedisSubscriptionEvent;
    FOnError: TRedisPubSubErrorEvent;
    FOnDisconnected: TRedisPubSubNotifyEvent;
    FOnReconnected: TRedisPubSubNotifyEvent;
    function WantListOf(AKind: TRedisPubSubKind): TStringList;
    function LiveListOf(AKind: TRedisPubSubKind): TStringList;
    function NamesOf(AList: TStringList): TRedisStringArray;
    function Stopping: Boolean;
    function InReaderThread: Boolean;
    /// Parametros da conexao de pub/sub: os do assinante, com o read timeout
    /// desligado (silencio nao e' falha aqui).
    function ConnectionParams: TRedisParams;
    function ProtocolInUse: TRedisProtocol;
    procedure EnsureRunning;
    procedure CheckCommandAllowed(const AName: string);
    procedure SendCommand(const AArgs: array of TRedisArg);
    procedure ChangeSubscription(const ACommand: string; AKind: TRedisPubSubKind;
      const ANames: array of string; AAdding: Boolean);
    function WaitSubscription(AKind: TRedisPubSubKind;
      const ANames: array of string; AAdding: Boolean;
      AGeneration: Integer): Boolean;
    function WaitReply(AGeneration, ATimeoutMs: Integer): IRedisReply;
    procedure Report(AError: Exception);
    procedure SafeNotify(AEvent: TRedisPubSubNotifyEvent);
    { --- daqui para baixo, so' a thread de leitura chama --- }
    procedure RunLoop;
    function EnsureConnected: Boolean;
    procedure ReplaySubscriptions;
    procedure DispatchReply(const AReply: IRedisReply);
    procedure HandleMessage(const AReply: IRedisReply; AVerb: TRedisPubSubVerb);
    procedure HandleConfirmation(const AReply: IRedisReply;
      AVerb: TRedisPubSubVerb);
    procedure HandleCommandReply(const AReply: IRedisReply);
    procedure NoteLoss(AError: Exception);
    procedure DropConnection;
  public
    /// Assinante com conexao propria, aberta no Start.
    constructor Create(const AParams: TRedisParams);

    /// Assinante sobre uma conexao ja' existente (aberta ou nao).
    ///
    /// NAO reconecta: uma conexao dada de fora pode nem ser de socket (as
    /// suites de teste passam uma sobre stream), e reabrir por conta propria
    /// sairia conectando em outro lugar. Perder a conexao aqui para o
    /// assinante de vez.
    constructor CreateOnConnection(AConnection: TRedisConnection;
      AOwnsConnection: Boolean = True);

    destructor Destroy; override;

    /// Abre a conexao (se preciso) e sobe a thread de leitura. Falha de
    /// conexao ou de handshake levanta AQUI, na thread de quem chamou — e' o
    /// motivo de o Start abrir antes de subir a thread.
    procedure Start;

    /// Para a thread e derruba a conexao. Idempotente.
    ///
    /// A conexao e' invalidada, nao devolvida ao mundo: e' o Abort que
    /// desbloqueia a leitura parada em silencio. Uma conexao passada em
    /// CreateOnConnection com AOwnsConnection=False fica inutilizavel depois
    /// disto.
    ///
    /// Chamado de DENTRO de um callback, ele sinaliza e volta na hora, sem
    /// esperar a thread morrer — seria a espera da propria thread por si
    /// mesma. O ciclo se fecha no Free, que roda em outra thread.
    procedure Stop;

    { --- Assinaturas ---

      As operacoes esperam a confirmacao do servidor (ate' SubscribeTimeoutMs)
      e so' voltam quando ela chega — assim, depois do Subscribe, a app sabe
      que esta' realmente inscrita e nao ha' corrida com o primeiro PUBLISH.

      Duas excecoes, as duas documentadas por um motivo:
      - chamada de DENTRO do callback nao espera (a thread que confirmaria e'
        justamente a que esta' rodando o callback);
      - com a conexao caida e AutoReconnect ligado, a assinatura fica
        registrada e vai no fio quando ela voltar. }

    procedure Subscribe(const AChannels: array of string);
    procedure Unsubscribe(const AChannels: array of string); overload;
    /// Cancela TODAS as assinaturas por nome exato.
    procedure Unsubscribe; overload;

    procedure PSubscribe(const APatterns: array of string);
    procedure PUnsubscribe(const APatterns: array of string); overload;
    procedure PUnsubscribe; overload;

    /// Assinatura shardada (Redis 7+).
    procedure SSubscribe(const AChannels: array of string);
    procedure SUnsubscribe(const AChannels: array of string); overload;
    procedure SUnsubscribe; overload;

    /// PING pela conexao do assinante. Em modo de assinatura o servidor
    /// responde um array ['pong', ''] em vez de +PONG; os dois contam.
    function Ping: Boolean;

    /// Executa um comando comum PELA CONEXAO DO ASSINANTE e espera a resposta.
    ///
    /// Em RESP2 so' vale enquanto nao ha' assinatura ativa, ou para os
    /// comandos que o servidor aceita em modo de assinatura (SUBSCRIBE e
    /// familia, PING, RESET, QUIT): qualquer outro levanta ERedisPubSubError
    /// citando o motivo, em vez de deixar o servidor responder um erro que nao
    /// explica nada. Em RESP3 nao ha' restricao.
    ///
    /// NAO pode ser chamado de dentro de um callback: quem leria a resposta e'
    /// a thread que esta' executando o callback.
    function Execute(const AName: string;
      const AArgs: array of TRedisArg): IRedisReply; overload;
    function Execute(const AName: string): IRedisReply; overload;

    /// Canais/padroes/shards CONFIRMADOS pelo servidor neste instante.
    function Channels: TRedisStringArray;
    function Patterns: TRedisStringArray;
    function ShardChannels: TRedisStringArray;
    /// Total confirmado (canais + padroes + shards).
    function SubscriptionCount: Integer;

    /// True quando ha' conexao aberta agora.
    function Connected: Boolean;

    /// True entre Start e Stop. Nao diz que a conexao esta' de pe' — com
    /// AutoReconnect, ela pode estar sendo restabelecida.
    property Active: Boolean read FRunning;

    /// Mensagem da ultima falha vista pela thread de leitura ('' se nenhuma).
    property LastError: string read FLastError;

    /// Reabre a conexao e reenvia as assinaturas quando a conexao cai.
    /// Sempre False num assinante criado com CreateOnConnection.
    property AutoReconnect: Boolean read FAutoReconnect write FAutoReconnect;
    /// Espera antes da primeira tentativa; dobra a cada falha ate'
    /// MaxReconnectDelayMs.
    property ReconnectDelayMs: Integer read FReconnectDelayMs
      write FReconnectDelayMs;
    property MaxReconnectDelayMs: Integer read FMaxReconnectDelayMs
      write FMaxReconnectDelayMs;
    /// Espera pela confirmacao de uma assinatura.
    property SubscribeTimeoutMs: Integer read FSubscribeTimeoutMs
      write FSubscribeTimeoutMs;
    /// Espera pela resposta de um Execute/Ping.
    property CommandTimeoutMs: Integer read FCommandTimeoutMs
      write FCommandTimeoutMs;

    property Params: TRedisParams read FParams;

    property OnMessage: TRedisMessageEvent read FOnMessage write FOnMessage;
    property OnSubscribed: TRedisSubscriptionEvent read FOnSubscribed
      write FOnSubscribed;
    property OnUnsubscribed: TRedisSubscriptionEvent read FOnUnsubscribed
      write FOnUnsubscribed;
    /// Falha da thread de leitura ou excecao escapada de um callback. Nunca
    /// levanta para fora da thread — quem quiser saber, escuta aqui.
    property OnError: TRedisPubSubErrorEvent read FOnError write FOnError;
    property OnDisconnected: TRedisPubSubNotifyEvent read FOnDisconnected
      write FOnDisconnected;
    property OnReconnected: TRedisPubSubNotifyEvent read FOnReconnected
      write FOnReconnected;
  end;

/// Classifica uma resposta como trafego de pub/sub.
///
/// Devolve pvNone para o que nao for: resposta de comando comum, escalar, ou
/// agregado com a aridade errada para o verbo (um PUBSUB CHANNELS que devolva
/// tres canais, o primeiro chamado 'subscribe', nao vira confirmacao).
///
/// Em RESP3 quem separa os dois mundos e' o tipo push ('>'), e o assinante usa
/// isso; esta funcao aceita as duas formas de proposito, para que o mesmo
/// teste rode nos dois protocolos.
function RedisPubSubVerbOf(const AReply: IRedisReply): TRedisPubSubVerb;

/// Converte uma resposta ja' classificada como message/pmessage/smessage no
/// registro entregue ao callback. False se a forma nao bater.
function RedisParsePubSubMessage(const AReply: IRedisReply;
  out AMessage: TRedisPubSubMessage): Boolean;

/// True para os comandos que o servidor aceita numa conexao RESP2 em modo de
/// assinatura.
function RedisAllowedWhileSubscribed(const AName: string): Boolean;

implementation

{ TRedisPubSubMessage }

function TRedisPubSubMessage.Text: string;
begin
  Result := RedisUtf8Decode(Payload);
end;

{ Funcoes de classificacao }

function RedisPubSubVerbOf(const AReply: IRedisReply): TRedisPubSubVerb;
var
  LVerb: string;
  LCount: Integer;

  // Verbo confere, aridade confere — e, nas confirmacoes, o terceiro item e'
  // mesmo um inteiro? Sao essas tres perguntas que separam trafego de pub/sub
  // de um array qualquer cujo primeiro item por acaso se chama 'subscribe':
  // um PUBSUB CHANNELS pode devolver exatamente isso, e numa conexao RESP3
  // (onde comando comum e' permitido) a confusao seria silenciosa.
  function Fits(AExpected: Integer; AResult: TRedisPubSubVerb;
    ACounted: Boolean): TRedisPubSubVerb;
  begin
    Result := pvNone;
    if LCount <> AExpected then
      Exit;
    if ACounted and (AReply[2].Kind <> rkInteger) then
      Exit;
    Result := AResult;
  end;

begin
  Result := pvNone;
  if AReply = nil then
    Exit;
  // Em RESP3 o push e' um tipo proprio; em RESP2 tudo chega como array. Set e
  // mapa nunca aparecem aqui.
  if not (AReply.Kind in [rkArray, rkPush]) then
    Exit;
  LCount := AReply.Count;
  if LCount < 3 then
    Exit;
  if not (AReply[0].Kind in [rkSimpleString, rkBulkString, rkVerbatim]) then
    Exit;
  LVerb := LowerCase(AReply[0].AsString);

  if LVerb = 'message' then
    Result := Fits(3, pvMessage, False)
  else if LVerb = 'pmessage' then
    Result := Fits(4, pvPMessage, False)
  else if LVerb = 'smessage' then
    Result := Fits(3, pvSMessage, False)
  else if LVerb = 'subscribe' then
    Result := Fits(3, pvSubscribe, True)
  else if LVerb = 'unsubscribe' then
    Result := Fits(3, pvUnsubscribe, True)
  else if LVerb = 'psubscribe' then
    Result := Fits(3, pvPSubscribe, True)
  else if LVerb = 'punsubscribe' then
    Result := Fits(3, pvPUnsubscribe, True)
  else if LVerb = 'ssubscribe' then
    Result := Fits(3, pvSSubscribe, True)
  else if LVerb = 'sunsubscribe' then
    Result := Fits(3, pvSUnsubscribe, True);
end;

function RedisParsePubSubMessage(const AReply: IRedisReply;
  out AMessage: TRedisPubSubMessage): Boolean;
var
  LVerb: TRedisPubSubVerb;
begin
  AMessage.Kind := pkChannel;
  AMessage.Channel := '';
  AMessage.Pattern := '';
  AMessage.Payload := nil;

  LVerb := RedisPubSubVerbOf(AReply);
  Result := True;
  case LVerb of
    pvMessage:
      begin
        AMessage.Kind := pkChannel;
        AMessage.Channel := AReply[1].AsString;
        AMessage.Payload := AReply[2].AsBytes;
      end;
    pvSMessage:
      begin
        AMessage.Kind := pkShard;
        AMessage.Channel := AReply[1].AsString;
        AMessage.Payload := AReply[2].AsBytes;
      end;
    pvPMessage:
      begin
        // No pmessage vem o padrao E o canal real: quem assinou 'noticias.*'
        // precisa saber que a mensagem veio de 'noticias.esporte'.
        AMessage.Kind := pkPattern;
        AMessage.Pattern := AReply[1].AsString;
        AMessage.Channel := AReply[2].AsString;
        AMessage.Payload := AReply[3].AsBytes;
      end;
  else
    Result := False;
  end;
end;

function RedisAllowedWhileSubscribed(const AName: string): Boolean;
var
  LName: string;
begin
  LName := UpperCase(Trim(AName));
  Result := (LName = 'SUBSCRIBE') or (LName = 'UNSUBSCRIBE') or
    (LName = 'PSUBSCRIBE') or (LName = 'PUNSUBSCRIBE') or
    (LName = 'SSUBSCRIBE') or (LName = 'SUNSUBSCRIBE') or
    (LName = 'PING') or (LName = 'RESET') or (LName = 'QUIT');
end;

{ TRedisSubscriberThread }

constructor TRedisSubscriberThread.Create(AOwner: TRedisSubscriber);
begin
  FOwner := AOwner;
  FreeOnTerminate := False;   // quem espera e libera e' o Stop
  inherited Create(False);
end;

procedure TRedisSubscriberThread.Execute;
begin
  FOwner.RunLoop;
end;

{ TRedisSubscriber }

// Lista ordenada, sensivel a maiusculas e sem repetidos: nome de canal no
// Redis e' binario-comparado, e 'Noticias' nao e' 'noticias'.
function NewNameList: TStringList;
begin
  Result := TStringList.Create;
  Result.CaseSensitive := True;
  Result.Sorted := True;
  Result.Duplicates := dupIgnore;
end;

constructor TRedisSubscriber.Create(const AParams: TRedisParams);
begin
  inherited Create;
  FParams := AParams;
  FOwnsConnection := True;
  FAdopted := False;
  FAutoReconnect := True;
  FReconnectDelayMs := REDIS_RECONNECT_DELAY_MS;
  FMaxReconnectDelayMs := REDIS_RECONNECT_MAX_DELAY_MS;
  FSubscribeTimeoutMs := REDIS_SUBSCRIBE_TIMEOUT_MS;
  FCommandTimeoutMs := REDIS_SUBSCRIBE_TIMEOUT_MS;
  FConnLock := TCriticalSection.Create;
  FCmdLock := TCriticalSection.Create;
  FMon := TRedisMonitor.Create;
  FStop := TEvent.Create(nil, True, False, '');   // manual reset
  FWantChannels := NewNameList;
  FWantPatterns := NewNameList;
  FWantShards := NewNameList;
  FLiveChannels := NewNameList;
  FLivePatterns := NewNameList;
  FLiveShards := NewNameList;
end;

constructor TRedisSubscriber.CreateOnConnection(AConnection: TRedisConnection;
  AOwnsConnection: Boolean);
begin
  if AConnection = nil then
    raise ERedisPubSubError.Create('conexao nao pode ser nil');
  Create(AConnection.Params);
  FConnection := AConnection;
  FOwnsConnection := AOwnsConnection;
  FAdopted := True;
  // Conexao de fora nao se reabre sozinha: pode nem ser de socket.
  FAutoReconnect := False;
end;

destructor TRedisSubscriber.Destroy;
begin
  Stop;
  FWantChannels.Free;
  FWantPatterns.Free;
  FWantShards.Free;
  FLiveChannels.Free;
  FLivePatterns.Free;
  FLiveShards.Free;
  FStop.Free;
  FMon.Free;
  FCmdLock.Free;
  FConnLock.Free;
  inherited Destroy;
end;

function TRedisSubscriber.WantListOf(AKind: TRedisPubSubKind): TStringList;
begin
  case AKind of
    pkPattern: Result := FWantPatterns;
    pkShard: Result := FWantShards;
  else
    Result := FWantChannels;
  end;
end;

function TRedisSubscriber.LiveListOf(AKind: TRedisPubSubKind): TStringList;
begin
  case AKind of
    pkPattern: Result := FLivePatterns;
    pkShard: Result := FLiveShards;
  else
    Result := FLiveChannels;
  end;
end;

function TRedisSubscriber.NamesOf(AList: TStringList): TRedisStringArray;
var
  I: Integer;
begin
  Result := nil;
  FMon.Enter;
  try
    SetLength(Result, AList.Count);
    for I := 0 to AList.Count - 1 do
      Result[I] := AList[I];
  finally
    FMon.Leave;
  end;
end;

function TRedisSubscriber.Stopping: Boolean;
begin
  Result := FStop.WaitFor(0) = wrSignaled;
end;

function TRedisSubscriber.InReaderThread: Boolean;
begin
  Result := (FThread <> nil) and
    (TThread.CurrentThread.ThreadID = FThread.ThreadID);
end;

function TRedisSubscriber.ConnectionParams: TRedisParams;
begin
  Result := FParams;
  // Silencio nao e' falha: um canal pode ficar horas sem publicar nada, e um
  // read timeout aqui invalidaria a conexao a toa (o kernel descarta conexao
  // que estourou timeout, e com razao — no modo pergunta-resposta ha' uma
  // resposta atrasada a caminho). Quem desbloqueia a leitura no Stop e' o
  // Abort, nao o relogio.
  Result.ReceiveTimeoutMs := 0;
end;

function TRedisSubscriber.ProtocolInUse: TRedisProtocol;
var
  LConn: TRedisConnection;
begin
  LConn := FConnection;
  if (LConn <> nil) and LConn.IsOpen then
    Result := LConn.NegotiatedProtocol
  else
    Result := FParams.Protocol;
end;

procedure TRedisSubscriber.EnsureRunning;
begin
  if not FRunning then
    raise ERedisPubSubError.Create(
      'assinante nao esta ativo; chame Start antes');
end;

procedure TRedisSubscriber.CheckCommandAllowed(const AName: string);
begin
  if ProtocolInUse = rpRESP3 then
    Exit;   // em RESP3 a conexao continua normal
  if RedisAllowedWhileSubscribed(AName) then
    Exit;
  if SubscriptionCount = 0 then
    Exit;   // sem assinatura ativa, a conexao RESP2 ainda e' comum
  raise ERedisPubSubError.CreateFmt(
    'em RESP2 o servidor recusa %s numa conexao com assinatura ativa; ' +
    'use outra conexao (o pool do TRedisClient) ou negocie RESP3 com HELLO 3',
    [UpperCase(AName)]);
end;

procedure TRedisSubscriber.SendCommand(const AArgs: array of TRedisArg);
var
  LConn: TRedisConnection;
begin
  FConnLock.Enter;
  try
    LConn := FConnection;
    if (LConn = nil) or LConn.IsBroken or (not LConn.IsOpen) then
      raise ERedisConnectionLost.Create(
        'a conexao do assinante nao esta disponivel');
    LConn.SendArgs(AArgs);
  finally
    FConnLock.Leave;
  end;
end;

procedure TRedisSubscriber.ChangeSubscription(const ACommand: string;
  AKind: TRedisPubSubKind; const ANames: array of string; AAdding: Boolean);
var
  I, LIndex, LGeneration: Integer;
  LArgs: TRedisArgs;
  LWant: TStringList;
  LSent: Boolean;
begin
  EnsureRunning;
  for I := 0 to High(ANames) do
    if ANames[I] = '' then
      raise ERedisPubSubError.Create('nome de canal/padrao nao pode ser vazio');

  // Registra o desejo ANTES de mandar: se a conexao cair no meio, e' esta
  // lista que a reconexao reenvia.
  LWant := WantListOf(AKind);
  FMon.Enter;
  try
    LGeneration := FGeneration;
    if AAdding then
    begin
      for I := 0 to High(ANames) do
        LWant.Add(ANames[I]);
    end
    else if Length(ANames) = 0 then
      LWant.Clear
    else
      for I := 0 to High(ANames) do
      begin
        LIndex := LWant.IndexOf(ANames[I]);
        if LIndex >= 0 then
          LWant.Delete(LIndex);
      end;
  finally
    FMon.Leave;
  end;

  SetLength(LArgs, Length(ANames) + 1);
  LArgs[0] := ACommand;
  for I := 0 to High(ANames) do
    LArgs[I + 1] := ANames[I];

  LSent := True;
  try
    SendCommand(LArgs);
  except
    on E: ERedisConnectionLost do
    begin
      // Com reconexao ligada isto nao e' erro: a assinatura ja' esta'
      // registrada e vai no fio assim que a conexao voltar.
      if not FAutoReconnect then
        raise;
      LSent := False;
    end;
  end;

  if not LSent then
    Exit;
  // De dentro do callback nao da' para esperar: quem leria a confirmacao e' a
  // propria thread que esta' rodando o callback.
  if InReaderThread then
    Exit;
  if not WaitSubscription(AKind, ANames, AAdding, LGeneration) then
    raise ERedisTimeout.CreateFmt(
      'o servidor nao confirmou %s em %d ms', [UpperCase(ACommand),
      FSubscribeTimeoutMs]);
end;

function TRedisSubscriber.WaitSubscription(AKind: TRedisPubSubKind;
  const ANames: array of string; AAdding: Boolean;
  AGeneration: Integer): Boolean;
var
  LLive: TStringList;
  LDeadline: UInt64;
  LNow: UInt64;

  // A condicao e' o ESTADO, nao a contagem de confirmacoes: assim o resultado
  // nao depende de quantas mensagens chegaram nem da ordem delas.
  function Satisfied: Boolean;
  var
    J: Integer;
  begin
    if Length(ANames) = 0 then
      Exit(LLive.Count = 0);   // "cancela tudo"
    for J := 0 to High(ANames) do
      if (LLive.IndexOf(ANames[J]) >= 0) <> AAdding then
        Exit(False);
    Result := True;
  end;

begin
  LLive := LiveListOf(AKind);
  LDeadline := RedisTickMs + UInt64(FSubscribeTimeoutMs);
  Result := False;
  FMon.Enter;
  try
    while True do
    begin
      if FGeneration <> AGeneration then
        Exit(False);            // caiu no meio: nao ha' o que confirmar
      if Satisfied then
        Exit(True);
      LNow := RedisTickMs;
      if LNow >= LDeadline then
        Exit(False);
      FMon.Wait(Cardinal(LDeadline - LNow));
    end;
  finally
    FMon.Leave;
  end;
end;

function TRedisSubscriber.WaitReply(AGeneration,
  ATimeoutMs: Integer): IRedisReply;
var
  LDeadline, LNow: UInt64;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  FMon.Enter;
  try
    while not FReplyReady do
    begin
      if FGeneration <> AGeneration then
        raise ERedisConnectionLost.Create(
          'a conexao do assinante caiu antes da resposta');
      LNow := RedisTickMs;
      if LNow >= LDeadline then
        raise ERedisTimeout.CreateFmt(
          'o servidor nao respondeu em %d ms', [ATimeoutMs]);
      FMon.Wait(Cardinal(LDeadline - LNow));
    end;
    Result := FReply;
    FReply := nil;
    FReplyReady := False;
  finally
    FMon.Leave;
  end;
end;

procedure TRedisSubscriber.Report(AError: Exception);
begin
  FLastError := AError.ClassName + ': ' + AError.Message;
  if not Assigned(FOnError) then
    Exit;
  // Callback da aplicacao nunca derruba a thread de leitura — nem o de erro.
  try
    FOnError(Self, AError);
  except
    // engolido de proposito: nao ha' para onde propagar
  end;
end;

procedure TRedisSubscriber.SafeNotify(AEvent: TRedisPubSubNotifyEvent);
begin
  if not Assigned(AEvent) then
    Exit;
  try
    AEvent(Self);
  except
    on E: Exception do
      Report(E);
  end;
end;

procedure TRedisSubscriber.Start;
var
  LConn: TRedisConnection;
begin
  if FRunning then
    Exit;
  // Conexao adotada ja' foi embora no Stop anterior: abrir outra aqui seria
  // sair conectando num lugar que quem passou a conexao nunca escolheu.
  if FAdopted and (FConnection = nil) then
    raise ERedisPubSubError.Create(
      'assinante criado sobre uma conexao pronta nao pode ser reiniciado ' +
      'depois do Stop; crie outro');
  FStop.ResetEvent;
  FLastError := '';
  FConnLock.Enter;
  try
    if FConnection = nil then
    begin
      LConn := TRedisConnection.Create(ConnectionParams);
      try
        LConn.Open;
      except
        LConn.Free;
        raise;
      end;
      FConnection := LConn;
    end
    else if not FConnection.IsOpen then
      FConnection.Open;
  finally
    FConnLock.Leave;
  end;
  FRunning := True;
  // So' agora sobe a thread: falha de conexao/handshake levanta na thread de
  // quem chamou o Start, onde da' para tratar.
  FThread := TRedisSubscriberThread.Create(Self);
end;

procedure TRedisSubscriber.Stop;
var
  LRodou: Boolean;
begin
  // Stop chamado de DENTRO de um callback nao pode esperar a propria thread
  // morrer. Sinaliza, derruba o socket e volta: o laco termina sozinho, e a
  // faxina fica para o Free — que roda em outra thread.
  if InReaderThread then
  begin
    FStop.SetEvent;
    FConnLock.Enter;
    try
      if FConnection <> nil then
        FConnection.Abort;
    finally
      FConnLock.Leave;
    end;
    Exit;
  end;

  LRodou := FRunning or (FThread <> nil);
  if LRodou then
  begin
    FStop.SetEvent;
    // Derruba o socket: e' o que desbloqueia a thread parada num canal em
    // silencio (nao ha' read timeout nesta conexao, e de proposito).
    FConnLock.Enter;
    try
      if FConnection <> nil then
        FConnection.Abort;
    finally
      FConnLock.Leave;
    end;
    if FThread <> nil then
    begin
      FThread.Terminate;
      FThread.WaitFor;
      FreeAndNil(FThread);
    end;
    FRunning := False;
  end;
  // A thread ja' morreu: agora ninguem mais toca na conexao, e a faxina
  // (reader, stream, socket) pode acontecer. Assinante que nunca subiu nao
  // mexe em conexao alheia — so' libera a que for dele.
  FConnLock.Enter;
  try
    if FConnection <> nil then
    begin
      if FOwnsConnection then
        FreeAndNil(FConnection)
      else if LRodou then
        FConnection.Close;
    end;
  finally
    FConnLock.Leave;
  end;
  FMon.Enter;
  try
    FLiveChannels.Clear;
    FLivePatterns.Clear;
    FLiveShards.Clear;
    Inc(FGeneration);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

procedure TRedisSubscriber.Subscribe(const AChannels: array of string);
begin
  ChangeSubscription('SUBSCRIBE', pkChannel, AChannels, True);
end;

procedure TRedisSubscriber.Unsubscribe(const AChannels: array of string);
begin
  ChangeSubscription('UNSUBSCRIBE', pkChannel, AChannels, False);
end;

procedure TRedisSubscriber.Unsubscribe;
begin
  ChangeSubscription('UNSUBSCRIBE', pkChannel, [], False);
end;

procedure TRedisSubscriber.PSubscribe(const APatterns: array of string);
begin
  ChangeSubscription('PSUBSCRIBE', pkPattern, APatterns, True);
end;

procedure TRedisSubscriber.PUnsubscribe(const APatterns: array of string);
begin
  ChangeSubscription('PUNSUBSCRIBE', pkPattern, APatterns, False);
end;

procedure TRedisSubscriber.PUnsubscribe;
begin
  ChangeSubscription('PUNSUBSCRIBE', pkPattern, [], False);
end;

procedure TRedisSubscriber.SSubscribe(const AChannels: array of string);
begin
  ChangeSubscription('SSUBSCRIBE', pkShard, AChannels, True);
end;

procedure TRedisSubscriber.SUnsubscribe(const AChannels: array of string);
begin
  ChangeSubscription('SUNSUBSCRIBE', pkShard, AChannels, False);
end;

procedure TRedisSubscriber.SUnsubscribe;
begin
  ChangeSubscription('SUNSUBSCRIBE', pkShard, [], False);
end;

function TRedisSubscriber.Ping: Boolean;
var
  LReply: IRedisReply;
begin
  LReply := Execute('PING', []);
  // Fora do modo de assinatura vem +PONG; dentro dele, ['pong', ''].
  if LReply.IsAggregate then
    Result := (LReply.Count > 0) and (LowerCase(LReply[0].AsString) = 'pong')
  else
    Result := LowerCase(LReply.AsString) = 'pong';
end;

function TRedisSubscriber.Execute(const AName: string;
  const AArgs: array of TRedisArg): IRedisReply;
var
  LGeneration: Integer;
  LArgs: TRedisArgs;
  I: Integer;
begin
  EnsureRunning;
  if InReaderThread then
    raise ERedisPubSubError.Create(
      'Execute nao pode ser chamado de dentro de um callback: quem leria a ' +
      'resposta e a propria thread que esta rodando o callback');
  CheckCommandAllowed(AName);

  SetLength(LArgs, Length(AArgs) + 1);
  LArgs[0] := AName;
  for I := 0 to High(AArgs) do
    LArgs[I + 1] := AArgs[I];

  FCmdLock.Enter;
  try
    FMon.Enter;
    try
      LGeneration := FGeneration;
      FReply := nil;
      FReplyReady := False;
    finally
      FMon.Leave;
    end;
    SendCommand(LArgs);
    Result := WaitReply(LGeneration, FCommandTimeoutMs);
  finally
    FCmdLock.Leave;
  end;
  Result.RaiseIfError;
end;

function TRedisSubscriber.Execute(const AName: string): IRedisReply;
begin
  Result := Execute(AName, []);
end;

function TRedisSubscriber.Channels: TRedisStringArray;
begin
  Result := NamesOf(FLiveChannels);
end;

function TRedisSubscriber.Patterns: TRedisStringArray;
begin
  Result := NamesOf(FLivePatterns);
end;

function TRedisSubscriber.ShardChannels: TRedisStringArray;
begin
  Result := NamesOf(FLiveShards);
end;

function TRedisSubscriber.SubscriptionCount: Integer;
begin
  FMon.Enter;
  try
    Result := FLiveChannels.Count + FLivePatterns.Count + FLiveShards.Count;
  finally
    FMon.Leave;
  end;
end;

function TRedisSubscriber.Connected: Boolean;
var
  LConn: TRedisConnection;
begin
  FConnLock.Enter;
  try
    LConn := FConnection;
    Result := (LConn <> nil) and LConn.IsOpen and (not LConn.IsBroken);
  finally
    FConnLock.Leave;
  end;
end;

{ --- Thread de leitura --------------------------------------------------- }

procedure TRedisSubscriber.RunLoop;
var
  LConn: TRedisConnection;
  LDelay: Integer;
begin
  LDelay := FReconnectDelayMs;
  while not Stopping do
  begin
    try
      if not EnsureConnected then
        Break;                    // sem como reconectar: acabou
      LDelay := FReconnectDelayMs;
      while not Stopping do
      begin
        LConn := FConnection;     // so' esta thread troca este ponteiro
        if LConn = nil then
          raise ERedisConnectionLost.Create('a conexao do assinante sumiu');
        DispatchReply(LConn.Receive);
      end;
    except
      on E: Exception do
      begin
        NoteLoss(E);
        if Stopping or (not FAutoReconnect) then
          Break;
        // Espera interrompivel: o Stop acorda daqui na hora.
        FStop.WaitFor(Cardinal(LDelay));
        LDelay := LDelay * 2;
        if LDelay > FMaxReconnectDelayMs then
          LDelay := FMaxReconnectDelayMs;
      end;
    end;
  end;
end;

function TRedisSubscriber.EnsureConnected: Boolean;
var
  LConn: TRedisConnection;
begin
  if (FConnection <> nil) and FConnection.IsOpen and
     (not FConnection.IsBroken) then
    Exit(True);
  if FAdopted then
    Exit(False);   // conexao de fora nao se reabre

  DropConnection;
  LConn := TRedisConnection.Create(ConnectionParams);
  try
    LConn.Open;
  except
    LConn.Free;
    raise;         // o laco trata: reporta, espera e tenta de novo
  end;
  FConnLock.Enter;
  try
    FConnection := LConn;
  finally
    FConnLock.Leave;
  end;
  ReplaySubscriptions;
  SafeNotify(FOnReconnected);
  Result := True;
end;

procedure TRedisSubscriber.ReplaySubscriptions;

  // Reenvia sem esperar confirmacao: quem confirmaria e' esta mesma thread,
  // logo abaixo, no laco de leitura.
  procedure Replay(const ACommand: string; AList: TStringList);
  var
    LArgs: TRedisArgs;
    I: Integer;
  begin
    FMon.Enter;
    try
      if AList.Count = 0 then
        Exit;
      SetLength(LArgs, AList.Count + 1);
      LArgs[0] := ACommand;
      for I := 0 to AList.Count - 1 do
        LArgs[I + 1] := AList[I];
    finally
      FMon.Leave;
    end;
    SendCommand(LArgs);
  end;

begin
  // A unica topologia que existe no Redis para replayar. Nao ha' comando em
  // voo para repetir: o que se perdeu, perdeu-se (ver o cabecalho da unit).
  Replay('SUBSCRIBE', FWantChannels);
  Replay('PSUBSCRIBE', FWantPatterns);
  Replay('SSUBSCRIBE', FWantShards);
end;

procedure TRedisSubscriber.DispatchReply(const AReply: IRedisReply);
var
  LVerb: TRedisPubSubVerb;
begin
  // Em RESP3 o que e' push vem marcado como push, e mais nada e' pub/sub. Em
  // RESP2 nao ha' marca: vale a forma do array.
  if (ProtocolInUse = rpRESP3) and (AReply.Kind <> rkPush) then
  begin
    HandleCommandReply(AReply);
    Exit;
  end;
  LVerb := RedisPubSubVerbOf(AReply);
  case LVerb of
    pvMessage, pvPMessage, pvSMessage:
      HandleMessage(AReply, LVerb);
    pvSubscribe, pvUnsubscribe, pvPSubscribe, pvPUnsubscribe,
    pvSSubscribe, pvSUnsubscribe:
      HandleConfirmation(AReply, LVerb);
  else
    HandleCommandReply(AReply);
  end;
end;

procedure TRedisSubscriber.HandleMessage(const AReply: IRedisReply;
  AVerb: TRedisPubSubVerb);
var
  LMessage: TRedisPubSubMessage;
begin
  if not RedisParsePubSubMessage(AReply, LMessage) then
    Exit;
  if not Assigned(FOnMessage) then
    Exit;
  // Excecao do callback da aplicacao NAO derruba a conexao: ela vai para o
  // OnError e a proxima mensagem e' entregue normalmente. Deixar subir daria
  // uma reconexao a cada bug de quem chama.
  try
    FOnMessage(Self, LMessage);
  except
    on E: Exception do
      Report(E);
  end;
end;

procedure TRedisSubscriber.HandleConfirmation(const AReply: IRedisReply;
  AVerb: TRedisPubSubVerb);
var
  LKind: TRedisPubSubKind;
  LAdding: Boolean;
  LName: string;
  LCount: Integer;
  LLive: TStringList;
  LEvent: TRedisSubscriptionEvent;
begin
  case AVerb of
    pvPSubscribe, pvPUnsubscribe: LKind := pkPattern;
    pvSSubscribe, pvSUnsubscribe: LKind := pkShard;
  else
    LKind := pkChannel;
  end;
  LAdding := AVerb in [pvSubscribe, pvPSubscribe, pvSSubscribe];
  LCount := 0;
  if AReply[2].Kind = rkInteger then
    LCount := Integer(AReply[2].AsInteger);
  LLive := LiveListOf(LKind);

  FMon.Enter;
  try
    if AReply[1].IsNull then
    begin
      // 'unsubscribe' com nome nulo: nao havia nada assinado daquele tipo.
      LName := '';
      LLive.Clear;
    end
    else
    begin
      LName := AReply[1].AsString;
      if LAdding then
        LLive.Add(LName)
      else if LLive.IndexOf(LName) >= 0 then
        LLive.Delete(LLive.IndexOf(LName));
    end;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;

  if LAdding then
    LEvent := FOnSubscribed
  else
    LEvent := FOnUnsubscribed;
  if not Assigned(LEvent) then
    Exit;
  try
    LEvent(Self, LKind, LName, LCount);
  except
    on E: Exception do
      Report(E);
  end;
end;

procedure TRedisSubscriber.HandleCommandReply(const AReply: IRedisReply);
begin
  // Resposta de comando comum (PING, ou qualquer comando em RESP3). Quem
  // estiver no WaitReply leva; se nao houver ninguem, cai no chao — o que so'
  // acontece se um Execute tiver desistido por timeout.
  FMon.Enter;
  try
    FReply := AReply;
    FReplyReady := True;
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
end;

procedure TRedisSubscriber.NoteLoss(AError: Exception);
begin
  FMon.Enter;
  try
    FLiveChannels.Clear;
    FLivePatterns.Clear;
    FLiveShards.Clear;
    // Nova geracao: quem esta' esperando confirmacao ou resposta desiste em
    // vez de ficar pendurado ate' o timeout.
    Inc(FGeneration);
    FMon.PulseAll;
  finally
    FMon.Leave;
  end;
  DropConnection;
  Report(AError);
  SafeNotify(FOnDisconnected);
end;

procedure TRedisSubscriber.DropConnection;
var
  LConn: TRedisConnection;
begin
  FConnLock.Enter;
  try
    LConn := FConnection;
    FConnection := nil;
  finally
    FConnLock.Leave;
  end;
  if LConn = nil then
    Exit;
  // Fora do lock: nenhuma outra thread alcanca esta conexao a partir daqui
  // (todas passam pelo FConnLock e agora encontram nil), e o destrutor pode
  // fechar socket, o que nao e' para acontecer sob lock.
  if FOwnsConnection then
    LConn.Free
  else
    LConn.Close;
end;

end.
