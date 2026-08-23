unit Redis.Client;

{ Fachada da lib: o objeto que a aplicacao segura.

  Reune num lugar so' o pool do M3 e as familias de comandos do M4:

      LClient := TRedisClient.Create(RedisDefaultParams);
      try
        LClient.Strings.SetEx('cache:42', 60, LJson);
        if LClient.Keys.Exists('cache:42') then
          ...
        LClient.ZSets.ZAdd('ranking', 1500, 'fabiano');
      finally
        LClient.Free;
      end;

  Nao ha' Connect: a conexao abre na primeira vez que um comando precisa dela.

  **Cada comando pega uma conexao do pool e devolve antes de retornar.** E' o
  que torna o cliente seguro para varias threads sem serializar nada. A
  contrapartida e' que dois comandos consecutivos podem sair por conexoes
  diferentes — o que importa quando a sequencia PRECISA da mesma conexao:
  MULTI/EXEC, WATCH, SELECT e os comandos bloqueantes. Para esses, tire uma
  conexao e amarre um cliente a ela:

      LConn := LClient.Acquire;
      try
        LDedicado := TRedisClient.CreateOnConnection(LConn);
        try
          ...                     // tudo pela MESMA conexao
        finally
          LDedicado.Free;
        end;
      finally
        LClient.Release(LConn);
      end;

  Os bloqueantes (BLPOP, BRPOP, BLMOVE) ja' cuidam disso sozinhos: passam por
  ExecuteBlocking, que os manda por um pool SEPARADO, com o read timeout
  esticado para alem do timeout do comando. Se saissem do pool comum, um worker
  esperando 30 s por uma tarefa seguraria uma conexao que as outras threads
  precisam — e, pior, morreria de timeout de socket antes de o comando
  terminar, deixando a resposta a caminho para contaminar a proxima conexao. }

{$I redis.inc}

interface

uses
  SysUtils,
  SyncObjs,
  Redis.Types,
  Redis.Connection,
  Redis.Pool,
  Redis.Commands,
  Redis.Commands.Keys,
  Redis.Commands.Strings,
  Redis.Commands.Hashes,
  Redis.Commands.Lists,
  Redis.Commands.Sets,
  Redis.Commands.ZSets,
  Redis.Commands.Scripting,
  Redis.Commands.PubSub,
  Redis.PubSub,
  Redis.Transaction;

const
  /// Folga somada ao timeout de um comando bloqueante para chegar ao read
  /// timeout do socket. Dois segundos cobrem a latencia de rede e o tempo que
  /// o servidor leva para responder no instante em que o prazo vence — sem
  /// isso, o socket desistiria no mesmo instante em que a resposta chega, e a
  /// conexao seria descartada a toa.
  REDIS_BLOCKING_MARGIN_MS = 2000;

type
  /// Cliente Redis: pool + fachadas tipadas por familia.
  ///
  /// Thread-safe no modo pool (cada comando pega a sua conexao). No modo
  /// conexao unica (CreateOnConnection) a seguranca e' a da propria conexao:
  /// os comandos serializam sob o lock dela.
  TRedisClient = class(TRedisCommandExecutor)
  private
    FParams: TRedisParams;
    FPoolParams: TRedisPoolParams;
    FPool: TRedisPool;
    FOwnsPool: Boolean;
    FConnection: TRedisConnection;
    FOwnsConnection: Boolean;
    FLock: TCriticalSection;
    FBlockingPool: TRedisPool;
    FKeys: TRedisKeysCommands;
    FStrings: TRedisStringsCommands;
    FHashes: TRedisHashesCommands;
    FLists: TRedisListsCommands;
    FSets: TRedisSetsCommands;
    FZSets: TRedisZSetsCommands;
    FScripting: TRedisScriptingCommands;
    FPubSub: TRedisPubSubCommands;
    procedure CreateFamilies;
    /// Pool dedicado aos comandos bloqueantes, criado na primeira vez que um
    /// deles roda. Fica separado do pool comum de proposito.
    function BlockingPool: TRedisPool;
    /// Read timeout de socket para um comando bloqueante de ATimeoutSeconds.
    /// Zero significa "sem timeout" — o mesmo que o Redis entende por espera
    /// indefinida.
    function BlockingReceiveTimeoutMs(ATimeoutSeconds: Double): Integer;
  public
    /// Cliente com pool proprio, com os parametros de pool padrao.
    constructor Create(const AParams: TRedisParams); overload;
    /// Cliente com pool proprio e parametros de pool escolhidos.
    constructor Create(const AParams: TRedisParams;
      const APoolParams: TRedisPoolParams); overload;
    /// Cliente sobre um pool que ja' existe. Util para varios clientes (ou
    /// varias camadas da app) dividirem o mesmo conjunto de conexoes.
    constructor CreateOnPool(APool: TRedisPool; AOwnsPool: Boolean = False);
    /// Cliente amarrado a UMA conexao: todo comando sai por ela. E' o modo
    /// para sequencias que dependem da mesma conexao.
    constructor CreateOnConnection(AConnection: TRedisConnection;
      AOwnsConnection: Boolean = False);

    destructor Destroy; override;

    { --- Execucao generica (o kernel do M2, alcancavel daqui) --- }

    function Execute(const AName: string;
      const AArgs: array of TRedisArg): IRedisReply; overload; override;
    function ExecuteRaw(const AName: string;
      const AArgs: array of TRedisArg): IRedisReply; overload; override;
    function ExecuteBlocking(const AName: string; const AArgs: array of TRedisArg;
      ATimeoutSeconds: Double): IRedisReply; override;

    /// Envia um lote pela mesma conexao e devolve as respostas na ordem.
    /// Nao levanta em erro de servidor: cada item pode ser um rkError.
    function ExecutePipeline(APipeline: TRedisPipeline): TRedisReplyArray;

    /// PING. False se o servidor respondeu outra coisa; levanta se a conexao
    /// morreu.
    function Ping: Boolean;

    /// Abre um bloco MULTI/EXEC sobre uma conexao DEDICADA, tirada do pool e
    /// devolvida quando a transacao for liberada.
    ///
    /// O try/finally nao e' opcional: enquanto a transacao viver, aquela
    /// conexao esta' fora de circulacao, e esquecer o Free vaza uma conexao do
    /// pool a cada transacao.
    ///
    ///   LTx := LClient.BeginTransaction;
    ///   try
    ///     LTx.Watch(['saldo']);
    ///     ...
    ///   finally
    ///     LTx.Free;
    ///   end;
    ///
    /// No modo conexao unica devolve uma transacao sobre a propria conexao do
    /// cliente, e liberar a transacao nao a fecha.
    function BeginTransaction: TRedisTransaction;

    /// Cria um assinante de pub/sub com os MESMOS parametros de conexao deste
    /// cliente (host, porta, senha, banco, TLS, protocolo).
    ///
    /// O assinante NAO usa o pool: pub/sub exige conexao dedicada, e a dele
    /// nasce no Start e morre no Free. Quem chama e' dono do objeto — este
    /// metodo e um atalho de configuracao, nao um registro:
    ///
    ///   LSub := LClient.CreateSubscriber;
    ///   try
    ///     LSub.OnMessage := Chegou;
    ///     LSub.Start;
    ///     LSub.Subscribe([''noticias'']);
    ///     ...
    ///   finally
    ///     LSub.Free;
    ///   end;
    function CreateSubscriber: TRedisSubscriber;

    /// Empresta uma conexao. No modo conexao unica devolve sempre a mesma, e
    /// Release e' no-op — o que deixa o try/finally identico nos dois modos.
    function Acquire: TRedisConnection;
    /// Devolve a conexao. Passar nil e' no-op.
    procedure Release(AConnection: TRedisConnection);

    /// Fecha os pools. As conexoes emprestadas neste instante morrem quando
    /// forem devolvidas. No modo conexao unica, fecha a conexao.
    procedure Close;

    { --- Fachadas por familia --- }

    /// Comandos genericos de chave: DEL, EXPIRE, TTL, SCAN, TYPE, RENAME.
    property Keys: TRedisKeysCommands read FKeys;
    /// Comandos de string: GET, SET, INCR, MGET.
    property Strings: TRedisStringsCommands read FStrings;
    /// Comandos de hash: HSET, HGET, HGETALL, HSCAN.
    property Hashes: TRedisHashesCommands read FHashes;
    /// Comandos de lista: LPUSH, RPOP, LRANGE, BLPOP.
    property Lists: TRedisListsCommands read FLists;
    /// Comandos de conjunto: SADD, SMEMBERS, SINTER.
    property Sets: TRedisSetsCommands read FSets;
    /// Comandos de sorted set: ZADD, ZRANGE, ZINCRBY.
    property ZSets: TRedisZSetsCommands read FZSets;
    /// EVAL/EVALSHA com cache de SHA, e a familia SCRIPT.
    property Scripting: TRedisScriptingCommands read FScripting;
    /// PUBLISH e a familia PUBSUB — o lado de quem publica. Assinar e' outra
    /// historia: precisa de conexao dedicada, e quem cuida disso e' o
    /// TRedisSubscriber (CreateSubscriber, logo abaixo).
    property PubSub: TRedisPubSubCommands read FPubSub;

    /// Parametros de conexao em uso.
    property Params: TRedisParams read FParams;
    /// Pool de conexoes, ou nil no modo conexao unica.
    property Pool: TRedisPool read FPool;
    /// Conexao a que o cliente esta' amarrado, ou nil no modo pool.
    property Connection: TRedisConnection read FConnection;
  end;

implementation

{ TRedisClient }

constructor TRedisClient.Create(const AParams: TRedisParams);
begin
  Create(AParams, RedisDefaultPoolParams);
end;

constructor TRedisClient.Create(const AParams: TRedisParams;
  const APoolParams: TRedisPoolParams);
begin
  inherited Create;
  FParams := AParams;
  FPoolParams := APoolParams;
  FLock := TCriticalSection.Create;
  FPool := TRedisPool.Create(FParams, FPoolParams);
  FOwnsPool := True;
  CreateFamilies;
end;

constructor TRedisClient.CreateOnPool(APool: TRedisPool; AOwnsPool: Boolean);
begin
  inherited Create;
  if APool = nil then
    raise ERedisException.Create('TRedisClient.CreateOnPool sem pool');
  FLock := TCriticalSection.Create;
  FPool := APool;
  FOwnsPool := AOwnsPool;
  FParams := APool.Params;
  FPoolParams := APool.PoolParams;
  CreateFamilies;
end;

constructor TRedisClient.CreateOnConnection(AConnection: TRedisConnection;
  AOwnsConnection: Boolean);
begin
  inherited Create;
  if AConnection = nil then
    raise ERedisException.Create('TRedisClient.CreateOnConnection sem conexao');
  FLock := TCriticalSection.Create;
  FConnection := AConnection;
  FOwnsConnection := AOwnsConnection;
  FParams := AConnection.Params;
  FPoolParams := RedisDefaultPoolParams;
  CreateFamilies;
end;

destructor TRedisClient.Destroy;
begin
  FKeys.Free;
  FStrings.Free;
  FHashes.Free;
  FLists.Free;
  FSets.Free;
  FZSets.Free;
  FScripting.Free;
  FPubSub.Free;
  FBlockingPool.Free;
  if FOwnsPool then
    FPool.Free;
  if FOwnsConnection then
    FConnection.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TRedisClient.CreateFamilies;
begin
  // Todas as familias nascem com o cliente: sao objetos de um ponteiro so', e
  // criar sob demanda exigiria lock em toda leitura de propriedade para nada.
  FKeys := TRedisKeysCommands.Create(Self);
  FStrings := TRedisStringsCommands.Create(Self);
  FHashes := TRedisHashesCommands.Create(Self);
  FLists := TRedisListsCommands.Create(Self);
  FSets := TRedisSetsCommands.Create(Self);
  FZSets := TRedisZSetsCommands.Create(Self);
  FScripting := TRedisScriptingCommands.Create(Self);
  FPubSub := TRedisPubSubCommands.Create(Self);
end;

function TRedisClient.CreateSubscriber: TRedisSubscriber;
begin
  Result := TRedisSubscriber.Create(FParams);
end;

function TRedisClient.BeginTransaction: TRedisTransaction;
var
  LConn: TRedisConnection;
begin
  LConn := Acquire;
  try
    // Release como retorno de chamada: no modo pool devolve a conexao, no modo
    // conexao unica e' no-op. A transacao nao precisa saber a diferenca.
    Result := TRedisTransaction.Create(LConn, Release);
  except
    Release(LConn);
    raise;
  end;
end;

function TRedisClient.Acquire: TRedisConnection;
begin
  if FConnection <> nil then
  begin
    // Modo conexao unica: abre na primeira vez, como o pool faria.
    if not FConnection.IsOpen then
      FConnection.Open;
    Result := FConnection;
  end
  else
    Result := FPool.Acquire;
end;

procedure TRedisClient.Release(AConnection: TRedisConnection);
begin
  if (FConnection = nil) and (FPool <> nil) then
    FPool.Release(AConnection);
end;

function TRedisClient.Execute(const AName: string;
  const AArgs: array of TRedisArg): IRedisReply;
var
  LConn: TRedisConnection;
begin
  LConn := Acquire;
  try
    Result := LConn.Execute(AName, AArgs);
  finally
    Release(LConn);
  end;
end;

function TRedisClient.ExecuteRaw(const AName: string;
  const AArgs: array of TRedisArg): IRedisReply;
var
  LConn: TRedisConnection;
begin
  LConn := Acquire;
  try
    Result := LConn.ExecuteRaw(AName, AArgs);
  finally
    Release(LConn);
  end;
end;

function TRedisClient.ExecutePipeline(APipeline: TRedisPipeline): TRedisReplyArray;
var
  LConn: TRedisConnection;
begin
  LConn := Acquire;
  try
    Result := LConn.ExecutePipeline(APipeline);
  finally
    Release(LConn);
  end;
end;

function TRedisClient.Ping: Boolean;
var
  LConn: TRedisConnection;
begin
  LConn := Acquire;
  try
    Result := LConn.Ping;
  finally
    Release(LConn);
  end;
end;

function TRedisClient.BlockingReceiveTimeoutMs(ATimeoutSeconds: Double): Integer;
var
  LMs: Double;
begin
  // Zero ou negativo: espera indefinida dos dois lados. E' a unica situacao em
  // que a lib deixa uma conexao sem read timeout, e so' porque o comando em si
  // nao tem prazo — sem isso o socket desistiria de um BLPOP que ainda esta'
  // legitimamente esperando.
  if ATimeoutSeconds <= 0 then
    Exit(0);
  LMs := ATimeoutSeconds * 1000 + REDIS_BLOCKING_MARGIN_MS;
  // Prazo tao longo que nao cabe num Integer de milissegundos (mais de 24
  // dias) e', na pratica, espera indefinida.
  if LMs >= MaxInt then
    Exit(0);
  Result := Round(LMs);
end;

function TRedisClient.BlockingPool: TRedisPool;
begin
  FLock.Enter;
  try
    if FBlockingPool = nil then
      // Criar o pool nao abre socket — as conexoes nascem no primeiro Acquire —
      // entao isto nao e' rede sob lock.
      FBlockingPool := TRedisPool.Create(FParams, FPoolParams);
    Result := FBlockingPool;
  finally
    FLock.Leave;
  end;
end;

function TRedisClient.ExecuteBlocking(const AName: string;
  const AArgs: array of TRedisArg; ATimeoutSeconds: Double): IRedisReply;
var
  LConn: TRedisConnection;
  LPool: TRedisPool;
  LAnterior: Integer;
begin
  if FConnection <> nil then
  begin
    // Modo conexao unica: a conexao JA' e' dedicada a quem chamou, entao basta
    // esticar o prazo dela pela duracao do comando e devolver ao que era.
    LConn := Acquire;
    LAnterior := LConn.Params.ReceiveTimeoutMs;
    try
      LConn.SetReceiveTimeout(BlockingReceiveTimeoutMs(ATimeoutSeconds));
      Result := LConn.Execute(AName, AArgs);
    finally
      LConn.SetReceiveTimeout(LAnterior);
    end;
    Exit;
  end;

  LPool := BlockingPool;
  LConn := LPool.Acquire;
  try
    LConn.SetReceiveTimeout(BlockingReceiveTimeoutMs(ATimeoutSeconds));
    try
      Result := LConn.Execute(AName, AArgs);
    finally
      // Devolver ao pool com o prazo esticado faria o health check de um
      // BLPOP de 30 s esperar 30 s por um PING. Restaura antes do Release —
      // se a conexao morreu, o pool a destroi e o valor nao importa.
      if LConn.IsUsable then
        LConn.SetReceiveTimeout(FParams.ReceiveTimeoutMs);
    end;
  finally
    LPool.Release(LConn);
  end;
end;

procedure TRedisClient.Close;
begin
  if FBlockingPool <> nil then
    FBlockingPool.Close;
  if FPool <> nil then
    FPool.Close;
  if (FConnection <> nil) and FConnection.IsOpen then
    FConnection.Close;
end;

end.
