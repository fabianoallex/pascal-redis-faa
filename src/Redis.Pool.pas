unit Redis.Pool;

{ Pool de conexoes: a unidade de concorrencia da lib.

  O AMQP multiplexa canais sobre um socket; o Redis nao multiplexa nada — uma
  conexao processa um comando por vez, em ordem estrita. Entao o que a
  aplicacao segura nao e' uma conexao, e' o pool: N threads, N conexoes, cada
  uma sozinha com o seu socket enquanto durar o comando.

  O ciclo e' sempre o mesmo, e o try/finally nao e' opcional:

      LConn := LPool.Acquire;
      try
        LConn.Execute('SET', ['k', 'v']);
      finally
        LPool.Release(LConn);   // devolver SEMPRE, inclusive em excecao
      end;

  O que este pool faz alem de guardar objetos:

  1. **Descarta conexao que nao serve.** Na devolucao, uma conexao invalidada
     (erro de I/O, timeout, fluxo malformado) ou SUJA (sobrou byte no buffer)
     e' destruida, nunca reciclada. Devolver uma conexao suja ao pool e' o bug
     classico de cliente Redis: o proximo comando leria a resposta atrasada do
     anterior e, dali em diante, todo valor sai deslocado por um.

  2. **Confere antes de emprestar.** Conexao parada ha' mais que
     HealthCheckAfterIdleMs leva um PING antes de ir para as maos de alguem —
     porque quem derruba conexao ociosa e' o servidor (timeout do lado de la',
     failover, `CLIENT KILL`) e ninguem avisa o cliente. O PING acontece FORA
     do lock do pool: uma ida e volta de rede segurando o lock congelaria todas
     as outras threads.

  3. **Reconecta, no unico sentido que o Redis permite.** Nao existe "retomar"
     uma conexao morta: o socket foi embora com o comando em voo. O que o pool
     faz e' abrir uma conexao NOVA no lugar, e o handshake dela ja' replaya o
     que havia de estado (HELLO/AUTH, CLIENT SETNAME, SELECT) — o Redis nao
     guarda topologia no servidor, ao contrario do AMQP. O comando que estava
     em voo NAO e' repetido: INCR, LPUSH e SETNX nao sao idempotentes, e a
     decisao de repetir e' de quem chamou.

  4. **Bounda a concorrencia.** No teto de MaxSize, Acquire espera por uma
     devolucao ate' AcquireTimeoutMs e entao levanta ERedisPoolExhausted — em
     vez de abrir socket sem limite ate' o servidor recusar (`max number of
     clients reached`), que e' a falha que derruba as outras aplicacoes que
     dividem o mesmo Redis.

  Thread-safe. O estado interno anda sob um TRedisMonitor, e toda operacao de
  rede (abrir conexao, PING de saude, fechar socket) acontece fora do lock. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Threading,
  Redis.Connection;

const
  /// Teto padrao de conexoes vivas. Dez cobre com folga uma app de UI ou um
  /// servico web modesto; quem precisa de mais sabe que precisa.
  REDIS_DEFAULT_POOL_SIZE = 10;

  /// Espera padrao por uma conexao quando o pool esta' no teto.
  REDIS_DEFAULT_ACQUIRE_TIMEOUT_MS = 5000;

  /// Conexao ociosa por mais que isto e' fechada em vez de emprestada (5 min).
  /// Serve para o pool encolher sozinho depois de um pico e para nao segurar
  /// socket que o servidor ja' esta' a ponto de derrubar.
  REDIS_DEFAULT_IDLE_TIMEOUT_MS = 300000;

  /// Acima deste tempo parada, a conexao leva um PING antes de ser emprestada
  /// (30 s). Abaixo disso o risco nao paga o round-trip.
  REDIS_DEFAULT_HEALTH_CHECK_MS = 30000;

type
  /// O pool esta' no teto e ninguem devolveu conexao dentro do prazo.
  ///
  /// Quase sempre significa uma de duas coisas: falta `Release` em algum
  /// try/finally, ou o MaxSize esta' abaixo do numero de threads que realmente
  /// falam com o Redis ao mesmo tempo.
  ERedisPoolExhausted = class(ERedisException);

  /// Uso do pool depois do Close (ou durante a destruicao).
  ERedisPoolClosed = class(ERedisException);

  /// Parametros do pool. Preencha com RedisDefaultPoolParams e ajuste o que
  /// interessa.
  TRedisPoolParams = record
    /// Maximo de conexoes vivas (emprestadas + ociosas). Minimo 1.
    MaxSize: Integer;
    /// Quanto esperar por uma devolucao quando o pool esta' no teto.
    /// Zero = nao espera: ou tem conexao livre, ou levanta.
    AcquireTimeoutMs: Integer;
    /// Idade maxima de uma conexao ociosa. Zero desliga a poda.
    IdleTimeoutMs: Integer;
    /// Tempo de ociosidade a partir do qual vale conferir com PING antes de
    /// emprestar. Zero confere sempre; negativo nunca confere.
    HealthCheckAfterIdleMs: Integer;
  end;

  /// Pool de conexoes com um servidor Redis.
  TRedisPool = class
  private
    FParams: TRedisParams;
    FPoolParams: TRedisPoolParams;
    FMonitor: TRedisMonitor;
    FIdle: array of TRedisConnection;
    FIdleSince: array of UInt64;
    FIdleCount: Integer;
    FTotal: Integer;      // vivas: emprestadas + ociosas
    FInUse: Integer;
    FClosing: Boolean;
    FCreated: Int64;
    FDiscarded: Int64;
    procedure PushIdle(AConnection: TRedisConnection);
    function PopIdle(out AIdleMs: UInt64): TRedisConnection;
    /// Devolve a vaga que uma conexao ocupava. Chamar SEGURANDO o lock.
    procedure GiveBackSlot;
  protected
    /// Cria e ABRE uma conexao. Ponto de extensao: as suites unitarias
    /// sobrescrevem para devolver conexoes sobre um servidor falso em memoria,
    /// o que torna a logica do pool testavel sem rede.
    function CreateConnection: TRedisConnection; virtual;
  public
    constructor Create(const AParams: TRedisParams); overload;
    constructor Create(const AParams: TRedisParams;
      const APoolParams: TRedisPoolParams); overload;
    destructor Destroy; override;

    /// Empresta uma conexao aberta e sa'. Reusa uma ociosa quando ha'; senao
    /// abre uma nova, respeitando o MaxSize.
    ///
    /// Levanta ERedisPoolExhausted se o pool estiver no teto e ninguem
    /// devolver a tempo, e propaga a falha de conexao (ERedisConnectionLost)
    /// quando o servidor esta' fora do ar.
    function Acquire: TRedisConnection;

    /// Devolve a conexao ao pool — SEMPRE, inclusive quando o comando
    /// levantou excecao. Conexao que nao esta' utilizavel (invalidada, suja ou
    /// com o banco trocado) e' destruida em vez de reciclada.
    ///
    /// Passar nil e' no-op, para o finally poder ser escrito sem if.
    procedure Release(AConnection: TRedisConnection);

    /// Fecha as conexoes ociosas e passa a recusar novos Acquire. As
    /// emprestadas neste momento sao destruidas quando forem devolvidas — o
    /// pool nao derruba socket embaixo de quem esta' no meio de um comando.
    procedure Close;

    /// Parametros de conexao usados para abrir cada conexao do pool.
    property Params: TRedisParams read FParams;
    property PoolParams: TRedisPoolParams read FPoolParams;

    /// Conexoes vivas: emprestadas + ociosas.
    property TotalCount: Integer read FTotal;
    /// Conexoes emprestadas neste instante.
    property InUseCount: Integer read FInUse;
    /// Conexoes prontas para emprestar.
    property IdleCount: Integer read FIdleCount;
    /// Quantas conexoes o pool ja' abriu desde que existe.
    property CreatedCount: Int64 read FCreated;
    /// Quantas ele destruiu (invalidada, suja, vencida por ociosidade ou
    /// reprovada no health check). Crescendo rapido, e' sintoma: ou o servidor
    /// esta' derrubando conexao, ou o codigo esta' sujando conexao.
    property DiscardedCount: Int64 read FDiscarded;
  end;

/// Parametros de pool com os defaults saos.
function RedisDefaultPoolParams: TRedisPoolParams;

implementation

function RedisDefaultPoolParams: TRedisPoolParams;
begin
  Result.MaxSize := REDIS_DEFAULT_POOL_SIZE;
  Result.AcquireTimeoutMs := REDIS_DEFAULT_ACQUIRE_TIMEOUT_MS;
  Result.IdleTimeoutMs := REDIS_DEFAULT_IDLE_TIMEOUT_MS;
  Result.HealthCheckAfterIdleMs := REDIS_DEFAULT_HEALTH_CHECK_MS;
end;

{ TRedisPool }

constructor TRedisPool.Create(const AParams: TRedisParams);
begin
  Create(AParams, RedisDefaultPoolParams);
end;

constructor TRedisPool.Create(const AParams: TRedisParams;
  const APoolParams: TRedisPoolParams);
begin
  inherited Create;
  FParams := AParams;
  FPoolParams := APoolParams;
  if FPoolParams.MaxSize < 1 then
    FPoolParams.MaxSize := 1;
  if FPoolParams.AcquireTimeoutMs < 0 then
    FPoolParams.AcquireTimeoutMs := 0;
  FMonitor := TRedisMonitor.Create;
end;

destructor TRedisPool.Destroy;
begin
  try
    Close;
  except
    // Destrutor nao propaga.
  end;
  FMonitor.Free;
  inherited;
end;

procedure TRedisPool.PushIdle(AConnection: TRedisConnection);
begin
  if FIdleCount = Length(FIdle) then
  begin
    SetLength(FIdle, FIdleCount + 8);
    SetLength(FIdleSince, FIdleCount + 8);
  end;
  FIdle[FIdleCount] := AConnection;
  FIdleSince[FIdleCount] := RedisTickMs;
  Inc(FIdleCount);
end;

function TRedisPool.PopIdle(out AIdleMs: UInt64): TRedisConnection;
begin
  if FIdleCount = 0 then
  begin
    AIdleMs := 0;
    Exit(nil);
  end;
  // LIFO: a ultima devolvida e' a primeira a sair. A conexao mais quente
  // (buffers do SO aquecidos, menos chance de ter sido derrubada) volta a
  // circular, e as do fundo envelhecem ate' a poda por ociosidade — que e'
  // como o pool encolhe sozinho depois de um pico.
  Dec(FIdleCount);
  Result := FIdle[FIdleCount];
  AIdleMs := RedisTickMs - FIdleSince[FIdleCount];
  FIdle[FIdleCount] := nil;
end;

procedure TRedisPool.GiveBackSlot;
begin
  Dec(FTotal);
  Inc(FDiscarded);
  FMonitor.PulseAll;  // ha' vaga: quem estava esperando pode tentar de novo
end;

function TRedisPool.CreateConnection: TRedisConnection;
begin
  Result := TRedisConnection.Create(FParams);
  try
    Result.Open;
  except
    Result.Free;
    raise;
  end;
end;

function TRedisPool.Acquire: TRedisConnection;
var
  LConn: TRedisConnection;
  LIdleMs: UInt64;
  LDeadline, LNow: UInt64;
  LCreate, LCheck, LSaudavel: Boolean;
  LWaitMs: UInt64;
begin
  LDeadline := RedisTickMs + UInt64(FPoolParams.AcquireTimeoutMs);
  while True do
  begin
    LConn := nil;
    LCreate := False;
    LCheck := False;

    FMonitor.Enter;
    try
      if FClosing then
        raise ERedisPoolClosed.Create('o pool foi fechado');

      // 1. Procura uma ociosa aproveitavel, podando as vencidas no caminho.
      while (LConn = nil) and (FIdleCount > 0) do
      begin
        LConn := PopIdle(LIdleMs);
        if (FPoolParams.IdleTimeoutMs > 0) and
           (LIdleMs > UInt64(FPoolParams.IdleTimeoutMs)) then
        begin
          // Vencida por ociosidade: fecha aqui mesmo. O Free so' encosta no
          // socket (shutdown/close), nao espera rede.
          LConn.Free;
          LConn := nil;
          GiveBackSlot;
        end
        else
          LCheck := (FPoolParams.HealthCheckAfterIdleMs >= 0) and
                    (LIdleMs >= UInt64(FPoolParams.HealthCheckAfterIdleMs));
      end;

      // 2. Sem ociosa: abre uma nova, se couber no teto.
      if LConn = nil then
      begin
        if FTotal < FPoolParams.MaxSize then
        begin
          // Reserva a vaga ANTES de soltar o lock: sem isto, N threads
          // veriam o mesmo "cabe mais uma" e o pool estouraria o MaxSize.
          Inc(FTotal);
          LCreate := True;
        end
        else
        begin
          // 3. No teto: espera devolucao ate' o prazo.
          LNow := RedisTickMs;
          if LNow >= LDeadline then
            raise ERedisPoolExhausted.CreateFmt(
              'o pool chegou ao teto de %d conexoes e nenhuma foi devolvida ' +
              'em %d ms (falta um Release em algum finally?)',
              [FPoolParams.MaxSize, FPoolParams.AcquireTimeoutMs]);
          LWaitMs := LDeadline - LNow;
          FMonitor.Wait(Cardinal(LWaitMs));
          Continue;  // reavalia sob o lock: o Wait pode acordar sem motivo
        end;
      end;
      Inc(FInUse);
    finally
      FMonitor.Leave;
    end;

    // --- daqui para baixo, fora do lock: so' rede ---

    if LCreate then
    begin
      try
        Result := CreateConnection;
      except
        // A vaga reservada tem de voltar, senao o pool encolhe a cada falha
        // de conexao ate' nao emprestar mais nada.
        FMonitor.Enter;
        try
          Dec(FInUse);
          GiveBackSlot;
        finally
          FMonitor.Leave;
        end;
        raise;
      end;
      FMonitor.Enter;
      try
        Inc(FCreated);
      finally
        FMonitor.Leave;
      end;
      Exit;
    end;

    if not LCheck then
      Exit(LConn);

    // Health check da ociosa. Quem derruba conexao parada e' o servidor
    // (timeout do lado de la', failover, CLIENT KILL) e o cliente so' descobre
    // no proximo comando — que seria o comando do usuario, nao este PING.
    LSaudavel := False;
    try
      LSaudavel := LConn.Ping;
    except
      on E: ERedisException do
        LSaudavel := False;  // caiu: descarta e tenta outra
    end;
    if LSaudavel then
      Exit(LConn);

    LConn.Free;
    FMonitor.Enter;
    try
      Dec(FInUse);
      GiveBackSlot;
    finally
      FMonitor.Leave;
    end;
    // Volta ao topo: pega outra ociosa ou abre uma nova no lugar desta.
  end;
end;

procedure TRedisPool.Release(AConnection: TRedisConnection);
var
  LDescarta: Boolean;
begin
  if AConnection = nil then
    Exit;  // deixa o finally ser escrito sem if
  FMonitor.Enter;
  try
    Dec(FInUse);
    // Tres motivos para nao reciclar:
    //   - o pool esta' fechando;
    //   - a conexao nao esta' utilizavel: invalidada por erro de I/O, ou SUJA
    //     (sobrou byte no buffer, entao o proximo comando leria a resposta
    //     atrasada deste);
    //   - o banco corrente nao e' mais o configurado. O SELECT vive na
    //     conexao, nao no servidor: reciclar assim entregaria a proxima thread
    //     um banco diferente do que ela pediu, e o estrago apareceria longe
    //     daqui.
    LDescarta := FClosing or (not AConnection.IsUsable) or
                 (AConnection.Database <> FParams.Database);
    if LDescarta then
      GiveBackSlot
    else
    begin
      PushIdle(AConnection);
      FMonitor.PulseAll;
    end;
  finally
    FMonitor.Leave;
  end;
  if LDescarta then
    AConnection.Free;  // fora do lock
end;

procedure TRedisPool.Close;
var
  LParaFechar: array of TRedisConnection;
  I: Integer;
begin
  LParaFechar := nil;
  FMonitor.Enter;
  try
    FClosing := True;
    SetLength(LParaFechar, FIdleCount);
    for I := 0 to FIdleCount - 1 do
    begin
      LParaFechar[I] := FIdle[I];
      FIdle[I] := nil;
      Dec(FTotal);
    end;
    FIdleCount := 0;
    // As emprestadas continuam vivas: derrubar o socket embaixo de quem esta'
    // no meio de um comando trocaria um shutdown ordenado por uma excecao em
    // outra thread. Elas morrem no Release, que ve o FClosing.
    FMonitor.PulseAll;
  finally
    FMonitor.Leave;
  end;
  for I := 0 to High(LParaFechar) do
    LParaFechar[I].Free;
end;

end.
