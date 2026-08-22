unit Redis.PoolTests;

{ Testes de Redis.Pool (DUnitX). Mesma cobertura do tests\Unit\fpc\Redis.PoolTests.pas
  (FPCUnit) — as duas suites sao mantidas linha a linha equivalentes, entao
  toda mudanca aqui vai para la' na mesma sessao.

  Tambem sem servidor: o TRedisFakePool sobrescreve o CreateConnection do pool
  para devolver conexoes sobre o TRedisFakeServerStream (o mesmo servidor falso
  da Redis.ConnectionTests). Isso torna testavel justamente o que e' caro de
  reproduzir contra um Redis de verdade: conexao ociosa que o servidor derrubou
  no meio do sono, conexao devolvida suja, poda por ociosidade e o teto do pool
  com uma thread esperando a devolucao de outra. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  Redis.Types,
  Redis.Connection,
  Redis.Pool,
  Redis.ConnectionTests,
  Redis.DUnitXCompat;

type
  { Pool que abre conexoes sobre um servidor falso em vez de socket.

    O roteiro de respostas e' o mesmo para toda conexao que ele abrir; trocar o
    roteiro muda o comportamento do "servidor" (roteiro vazio = servidor que
    aceita a conexao e nunca responde, que e' como se testa o health check
    reprovando). }
  TRedisFakePool = class(TRedisPool)
  private
    FScript: array of string;
    FAbertas: Integer;
    FFalharApos: Integer;
  protected
    function CreateConnection: TRedisConnection; override;
  public
    constructor Create(const AParams: TRedisParams;
      const APoolParams: TRedisPoolParams);
    /// Respostas que cada conexao nova vai encontrar.
    procedure UsarRoteiro(const ARespostas: array of string);
    /// Quantas conexoes o pool realmente abriu (o CreatedCount so' conta as
    /// que abriram com sucesso; este conta as tentativas bem-sucedidas de
    /// fabrica, que e' o que os testes de reuso querem saber).
    property Abertas: Integer read FAbertas;
    /// Depois de N conexoes abertas, a proxima falha. -1 (padrao) nunca falha.
    property FalharApos: Integer read FFalharApos write FFalharApos;
  end;

  { Devolve uma conexao ao pool depois de um tempo, de outra thread. E' o que
    permite testar que Acquire ESPERA pela devolucao em vez de levantar na
    hora. }
  TRedisReleaserThread = class(TThread)
  private
    FPool: TRedisPool;
    FConn: TRedisConnection;
    FDelayMs: Integer;
  protected
    procedure Execute; override;
  public
    constructor Create(APool: TRedisPool; AConn: TRedisConnection;
      ADelayMs: Integer);
  end;

  [TestFixture]
  TRedisPoolParamsTests = class
  public
    [Test] procedure Default_TemTetoEPrazos;
    [Test] procedure MaxSizeInvalido_ViraUm;
  end;

  [TestFixture]
  TRedisPoolTests = class
  public
    [Test] procedure Acquire_AbreConexaoNova;
    [Test] procedure Release_DevolveParaOcioso;
    [Test] procedure Acquire_ReusaAConexaoOciosa;
    [Test] procedure Acquire_NoTeto_LevantaDepoisDoPrazo;
    [Test] procedure Acquire_SemEspera_LevantaNaHora;
    [Test] procedure Acquire_EsperaADevolucaoDeOutraThread;
    [Test] procedure Release_ConexaoInvalidada_Descarta;
    [Test] procedure Release_ConexaoSuja_Descarta;
    [Test] procedure Release_BancoTrocado_Descarta;
    [Test] procedure Release_Nil_NaoExplode;
    [Test] procedure HealthCheck_OciosaQueCaiu_ETrocada;
    [Test] procedure HealthCheck_Desligado_NaoConfere;
    [Test] procedure Ociosa_VencidaPorTempo_EPodada;
    [Test] procedure Falha_AoAbrir_DevolveAVaga;
    [Test] procedure Close_FechaAsOciosasERecusaAcquire;
    [Test] procedure Close_NaoDerrubaConexaoEmprestada;
  end;

implementation

const
  CRLF = #13#10;

{ Helpers compartilhados }

// Parametros de conexao que nao emitem nada no handshake (sem senha, sem nome,
// banco 0): assim o roteiro do servidor falso so' precisa cobrir os comandos
// que o teste realmente manda.
function ParamsMudos: TRedisParams;
begin
  Result := RedisDefaultParams;
end;

function PoolParams(AMaxSize, AAcquireMs, AIdleMs, AHealthMs: Integer): TRedisPoolParams;
begin
  Result.MaxSize := AMaxSize;
  Result.AcquireTimeoutMs := AAcquireMs;
  Result.IdleTimeoutMs := AIdleMs;
  Result.HealthCheckAfterIdleMs := AHealthMs;
end;

{ TRedisFakePool }

constructor TRedisFakePool.Create(const AParams: TRedisParams;
  const APoolParams: TRedisPoolParams);
var
  I: Integer;
begin
  inherited Create(AParams, APoolParams);
  FFalharApos := -1;
  // Roteiro padrao: PONG a vontade, um por pacote. Cobre o health check e o
  // PING que os testes mandam, sem ninguem ter de contar quantos foram.
  SetLength(FScript, 32);
  for I := 0 to High(FScript) do
    FScript[I] := '+PONG' + CRLF;
end;

procedure TRedisFakePool.UsarRoteiro(const ARespostas: array of string);
var
  I: Integer;
begin
  SetLength(FScript, Length(ARespostas));
  for I := 0 to High(ARespostas) do
    FScript[I] := ARespostas[I];
end;

function TRedisFakePool.CreateConnection: TRedisConnection;
var
  LFake: TRedisFakeServerStream;
begin
  if (FFalharApos >= 0) and (FAbertas >= FFalharApos) then
    raise ERedisConnectionLost.Create('servidor falso recusou a conexao');
  LFake := TRedisFakeServerStream.Create(FScript);
  Result := TRedisConnection.CreateOnStream(LFake, Params);
  try
    Result.Open;
  except
    Result.Free;
    raise;
  end;
  Inc(FAbertas);
end;

{ TRedisReleaserThread }

constructor TRedisReleaserThread.Create(APool: TRedisPool;
  AConn: TRedisConnection; ADelayMs: Integer);
begin
  inherited Create(True);
  FreeOnTerminate := False;  // o teste faz WaitFor e Free
  FPool := APool;
  FConn := AConn;
  FDelayMs := ADelayMs;
end;

procedure TRedisReleaserThread.Execute;
begin
  Sleep(FDelayMs);
  FPool.Release(FConn);
end;

{ TRedisPoolParamsTests }

procedure TRedisPoolParamsTests.Default_TemTetoEPrazos;
var
  LParams: TRedisPoolParams;
begin
  LParams := RedisDefaultPoolParams;
  TAssert.AssertTrue('teto positivo', LParams.MaxSize > 0);
  TAssert.AssertTrue('espera por conexao', LParams.AcquireTimeoutMs > 0);
  TAssert.AssertTrue('poda por ociosidade', LParams.IdleTimeoutMs > 0);
  TAssert.AssertTrue('health check', LParams.HealthCheckAfterIdleMs > 0);
end;

procedure TRedisPoolParamsTests.MaxSizeInvalido_ViraUm;
var
  LPool: TRedisFakePool;
begin
  // Um pool de zero conexao nao emprestaria nada e travaria a app inteira no
  // primeiro Acquire; e' erro de configuracao que nao vale propagar.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(0, 100, 0, -1));
  try
    TAssert.AssertEquals(1, LPool.PoolParams.MaxSize);
  finally
    LPool.Free;
  end;
end;

{ TRedisPoolTests }

procedure TRedisPoolTests.Acquire_AbreConexaoNova;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LConn := LPool.Acquire;
    TAssert.AssertTrue('devia vir uma conexao', LConn <> nil);
    TAssert.AssertTrue('e ela devia estar aberta', LConn.IsOpen);
    TAssert.AssertEquals('vivas', 1, LPool.TotalCount);
    TAssert.AssertEquals('emprestadas', 1, LPool.InUseCount);
    TAssert.AssertEquals('ociosas', 0, LPool.IdleCount);
    TAssert.AssertEquals(Int64(1), LPool.CreatedCount);
    LPool.Release(LConn);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Release_DevolveParaOcioso;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LConn := LPool.Acquire;
    LPool.Release(LConn);
    TAssert.AssertEquals('vivas', 1, LPool.TotalCount);
    TAssert.AssertEquals('emprestadas', 0, LPool.InUseCount);
    TAssert.AssertEquals('ociosas', 1, LPool.IdleCount);
    TAssert.AssertEquals('nada descartado', Int64(0), LPool.DiscardedCount);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Acquire_ReusaAConexaoOciosa;
var
  LPool: TRedisFakePool;
  LPrimeira, LSegunda: TRedisConnection;
begin
  // Reuso e' a razao de o pool existir: abrir socket e refazer handshake a
  // cada comando custa mais do que o comando.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LPrimeira := LPool.Acquire;
    LPool.Release(LPrimeira);
    LSegunda := LPool.Acquire;
    TAssert.AssertTrue('devia ser a MESMA conexao', LPrimeira = LSegunda);
    TAssert.AssertEquals('so uma foi aberta', 1, LPool.Abertas);
    TAssert.AssertEquals(Int64(1), LPool.CreatedCount);
    LPool.Release(LSegunda);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Acquire_NoTeto_LevantaDepoisDoPrazo;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
  LClasse: string;
begin
  // No teto, o pool espera e depois desiste — nao abre socket sem limite ate'
  // o servidor recusar todo mundo com 'max number of clients reached'.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(1, 50, 0, -1));
  try
    LConn := LPool.Acquire;
    LClasse := '';
    try
      LPool.Acquire;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisPoolExhausted', LClasse);
    TAssert.AssertEquals('o teto foi respeitado', 1, LPool.TotalCount);
    LPool.Release(LConn);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Acquire_SemEspera_LevantaNaHora;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
  LClasse: string;
begin
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(1, 0, 0, -1));
  try
    LConn := LPool.Acquire;
    LClasse := '';
    try
      LPool.Acquire;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisPoolExhausted', LClasse);
    LPool.Release(LConn);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Acquire_EsperaADevolucaoDeOutraThread;
var
  LPool: TRedisFakePool;
  LPrimeira, LSegunda: TRedisConnection;
  LThread: TRedisReleaserThread;
begin
  // O contrato do teto: no limite, Acquire BLOQUEIA ate' alguem devolver.
  // Se o pool nao esperasse, este teste levantaria ERedisPoolExhausted; se
  // esperasse para sempre, o proprio AcquireTimeoutMs (1 s) desata o no e o
  // teste falha por excecao, nunca por travamento da suite.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(1, 1000, 0, -1));
  try
    LPrimeira := LPool.Acquire;
    LThread := TRedisReleaserThread.Create(LPool, LPrimeira, 50);
    try
      LThread.Start;
      LSegunda := LPool.Acquire;
      TAssert.AssertTrue('devia receber a conexao devolvida',
        LPrimeira = LSegunda);
      TAssert.AssertEquals('sem abrir outra', 1, LPool.Abertas);
      LPool.Release(LSegunda);
    finally
      LThread.WaitFor;
      LThread.Free;
    end;
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Release_ConexaoInvalidada_Descarta;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  // Roteiro vazio = servidor que aceita a conexao e nunca responde. O comando
  // morre de fim de fluxo e invalida a conexao.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LPool.UsarRoteiro([]);
    LConn := LPool.Acquire;
    try
      LConn.Execute('PING');
    except
      on E: ERedisConnectionLost do
        ;
    end;
    TAssert.AssertTrue('a conexao devia estar invalidada', LConn.IsBroken);
    LPool.Release(LConn);
    TAssert.AssertEquals('nao pode virar ociosa', 0, LPool.IdleCount);
    TAssert.AssertEquals('nem continuar viva', 0, LPool.TotalCount);
    TAssert.AssertEquals(Int64(1), LPool.DiscardedCount);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Release_ConexaoSuja_Descarta;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  // Duas respostas no mesmo pacote para um comando so': sobra uma no buffer.
  // A conexao continua ABERTA e sa' do ponto de vista de I/O — e e' justamente
  // por isso que este caso e' perigoso: sem a checagem, ela voltaria ao pool e
  // o proximo comando leria a resposta atrasada deste.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LPool.UsarRoteiro(['+PONG' + CRLF + '+PONG' + CRLF]);
    LConn := LPool.Acquire;
    LConn.Execute('PING');
    TAssert.AssertTrue('devia estar suja', LConn.IsDirty);
    TAssert.AssertFalse('mas nao invalidada', LConn.IsBroken);
    TAssert.AssertTrue('e ainda aberta', LConn.IsOpen);
    LPool.Release(LConn);
    TAssert.AssertEquals('conexao suja nao volta ao pool', 0, LPool.IdleCount);
    TAssert.AssertEquals(0, LPool.TotalCount);
    TAssert.AssertEquals(Int64(1), LPool.DiscardedCount);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Release_BancoTrocado_Descarta;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  // O SELECT vive na conexao, nao no servidor. Reciclar uma conexao em outro
  // banco entregaria a proxima thread um banco diferente do configurado, e o
  // estrago (ler e gravar no banco errado) apareceria bem longe daqui.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LPool.UsarRoteiro(['+OK' + CRLF]);
    LConn := LPool.Acquire;
    LConn.Select(3);
    TAssert.AssertEquals(3, LConn.Database);
    LPool.Release(LConn);
    TAssert.AssertEquals(0, LPool.IdleCount);
    TAssert.AssertEquals(Int64(1), LPool.DiscardedCount);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Release_Nil_NaoExplode;
var
  LPool: TRedisFakePool;
begin
  // Deixa o finally ser escrito sem if, mesmo quando o Acquire levantou.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LPool.Release(nil);
    TAssert.AssertEquals(0, LPool.TotalCount);
    TAssert.AssertEquals(0, LPool.InUseCount);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.HealthCheck_OciosaQueCaiu_ETrocada;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  // Quem derruba conexao parada e' o servidor (timeout do lado de la',
  // failover, CLIENT KILL) e ninguem avisa o cliente: sem o PING de saude, a
  // descoberta aconteceria no comando do usuario. Roteiro vazio faz o PING
  // encontrar o fim do fluxo, que e' exatamente esse cenario.
  //
  // A troca e' verificada pelos contadores, e NAO comparando o ponteiro da
  // conexao velha com o da nova: o gerenciador de memoria costuma devolver
  // exatamente o mesmo endereco no lugar do bloco que acabou de ser liberado,
  // entao 'velha <> nova' falha mesmo quando a troca aconteceu (e ainda por
  // cima lendo um ponteiro morto).
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, 0));
  try
    LPool.UsarRoteiro([]);
    LConn := LPool.Acquire;
    LPool.Release(LConn);
    TAssert.AssertEquals('ficou ociosa', 1, LPool.IdleCount);

    LConn := LPool.Acquire;
    TAssert.AssertTrue('a conexao emprestada tem de estar aberta', LConn.IsOpen);
    TAssert.AssertEquals('a que caiu foi descartada', Int64(1),
      LPool.DiscardedCount);
    TAssert.AssertEquals('e uma nova entrou no lugar', 2, LPool.Abertas);
    TAssert.AssertEquals('sem inflar o pool', 1, LPool.TotalCount);
    LPool.Release(LConn);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.HealthCheck_Desligado_NaoConfere;
var
  LPool: TRedisFakePool;
  LPrimeira, LSegunda: TRedisConnection;
begin
  // Health check negativo = nunca confere. Com o mesmo roteiro vazio do teste
  // anterior, a conexao volta intacta — o que prova que o PING de saude so'
  // acontece por causa do parametro, e nao por acaso.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LPool.UsarRoteiro([]);
    LPrimeira := LPool.Acquire;
    LPool.Release(LPrimeira);
    LSegunda := LPool.Acquire;
    TAssert.AssertTrue('devia ser a mesma', LPrimeira = LSegunda);
    TAssert.AssertEquals('sem descarte', Int64(0), LPool.DiscardedCount);
    TAssert.AssertEquals('sem conexao nova', 1, LPool.Abertas);
    LPool.Release(LSegunda);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Ociosa_VencidaPorTempo_EPodada;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  // Poda por ociosidade: e' como o pool encolhe depois de um pico, em vez de
  // segurar para sempre os sockets do momento de maior carga. Aqui tambem os
  // contadores e' que atestam a troca — ponteiro de objeto liberado costuma
  // ser reaproveitado pelo alocador no objeto seguinte.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 1, -1));
  try
    LConn := LPool.Acquire;
    LPool.Release(LConn);
    Sleep(20);  // passa do IdleTimeoutMs de 1 ms
    LConn := LPool.Acquire;
    TAssert.AssertEquals('a vencida foi podada', Int64(1), LPool.DiscardedCount);
    TAssert.AssertEquals('e outra foi aberta', 2, LPool.Abertas);
    TAssert.AssertEquals('sem inflar o pool', 1, LPool.TotalCount);
    LPool.Release(LConn);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Falha_AoAbrir_DevolveAVaga;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
  LClasse: string;
begin
  // A vaga e' reservada ANTES de soltar o lock (senao N threads estourariam o
  // teto juntas). Se a conexao nao abre, a reserva TEM de voltar — senao o
  // pool encolhe a cada servidor fora do ar ate' nao emprestar mais nada, e a
  // app so' volta ao normal com restart.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(2, 50, 0, -1));
  try
    LPool.FalharApos := 0;
    LClasse := '';
    try
      LPool.Acquire;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisConnectionLost', LClasse);
    TAssert.AssertEquals('a vaga voltou', 0, LPool.TotalCount);
    TAssert.AssertEquals('e ninguem ficou como emprestada', 0, LPool.InUseCount);

    LPool.FalharApos := -1;
    LConn := LPool.Acquire;  // o pool tem de voltar a funcionar sozinho
    TAssert.AssertTrue('devia emprestar de novo', LConn <> nil);
    LPool.Release(LConn);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Close_FechaAsOciosasERecusaAcquire;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
  LClasse: string;
begin
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LConn := LPool.Acquire;
    LPool.Release(LConn);
    LPool.Close;
    TAssert.AssertEquals('as ociosas foram fechadas', 0, LPool.IdleCount);
    TAssert.AssertEquals(0, LPool.TotalCount);
    LClasse := '';
    try
      LPool.Acquire;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisPoolClosed', LClasse);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolTests.Close_NaoDerrubaConexaoEmprestada;
var
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  // Fechar o pool nao pode derrubar o socket embaixo de quem esta' no meio de
  // um comando: isso trocaria um shutdown ordenado por uma excecao em outra
  // thread. A emprestada morre na devolucao.
  LPool := TRedisFakePool.Create(ParamsMudos, PoolParams(4, 100, 0, -1));
  try
    LConn := LPool.Acquire;
    LPool.Close;
    TAssert.AssertTrue('a emprestada continua aberta', LConn.IsOpen);
    TAssert.AssertTrue('e utilizavel', LConn.IsUsable);
    LPool.Release(LConn);
    TAssert.AssertEquals('e e destruida na devolucao', 0, LPool.TotalCount);
    TAssert.AssertEquals(0, LPool.IdleCount);
  finally
    LPool.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisPoolParamsTests);
  TDUnitX.RegisterTestFixture(TRedisPoolTests);

end.
