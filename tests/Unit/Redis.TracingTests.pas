unit Redis.TracingTests;

{ Testes de Redis.Tracing e dos spans que a conexao e o pool abrem (DUnitX).
  Mesma cobertura do tests\Unit\fpc\Redis.TracingTests.pas (FPCUnit) — as duas
  suites sao mantidas linha a linha equivalentes, entao toda mudanca aqui vai
  para la' na mesma sessao.

  Sem servidor e sem coletor: o tracer da pascal-common-faa e' ligado com um
  exportador que so' guarda os spans (TRedisSpanCollector) e sem thread de
  exportacao (AAutoFlush = False); o teste chama FlushNow e le o que chegou.
  A conexao roda sobre o TRedisFakeServerStream da Redis.ConnectionTests, e o
  pool e' o TRedisFakePool da Redis.PoolTests.

  O tracer e' estado do processo: todo teste que o liga desliga num finally,
  senao os spans de um teste vazariam para o seguinte. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  PascalCommon.Tracing,
  Redis.Types,
  Redis.Connection,
  Redis.Pool,
  Redis.Tracing,
  Redis.ConnectionTests,
  Redis.PoolTests,
  Redis.DUnitXCompat;

type
  { Exportador que guarda os spans recebidos. }
  TRedisSpanCollector = class(TInterfacedObject, IPcSpanExporter)
  private
    FSpans: TPcSpanDataArray;
  public
    function ExportSpans(const ASpans: TPcSpanDataArray; out AError: string): Boolean;
    property Spans: TPcSpanDataArray read FSpans;
  end;

  [TestFixture]
  TRedisTracingTextTests = class
  public
    [Test] procedure Operacao_ComandoEmMaiusculas;
    [Test] procedure Operacao_ContainerJuntaOSubcomando;
    [Test] procedure Operacao_SubcomandoEstranho_FicaSoOComando;
    [Test] procedure Texto_SoAChave;
    [Test] procedure Texto_ValorViraInterrogacao;
    [Test] procedure Texto_SemChave_NaoMostraArgumento;
    [Test] procedure Texto_ContainerComChave;
    [Test] procedure Texto_ContainerSemChave;
    [Test] procedure Texto_EvalPegaAChaveDepoisDoNumkeys;
    [Test] procedure Texto_EvalSemChave;
    [Test] procedure Texto_XReadPegaAChaveDepoisDeStreams;
    [Test] procedure Texto_ChaveLonga_ECortada;
    [Test] procedure Texto_NomeDentroDoArray;
    [Test] procedure Texto_ChaveBinaria_NaoLevanta;
  end;

  [TestFixture]
  TRedisTracingSpanTests = class
  public
    [Test] procedure Desligado_NaoCriaSpan;
    [Test] procedure Execute_AbreSpanClientComAtributos;
    [Test] procedure Execute_FilhoDoSpanCorrente;
    [Test] procedure ExecuteArgs_UsaONomeDoArray;
    [Test] procedure RespostaDeErro_StatusDeErroComCodigo;
    [Test] procedure QuedaDeConexao_StatusDeErroComClasse;
    [Test] procedure Ping_NaoAbreSpan;
    [Test] procedure Handshake_NaoAbreSpan;
    [Test] procedure Pipeline_UmSpanComTamanho;
    [Test] procedure Pipeline_ErroNoMeio_ContaOsErros;
    [Test] procedure Transacao_SpanMultiSemOEnvelope;
    [Test] procedure Transacao_ErroDentroDoExec_Conta;
    [Test] procedure PoolSemEspera_NaoAbreSpan;
    [Test] procedure PoolEsgotado_SpanDeEsperaComErro;
    [Test] procedure PoolEsperaADevolucao_SpanDeEsperaSemErro;
  end;

implementation

const
  CRLF = #13#10;

{ Helpers }

function TRedisSpanCollector.ExportSpans(const ASpans: TPcSpanDataArray;
  out AError: string): Boolean;
var
  I, LBase: Integer;
begin
  AError := '';
  LBase := Length(FSpans);
  SetLength(FSpans, LBase + Length(ASpans));
  for I := 0 to High(ASpans) do
    FSpans[LBase + I] := ASpans[I];
  Result := True;
end;

// Liga o tracer sem thread: os spans so' saem no FlushNow do teste.
procedure LigarTracing(const AExporter: IPcSpanExporter);
begin
  TPcTracing.Start(TPcTracingOptions.Default('redis-tests', '0'), AExporter, False);
end;

function Atributo(const ASpan: TPcSpanData; const AKey: string): string;
var
  I: Integer;
begin
  Result := '<ausente>';
  for I := 0 to High(ASpan.Attributes) do
    if ASpan.Attributes[I].Key = AKey then
    begin
      case ASpan.Attributes[I].ValueType of
        satInt: Result := IntToStr(ASpan.Attributes[I].IntValue);
        satBool: Result := BoolToStr(ASpan.Attributes[I].BoolValue, True);
      else
        Result := ASpan.Attributes[I].StringValue;
      end;
      Exit;
    end;
end;

function Conexao(const ARespostas: array of string): TRedisConnection;
begin
  Result := TRedisConnection.CreateOnStream(
    TRedisFakeServerStream.Create(ARespostas), RedisDefaultParams);
  Result.Open;
end;

function ParamsDoPool(AMaxSize, AAcquireMs: Integer): TRedisPoolParams;
begin
  Result := RedisDefaultPoolParams;
  Result.MaxSize := AMaxSize;
  Result.AcquireTimeoutMs := AAcquireMs;
  Result.IdleTimeoutMs := 0;
  Result.HealthCheckAfterIdleMs := -1;
end;

function Bytes(const AValues: array of Byte): TBytes;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, Length(AValues));
  for I := 0 to High(AValues) do
    Result[I] := AValues[I];
end;

{ TRedisTracingTextTests }

procedure TRedisTracingTextTests.Operacao_ComandoEmMaiusculas;
begin
  TAssert.AssertEquals('GET', RedisSpanOperation('get', ['k'], 0));
end;

procedure TRedisTracingTextTests.Operacao_ContainerJuntaOSubcomando;
begin
  TAssert.AssertEquals('CLIENT SETNAME', RedisSpanOperation('CLIENT', ['setname', 'app'], 0));
  TAssert.AssertEquals('XGROUP CREATE', RedisSpanOperation('xgroup', ['CREATE', 's', 'g', '$'], 0));
end;

procedure TRedisTracingTextTests.Operacao_SubcomandoEstranho_FicaSoOComando;
begin
  // O nome do span tem de ter cardinalidade baixa: texto livre no lugar do
  // subcomando nao entra.
  TAssert.AssertEquals('CONFIG', RedisSpanOperation('CONFIG', ['nao e subcomando'], 0));
  TAssert.AssertEquals('CONFIG', RedisSpanOperation('CONFIG', [], 0));
end;

procedure TRedisTracingTextTests.Texto_SoAChave;
begin
  TAssert.AssertEquals('GET sessao:42', RedisSpanQueryText('GET', ['sessao:42'], 0));
end;

procedure TRedisTracingTextTests.Texto_ValorViraInterrogacao;
begin
  TAssert.AssertEquals('SET sessao:42 ?',
    RedisSpanQueryText('SET', ['sessao:42', 'token-secreto', 'EX', 60], 0));
  TAssert.AssertEquals('HSET h ?', RedisSpanQueryText('HSET', ['h', 'campo', 'valor'], 0));
end;

procedure TRedisTracingTextTests.Texto_SemChave_NaoMostraArgumento;
begin
  TAssert.AssertEquals('AUTH ?', RedisSpanQueryText('AUTH', ['senha'], 0));
  TAssert.AssertEquals('HELLO ?', RedisSpanQueryText('HELLO', [3, 'AUTH', 'u', 'senha'], 0));
  TAssert.AssertEquals('ECHO ?', RedisSpanQueryText('ECHO', ['texto'], 0));
  TAssert.AssertEquals('PING', RedisSpanQueryText('PING', [], 0));
end;

procedure TRedisTracingTextTests.Texto_ContainerComChave;
begin
  TAssert.AssertEquals('XGROUP CREATE pedidos ?',
    RedisSpanQueryText('XGROUP', ['CREATE', 'pedidos', 'workers', '$'], 0));
  TAssert.AssertEquals('OBJECT ENCODING k', RedisSpanQueryText('OBJECT', ['ENCODING', 'k'], 0));
end;

procedure TRedisTracingTextTests.Texto_ContainerSemChave;
begin
  TAssert.AssertEquals('CONFIG SET ?', RedisSpanQueryText('CONFIG', ['SET', 'maxmemory', '1gb'], 0));
  TAssert.AssertEquals('ACL SETUSER ?',
    RedisSpanQueryText('ACL', ['SETUSER', 'bob', 'on', '>senha'], 0));
  TAssert.AssertEquals('SCRIPT FLUSH', RedisSpanQueryText('SCRIPT', ['FLUSH'], 0));
end;

procedure TRedisTracingTextTests.Texto_EvalPegaAChaveDepoisDoNumkeys;
begin
  TAssert.AssertEquals('EVALSHA lock:a ?',
    RedisSpanQueryText('EVALSHA', ['0123abcd', 1, 'lock:a', 'token'], 0));
  TAssert.AssertEquals('BLMPOP fila ?',
    RedisSpanQueryText('BLMPOP', [5, 2, 'fila', 'outra', 'LEFT'], 0));
  TAssert.AssertEquals('ZINTER a ?', RedisSpanQueryText('ZINTER', [2, 'a', 'b'], 0));
end;

procedure TRedisTracingTextTests.Texto_EvalSemChave;
begin
  TAssert.AssertEquals('EVAL ?', RedisSpanQueryText('EVAL', ['return 1', 0], 0));
end;

procedure TRedisTracingTextTests.Texto_XReadPegaAChaveDepoisDeStreams;
begin
  TAssert.AssertEquals('XREADGROUP pedidos ?', RedisSpanQueryText('XREADGROUP',
    ['GROUP', 'g', 'c', 'COUNT', 1, 'STREAMS', 'pedidos', '>'], 0));
  TAssert.AssertEquals('XREAD s ?', RedisSpanQueryText('XREAD', ['streams', 's', '0'], 0));
end;

procedure TRedisTracingTextTests.Texto_ChaveLonga_ECortada;
var
  LChave: string;
begin
  LChave := StringOfChar('a', 200);
  TAssert.AssertEquals('GET ' + StringOfChar('a', 128) + '...',
    RedisSpanQueryText('GET', [LChave], 0));
end;

procedure TRedisTracingTextTests.Texto_NomeDentroDoArray;
begin
  // A forma do ExecuteArgs: o nome e' o item 0 e os argumentos comecam no 1.
  TAssert.AssertEquals('CLIENT SETNAME ?',
    RedisSpanQueryText('CLIENT', ['CLIENT', 'SETNAME', 'app'], 1));
  TAssert.AssertEquals('CLIENT SETNAME',
    RedisSpanOperation('CLIENT', ['CLIENT', 'SETNAME', 'app'], 1));
end;

procedure TRedisTracingTextTests.Texto_ChaveBinaria_NaoLevanta;
var
  LTexto: string;
begin
  // Chave que nao e' UTF-8 valido (o Redis guarda bytes): o decode tolerante
  // da lib nao pode derrubar o comando por causa do span.
  LTexto := RedisSpanQueryText('GET', [Bytes([$FF, $FE, $41])], 0);
  TAssert.AssertEquals('GET ', Copy(LTexto, 1, 4));
end;

{ TRedisTracingSpanTests }

procedure TRedisTracingSpanTests.Desligado_NaoCriaSpan;
var
  LConn: TRedisConnection;
begin
  TAssert.AssertFalse('nenhum teste deixou o tracer ligado', TPcTracing.Enabled);
  TAssert.AssertTrue(RedisStartCommandSpan('GET', ['k'], 0, RedisDefaultParams, 0) = nil);
  TAssert.AssertTrue(RedisStartBatchSpan('PIPELINE', 2, RedisDefaultParams, 0) = nil);
  TAssert.AssertTrue(RedisStartPoolWaitSpan(1, RedisDefaultParams) = nil);
  LConn := Conexao(['$1' + CRLF + 'v' + CRLF]);
  try
    TAssert.AssertEquals('v', LConn.Execute('GET', ['k']).AsString);
  finally
    LConn.Free;
  end;
end;

procedure TRedisTracingSpanTests.Execute_AbreSpanClientComAtributos;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LSpan: TPcSpanData;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao(['+OK' + CRLF]);
    try
      LConn.Execute('set', ['sessao:42', 'token', 'EX', 60]);
    finally
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    LSpan := LColetor.Spans[0];
    TAssert.AssertEquals('SET', LSpan.Name);
    TAssert.AssertTrue('kind client', LSpan.Kind = skClient);
    TAssert.AssertTrue('sem erro', LSpan.Status = ssUnset);
    TAssert.AssertEquals('redis', Atributo(LSpan, 'db.system.name'));
    TAssert.AssertEquals('SET', Atributo(LSpan, 'db.operation.name'));
    TAssert.AssertEquals('SET sessao:42 ?', Atributo(LSpan, 'db.query.text'));
    TAssert.AssertEquals('0', Atributo(LSpan, 'db.namespace'));
    TAssert.AssertEquals('localhost', Atributo(LSpan, 'server.address'));
    TAssert.AssertEquals('6379', Atributo(LSpan, 'server.port'));
    TAssert.AssertEquals('', LSpan.ParentSpanId);
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.Execute_FilhoDoSpanCorrente;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LPai: IPcSpan;
  LTraceId, LSpanId: string;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao([':1' + CRLF]);
    try
      LPai := TPcTracing.StartSpan('GET /pedidos', skServer);
      LTraceId := LPai.TraceId;
      LSpanId := LPai.SpanId;
      LConn.Execute('INCR', ['contador']);
      TAssert.AssertTrue('o pai volta a ser o corrente', TPcTracing.Current = LPai);
      LPai.Finish;
      LPai := nil;
    finally
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(2, Length(LColetor.Spans));
    TAssert.AssertEquals('INCR', LColetor.Spans[0].Name);
    TAssert.AssertEquals(LTraceId, LColetor.Spans[0].TraceId);
    TAssert.AssertEquals(LSpanId, LColetor.Spans[0].ParentSpanId);
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.ExecuteArgs_UsaONomeDoArray;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao(['+OK' + CRLF]);
    try
      LConn.ExecuteArgs(['client', 'setname', 'app']);
    finally
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    TAssert.AssertEquals('CLIENT SETNAME', LColetor.Spans[0].Name);
    TAssert.AssertEquals('CLIENT SETNAME ?', Atributo(LColetor.Spans[0], 'db.query.text'));
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.RespostaDeErro_StatusDeErroComCodigo;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LLevantou: Boolean;
  LSpan: TPcSpanData;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao(['-WRONGTYPE Operation against a key holding the wrong kind of value' + CRLF]);
    try
      LLevantou := False;
      try
        LConn.Execute('INCR', ['lista']);
      except
        on E: ERedisReplyError do
          LLevantou := True;
      end;
      TAssert.AssertTrue('o Execute continua levantando', LLevantou);
      TAssert.AssertTrue('erro de servidor nao invalida a conexao', LConn.IsUsable);
    finally
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    LSpan := LColetor.Spans[0];
    TAssert.AssertTrue('status de erro', LSpan.Status = ssError);
    TAssert.AssertEquals('WRONGTYPE', Atributo(LSpan, 'error.type'));
    TAssert.AssertEquals('WRONGTYPE', Atributo(LSpan, 'db.response.status_code'));
    TAssert.AssertEquals('WRONGTYPE Operation against a key holding the wrong kind of value',
      LSpan.StatusMessage);
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.QuedaDeConexao_StatusDeErroComClasse;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LLevantou: Boolean;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    // Roteiro vazio: a leitura da resposta encontra EOF.
    LConn := Conexao([]);
    try
      LLevantou := False;
      try
        LConn.Execute('GET', ['k']);
      except
        on E: ERedisConnectionLost do
          LLevantou := True;
      end;
      TAssert.AssertTrue('a queda chega ao chamador', LLevantou);
    finally
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    TAssert.AssertTrue('status de erro', LColetor.Spans[0].Status = ssError);
    TAssert.AssertEquals('ERedisConnectionLost', Atributo(LColetor.Spans[0], 'error.type'));
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.Ping_NaoAbreSpan;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao(['+PONG' + CRLF]);
    try
      TAssert.AssertTrue(LConn.Ping);
    finally
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals('o health check do pool nao entra na trace', 0, Length(LColetor.Spans));
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.Handshake_NaoAbreSpan;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LParams := RedisDefaultParams;
    LParams.Password := 'senha';
    LParams.ClientName := 'app';
    LParams.Database := 2;
    LConn := TRedisConnection.CreateOnStream(
      TRedisFakeServerStream.Create(['+OK' + CRLF, '+OK' + CRLF, '+OK' + CRLF]), LParams);
    try
      LConn.Open;
    finally
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals('AUTH, CLIENT SETNAME e SELECT do handshake', 0,
      Length(LColetor.Spans));
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.Pipeline_UmSpanComTamanho;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LLote: TRedisPipeline;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao(['+OK' + CRLF + ':1' + CRLF + '$1' + CRLF + 'v' + CRLF]);
    LLote := TRedisPipeline.Create;
    try
      LLote.Queue('SET', ['k', 'v']);
      LLote.Queue('INCR', ['n']);
      LLote.Queue('GET', ['k']);
      TAssert.AssertEquals('SET', LLote.FirstCommand);
      LConn.ExecutePipeline(LLote);
    finally
      LLote.Free;
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    TAssert.AssertEquals('PIPELINE', LColetor.Spans[0].Name);
    TAssert.AssertEquals('3', Atributo(LColetor.Spans[0], 'db.operation.batch.size'));
    TAssert.AssertTrue('sem erro', LColetor.Spans[0].Status = ssUnset);
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.Pipeline_ErroNoMeio_ContaOsErros;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LLote: TRedisPipeline;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao(['+OK' + CRLF + '-ERR value is not an integer' + CRLF + ':1' + CRLF]);
    LLote := TRedisPipeline.Create;
    try
      LLote.Queue('SET', ['k', 'v']);
      LLote.Queue('INCR', ['k']);
      LLote.Queue('DEL', ['k']);
      LConn.ExecutePipeline(LLote);
    finally
      LLote.Free;
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    TAssert.AssertTrue('status de erro', LColetor.Spans[0].Status = ssError);
    TAssert.AssertEquals('1', Atributo(LColetor.Spans[0], 'pascalredis.batch.error_count'));
    TAssert.AssertEquals('ERR', Atributo(LColetor.Spans[0], 'error.type'));
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.Transacao_SpanMultiSemOEnvelope;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LLote: TRedisPipeline;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LConn := Conexao(['+OK' + CRLF + '+QUEUED' + CRLF + '+QUEUED' + CRLF +
      '*2' + CRLF + '+OK' + CRLF + ':1' + CRLF]);
    LLote := TRedisPipeline.Create;
    try
      LLote.QueueArgs(['multi']);
      LLote.Queue('SET', ['k', 'v']);
      LLote.Queue('INCR', ['n']);
      LLote.Queue('EXEC');
      LConn.ExecutePipeline(LLote);
    finally
      LLote.Free;
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    TAssert.AssertEquals('MULTI', LColetor.Spans[0].Name);
    TAssert.AssertEquals('sem contar MULTI e EXEC', '2',
      Atributo(LColetor.Spans[0], 'db.operation.batch.size'));
    TAssert.AssertTrue('sem erro', LColetor.Spans[0].Status = ssUnset);
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.Transacao_ErroDentroDoExec_Conta;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LConn: TRedisConnection;
  LLote: TRedisPipeline;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    // MULTI/EXEC nao e' rollback: o erro de execucao vem DENTRO do array do
    // EXEC, e o span tem de conta-lo mesmo assim.
    LConn := Conexao(['+OK' + CRLF + '+QUEUED' + CRLF + '+QUEUED' + CRLF +
      '*2' + CRLF + '+OK' + CRLF + '-WRONGTYPE Operation against a key' + CRLF]);
    LLote := TRedisPipeline.Create;
    try
      LLote.Queue('MULTI');
      LLote.Queue('SET', ['k', 'v']);
      LLote.Queue('LPUSH', ['k', 'x']);
      LLote.Queue('EXEC');
      LConn.ExecutePipeline(LLote);
    finally
      LLote.Free;
      LConn.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    TAssert.AssertTrue('status de erro', LColetor.Spans[0].Status = ssError);
    TAssert.AssertEquals('1', Atributo(LColetor.Spans[0], 'pascalredis.batch.error_count'));
    TAssert.AssertEquals('WRONGTYPE', Atributo(LColetor.Spans[0], 'error.type'));
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.PoolSemEspera_NaoAbreSpan;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LPool := TRedisFakePool.Create(RedisDefaultParams, ParamsDoPool(2, 50));
    try
      LConn := LPool.Acquire;
      LPool.Release(LConn);
    finally
      LPool.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(0, Length(LColetor.Spans));
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.PoolEsgotado_SpanDeEsperaComErro;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LPool: TRedisFakePool;
  LConn: TRedisConnection;
  LLevantou: Boolean;
  LSpan: TPcSpanData;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LPool := TRedisFakePool.Create(RedisDefaultParams, ParamsDoPool(1, 50));
    try
      LConn := LPool.Acquire;
      LLevantou := False;
      try
        LPool.Acquire;
      except
        on E: ERedisPoolExhausted do
          LLevantou := True;
      end;
      TAssert.AssertTrue(LLevantou);
      LPool.Release(LConn);
    finally
      LPool.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    LSpan := LColetor.Spans[0];
    TAssert.AssertEquals('pool wait', LSpan.Name);
    TAssert.AssertTrue('kind internal', LSpan.Kind = skInternal);
    TAssert.AssertTrue('status de erro', LSpan.Status = ssError);
    TAssert.AssertEquals('ERedisPoolExhausted', Atributo(LSpan, 'error.type'));
    TAssert.AssertEquals('1', Atributo(LSpan, 'pascalredis.pool.max_size'));
    TAssert.AssertTrue('esperou ao menos uma vez',
      StrToIntDef(Atributo(LSpan, 'pascalredis.pool.wait_attempts'), 0) >= 1);
  finally
    TPcTracing.Shutdown;
  end;
end;

procedure TRedisTracingSpanTests.PoolEsperaADevolucao_SpanDeEsperaSemErro;
var
  LColetor: TRedisSpanCollector;
  LRef: IPcSpanExporter;
  LPool: TRedisFakePool;
  LPrimeira, LSegunda: TRedisConnection;
  LThread: TRedisReleaserThread;
begin
  LColetor := TRedisSpanCollector.Create;
  LRef := LColetor;
  LigarTracing(LRef);
  try
    LPool := TRedisFakePool.Create(RedisDefaultParams, ParamsDoPool(1, 1000));
    try
      LPrimeira := LPool.Acquire;
      LThread := TRedisReleaserThread.Create(LPool, LPrimeira, 50);
      try
        LThread.Start;
        LSegunda := LPool.Acquire;
        LPool.Release(LSegunda);
      finally
        LThread.WaitFor;
        LThread.Free;
      end;
    finally
      LPool.Free;
    end;
    TPcTracing.FlushNow;
    TAssert.AssertEquals(1, Length(LColetor.Spans));
    TAssert.AssertEquals('pool wait', LColetor.Spans[0].Name);
    TAssert.AssertTrue('conseguiu a conexao: sem erro', LColetor.Spans[0].Status = ssUnset);
  finally
    TPcTracing.Shutdown;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisTracingTextTests);
  TDUnitX.RegisterTestFixture(TRedisTracingSpanTests);

end.
