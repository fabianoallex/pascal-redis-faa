unit Redis.IntegrationTests;

{ Testes de integracao (FPCUnit). Mesma cobertura do tests\Integration\Redis.IntegrationTests.pas
  (DUnitX/Delphi) — as duas suites sao mantidas linha a linha equivalentes, entao
  toda mudanca aqui vai para la' na mesma sessao.

  PRECISAM DE UM REDIS DE VERDADE em localhost:6379 (docker/docker-compose.yml).
  Sem ele, tudo aqui falha por ERedisConnectionLost — e' o esperado, nao ha'
  como testar contra o servidor real sem o servidor real.

  O que so' da' para verificar aqui, e nao nas suites unitarias:

  - **Read timeout de verdade.** Um BLPOP mais longo que o timeout da conexao
    prova que o SO_RCVTIMEO foi mesmo aplicado no socket (o servidor falso nao
    tem socket, logo nao tem como estourar timeout de socket).
  - **Conexao contaminada.** Depois do timeout, a resposta atrasada do BLPOP
    ainda esta' a caminho. Este e' o teste que o roadmap pede desde o M0: a
    conexao seguinte NAO pode receber essa resposta no lugar da dela.
  - **Servidor derrubando conexao ociosa** (CLIENT KILL), que e' o motivo de o
    pool conferir a saude antes de emprestar.
  - **Concorrencia real**: varias threads emprestando e devolvendo do mesmo
    pool, com sockets de verdade.

  As chaves criadas usam o prefixo 'pascal-redis-faa:it:' e sao apagadas ao
  fim de cada teste — nada de FLUSHDB, que apagaria dados de quem estiver
  usando o mesmo servidor. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes, SyncObjs,
  Redis.Types, Redis.Threading, Redis.Connection, Redis.Pool, Redis.Client,
  Redis.Commands, Redis.Commands.Keys, Redis.Commands.Strings,
  Redis.Commands.Hashes, Redis.Commands.Lists, Redis.Commands.Sets,
  Redis.Commands.ZSets, Redis.Commands.Scripting, Redis.Commands.PubSub,
  Redis.Transaction, Redis.PubSub;

type
  { Roda comandos pelo pool numa thread propria. Guarda o erro em vez de
    deixa-lo escapar: excecao em thread de teste some sem deixar rastro, e o
    que interessa e' o teste falhar com a mensagem certa. }
  TRedisPoolWorkerThread = class(TThread)
  private
    FPool: TRedisPool;
    FIndex: Integer;
    FRounds: Integer;
    FOk: Integer;
    FErro: string;
  protected
    procedure Execute; override;
  public
    constructor Create(APool: TRedisPool; AIndex, ARounds: Integer);
    property Ok: Integer read FOk;
    property Erro: string read FErro;
  end;


  { Coleta o que o assinante entregou. Os callbacks rodam na thread de leitura
    do assinante, entao tudo aqui passa por lock — e a espera e' por condicao,
    nunca por Sleep fixo, que renderia teste lento e instavel. }
  TRedisMensagensRecebidas = class
  private
    FLock: TCriticalSection;
    FItens: TStringList;
    FUltimoPayload: TBytes;
    FReconexoes: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Mensagem(ASender: TObject; const AMessage: TRedisPubSubMessage);
    procedure Reconectou(ASender: TObject);
    /// Espera chegarem ACount mensagens; False se o prazo estourar.
    function Espera(ACount, ATimeoutMs: Integer): Boolean;
    function EsperaReconexao(ATimeoutMs: Integer): Boolean;
    /// As mensagens na ordem, separadas por '|'.
    function Texto: string;
    function UltimoPayload: TBytes;
    function Total: Integer;
  end;

  TRedisConnectionIntegrationTests = class(TTestCase)
  published
    procedure Conecta_ExecutaEFecha;
    procedure Binario_EAcentuacao_SobrevivemAoRoundTrip;
    procedure Pipeline_ContraServidorReal;
    procedure Resp3_HandshakeCompleto;
    procedure BancoInexistente_FalhaNoHandshake;
    procedure ReadTimeout_LevantaERedisTimeoutEInvalida;
    procedure ServidorDerrubaAConexao_ViraConnectionLost;
  end;

  TRedisPoolIntegrationTests = class(TTestCase)
  published
    procedure Acquire_ReusaAMesmaConexao;
    procedure ConexaoQueSofreuTimeout_NaoContaminaAProxima;
    procedure ConexaoDerrubadaPeloServidor_ETrocadaNoHealthCheck;
    procedure VariasThreads_CompartilhamOPool;
  end;

  TRedisClientIntegrationTests = class(TTestCase)
  published
    procedure Cliente_PorPool_ExecutaEDevolveAConexao;
    procedure Cliente_FamiliasContraServidorReal;
    procedure Cliente_PipelinePelaFachada;
    procedure Cliente_ConexaoDedicada_MantemOEstadoDaConexao;
  end;

  TRedisKeysIntegrationTests = class(TTestCase)
  published
    procedure Expiracao_TtlPersistETtlDeChaveAusente;
    procedure TipoRenomeacaoECopia;
    procedure Scan_VarreTodasAsChavesDoPrefixo;
  end;

  TRedisStringsIntegrationTests = class(TTestCase)
  published
    procedure SetGet_ComBinarioEAcentuacao;
    procedure SetWithOptions_NxNaoSobrescreve_EGetDevolveOAnterior;
    procedure SetSemKeepTtl_ApagaOPrazo_ComKeepTtl_Preserva;
    procedure ContadoresInteiroEFlutuante;
    procedure MGet_ChaveAusenteVemComoNulo;
  end;

  TRedisHashesIntegrationTests = class(TTestCase)
  published
    procedure HashCompleto_SetGetAllScanEDel;
    procedure HGetAll_MesmaFormaEmResp2EResp3;
  end;

  TRedisListsIntegrationTests = class(TTestCase)
  published
    procedure Lista_PushPopRangeEMove;
    procedure BLPop_ValorJaEnfileirado_VoltaNaHora;
    procedure BLPop_PrazoMaiorQueOReceiveTimeout_NaoEstouraOSocket;
  end;

  TRedisSetsIntegrationTests = class(TTestCase)
  published
    procedure Conjunto_AddPertinenciaEOperacoes;
    procedure SScan_VarreOConjuntoInteiro;
  end;

  TRedisZSetsIntegrationTests = class(TTestCase)
  published
    procedure Ranking_AddRangeEIncr;
    procedure ZRangeWithScores_MesmoResultadoEmResp2EResp3;
    procedure ZRangeByScore_FaixaAbertaELimite;
    procedure ZPopMin_EsvaziaDoMenorParaOMaior;
  end;

  TRedisTransactionIntegrationTests = class(TTestCase)
  published
    procedure Transacao_RodaOBlocoInteiro;
    procedure Watch_AbortaQuandoOutraConexaoMexeNaChave;
    procedure Watch_NaoAbortaQuandoNinguemMexe;
    procedure ErroNoMeioDoBloco_NaoDesfazOsOutros;
    procedure Transacao_DevolveAConexaoAoPool;
    procedure WatchPendente_NaoContaminaAProximaTransacao;
  end;


  TRedisPubSubIntegrationTests = class(TTestCase)
  published
    procedure Publica_EAssinanteRecebe;
    procedure PublishSemAssinante_DevolveZeroESePerde;
    procedure Padrao_CasaVariosCanais;
    procedure PubSubChannels_EnxergaAAssinatura;
    procedure Unsubscribe_ParaDeReceber;
    procedure PayloadBinario_SobreviveAoRoundTrip;
    procedure VariosAssinantes_TodosRecebem;
    procedure Resp2_ComandoComumComAssinatura_Levanta;
    procedure Resp3_ConexaoContinuaUtilizavel;
    procedure ConexaoDerrubada_ReconectaERefazAsAssinaturas;
  end;

  TRedisScriptingIntegrationTests = class(TTestCase)
  published
    procedure Eval_ExecutaLuaEConverteOsTipos;
    procedure ScriptLoad_OShaDoServidorBateComOLocal;
    procedure Run_SegundaChamadaUsaOCache;
    procedure Run_AposScriptFlush_SeRecuperaSozinho;
    procedure LockDistribuido_SoQuemTemOTokenLibera;
  end;

implementation

const
  PREFIXO = 'pascal-redis-faa:it:';

{ Helpers compartilhados }

function ParamsDeTeste: TRedisParams;
begin
  Result := RedisDefaultParams;
  Result.Host := 'localhost';
  Result.Port := REDIS_DEFAULT_PORT;
  Result.ClientName := 'pascal-redis-faa-it';
end;

function Chave(const ASufixo: string): string;
begin
  Result := PREFIXO + ASufixo;
end;

function MakeBytes(const AValues: array of Byte): TBytes;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, Length(AValues));
  for I := 0 to High(AValues) do
    Result[I] := AValues[I];
end;

function Hex(const ABytes: TBytes): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(ABytes) do
    Result := Result + IntToHex(ABytes[I], 2);
end;

// Conexao aberta com os parametros de teste. Quem chama e' dono.
function AbreConexao: TRedisConnection;
begin
  Result := TRedisConnection.Create(ParamsDeTeste);
  try
    Result.Open;
  except
    Result.Free;
    raise;
  end;
end;

// Apaga as chaves do teste sem tocar no resto do banco.
procedure Limpa(AConn: TRedisConnection; const ASufixos: array of string);
var
  LArgs: TRedisArgs;
  I: Integer;
begin
  SetLength(LArgs, Length(ASufixos) + 1);
  LArgs[0] := 'DEL';
  for I := 0 to High(ASufixos) do
    LArgs[I + 1] := Chave(ASufixos[I]);
  AConn.ExecuteArgs(LArgs);
end;

{ TRedisPoolWorkerThread }

constructor TRedisPoolWorkerThread.Create(APool: TRedisPool;
  AIndex, ARounds: Integer);
begin
  inherited Create(True);
  FreeOnTerminate := False;
  FPool := APool;
  FIndex := AIndex;
  FRounds := ARounds;
end;

procedure TRedisPoolWorkerThread.Execute;
var
  I: Integer;
  LConn: TRedisConnection;
  LChave, LValor: string;
begin
  try
    for I := 1 to FRounds do
    begin
      LChave := Chave('thread' + IntToStr(FIndex));
      LValor := IntToStr(FIndex) + '-' + IntToStr(I);
      LConn := FPool.Acquire;
      try
        LConn.Execute('SET', [LChave, LValor]);
        if LConn.Execute('GET', [LChave]).AsString <> LValor then
        begin
          FErro := 'valor trocado na thread ' + IntToStr(FIndex);
          Exit;
        end;
        Inc(FOk);
      finally
        FPool.Release(LConn);
      end;
    end;
  except
    on E: Exception do
      FErro := E.ClassName + ': ' + E.Message;
  end;
end;

{ TRedisConnectionIntegrationTests }

procedure TRedisConnectionIntegrationTests.Conecta_ExecutaEFecha;
var
  LConn: TRedisConnection;
begin
  LConn := AbreConexao;
  try
    TAssert.AssertTrue('devia abrir', LConn.IsOpen);
    TAssert.AssertTrue('PING', LConn.Ping);

    LConn.Execute('SET', [Chave('str'), 'valor']);
    TAssert.AssertEquals('valor', LConn.Execute('GET', [Chave('str')]).AsString);
    TAssert.AssertEquals(Int64(1), LConn.Execute('DEL', [Chave('str')]).AsInteger);
    TAssert.AssertTrue('chave ausente e nulo',
      LConn.Execute('GET', [Chave('str')]).IsNull);

    // O nome anunciado no handshake tem de estar la' do lado do servidor.
    TAssert.AssertEquals('pascal-redis-faa-it',
      LConn.Execute('CLIENT', ['GETNAME']).AsString);

    LConn.Close;
    TAssert.AssertFalse('devia fechar', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.Binario_EAcentuacao_SobrevivemAoRoundTrip;
var
  LConn: TRedisConnection;
  LOriginal: TBytes;
  LTexto: string;
begin
  LConn := AbreConexao;
  try
    // CRLF e zero no meio do valor: o que quebraria um comando inline.
    LOriginal := MakeBytes([65, 13, 10, 66, 0, 255, 200]);
    LConn.Execute('SET', [Chave('bin'), LOriginal]);
    TAssert.AssertEquals(Hex(LOriginal),
      Hex(LConn.Execute('GET', [Chave('bin')]).AsBytes));

    LTexto := 'ação e coração';
    LConn.Execute('SET', [Chave('utf8'), LTexto]);
    TAssert.AssertEquals(LTexto, LConn.Execute('GET', [Chave('utf8')]).AsString);

    Limpa(LConn, ['bin', 'utf8']);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.Pipeline_ContraServidorReal;
var
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
begin
  LConn := AbreConexao;
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('DEL', [Chave('lista')]);
    LPipe.Queue('RPUSH', [Chave('lista'), 'a']);
    LPipe.Queue('RPUSH', [Chave('lista'), 'b']);
    LPipe.Queue('LRANGE', [Chave('lista'), 0, -1]);
    LReplies := LConn.ExecutePipeline(LPipe);

    TAssert.AssertEquals(4, Length(LReplies));
    TAssert.AssertEquals(Int64(2), LReplies[2].AsInteger);
    TAssert.AssertEquals(2, LReplies[3].Count);
    TAssert.AssertEquals('a', LReplies[3][0].AsString);
    TAssert.AssertEquals('b', LReplies[3][1].AsString);
    // O lote nao pode deixar resto no buffer: e' a diferenca entre pipeline e
    // conexao suja.
    TAssert.AssertFalse('nao pode sujar a conexao', LConn.IsDirty);
    TAssert.AssertTrue('e ela continua util', LConn.IsUsable);

    Limpa(LConn, ['lista']);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.Resp3_HandshakeCompleto;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LMapa: IRedisReply;
begin
  LParams := ParamsDeTeste;
  LParams.Protocol := rpRESP3;
  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;
    TAssert.AssertTrue('devia negociar RESP3',
      LConn.NegotiatedProtocol = rpRESP3);
    TAssert.AssertTrue('o HELLO traz a versao', LConn.ServerVersion <> '');
    TAssert.AssertTrue('e o id da conexao', LConn.ServerId > 0);

    // O mapa achatado do M1 e' o que faz HGETALL ter a MESMA forma nos dois
    // protocolos: em RESP3 o kind muda, a contagem e o ValueByKey nao.
    LConn.Execute('DEL', [Chave('hash')]);
    LConn.Execute('HSET', [Chave('hash'), 'c1', 'v1', 'c2', 'v2']);
    LMapa := LConn.Execute('HGETALL', [Chave('hash')]);
    TAssert.AssertTrue('RESP3 devolve mapa', LMapa.Kind = rkMap);
    TAssert.AssertEquals('contagem achatada', 4, LMapa.Count);
    TAssert.AssertEquals('v1', LMapa.ValueByKey('c1').AsString);

    Limpa(LConn, ['hash']);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.BancoInexistente_FalhaNoHandshake;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LClasse: string;
begin
  // O SELECT do handshake tem de ser conferido: sem isso a conexao subiria
  // "aberta" no banco 0 e a app gravaria no lugar errado, calada.
  LParams := ParamsDeTeste;
  LParams.Database := 99;  // o padrao do Redis e' 16 bancos (0..15)
  LConn := TRedisConnection.Create(LParams);
  try
    LClasse := '';
    try
      LConn.Open;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisReplyError', LClasse);
    TAssert.AssertFalse('nao pode ficar aberta', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.ReadTimeout_LevantaERedisTimeoutEInvalida;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LClasse: string;
  LInicio: UInt64;
  LDecorridoMs: UInt64;
begin
  // BLPOP numa chave que nao existe fica esperando ate' o timeout DELE (2 s).
  // Como o timeout da CONEXAO e' menor (300 ms), quem estoura primeiro e' o
  // socket. Sem SO_RCVTIMEO no transporte, esta thread ficaria 2 s parada — e,
  // num servidor que emudecesse de vez, ficaria parada para sempre.
  LParams := ParamsDeTeste;
  LParams.ReceiveTimeoutMs := 300;
  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;
    LInicio := RedisTickMs;
    LClasse := '';
    try
      LConn.Execute('BLPOP', [Chave('fila-que-nao-existe'), 2]);
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    LDecorridoMs := RedisTickMs - LInicio;

    TAssert.AssertEquals('ERedisTimeout', LClasse);
    TAssert.AssertTrue('devia desistir bem antes dos 2 s do BLPOP: ' +
      IntToStr(LDecorridoMs) + ' ms', LDecorridoMs < 1500);
    // Timeout invalida: pode haver resposta a caminho, e reciclar contaminaria
    // o proximo comando.
    TAssert.AssertTrue('devia estar invalidada', LConn.IsBroken);
    TAssert.AssertFalse('e fora de uso', LConn.IsUsable);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.ServidorDerrubaAConexao_ViraConnectionLost;
var
  LVitima, LCarrasco: TRedisConnection;
  LId: Int64;
  LClasse: string;
begin
  // CLIENT KILL e' o que acontece de verdade num failover, num restart ou
  // quando o operador limpa conexoes penduradas. O cliente nao e' avisado: so'
  // descobre no proximo comando.
  LVitima := AbreConexao;
  LCarrasco := nil;
  try
    LId := LVitima.Execute('CLIENT', ['ID']).AsInteger;
    LCarrasco := AbreConexao;
    LCarrasco.Execute('CLIENT', ['KILL', 'ID', LId]);

    LClasse := '';
    try
      LVitima.Execute('PING');
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisConnectionLost', LClasse);
    TAssert.AssertTrue('devia estar invalidada', LVitima.IsBroken);
  finally
    LCarrasco.Free;
    LVitima.Free;
  end;
end;

{ TRedisPoolIntegrationTests }

procedure TRedisPoolIntegrationTests.Acquire_ReusaAMesmaConexao;
var
  LPool: TRedisPool;
  LConn: TRedisConnection;
  LPoolParams: TRedisPoolParams;
  I: Integer;
begin
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.HealthCheckAfterIdleMs := -1;  // sem PING: mede so' o reuso
  LPool := TRedisPool.Create(ParamsDeTeste, LPoolParams);
  try
    for I := 1 to 5 do
    begin
      LConn := LPool.Acquire;
      try
        TAssert.AssertTrue('PING na volta ' + IntToStr(I), LConn.Ping);
      finally
        LPool.Release(LConn);
      end;
    end;
    TAssert.AssertEquals('cinco idas, um socket', Int64(1), LPool.CreatedCount);
    TAssert.AssertEquals('nada descartado', Int64(0), LPool.DiscardedCount);
    TAssert.AssertEquals(1, LPool.IdleCount);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolIntegrationTests.ConexaoQueSofreuTimeout_NaoContaminaAProxima;
var
  LPool: TRedisPool;
  LParams: TRedisParams;
  LPoolParams: TRedisPoolParams;
  LConn: TRedisConnection;
  LReply: IRedisReply;
  LEstourou: Boolean;
begin
  // ESTE e' o teste de conexao-suja que o projeto persegue desde o inicio.
  //
  // O BLPOP sofre timeout do lado do cliente, mas o servidor NAO sabe disso:
  // ele ainda vai responder (na hora em que o BLPOP dele vencer). Se o pool
  // reciclasse essa conexao, o GET seguinte leria a resposta atrasada do BLPOP
  // — e a aplicacao receberia, calada, o valor errado. Com MaxSize = 1 o pool
  // e' obrigado a reusar a unica conexao que tem, entao se ele nao a destruir
  // o teste pega o erro na hora.
  LParams := ParamsDeTeste;
  LParams.ReceiveTimeoutMs := 300;
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 1;
  LPoolParams.HealthCheckAfterIdleMs := -1;  // sem rede de seguranca: o
                                             // descarte tem de vir do timeout
  LPool := TRedisPool.Create(LParams, LPoolParams);
  try
    LConn := LPool.Acquire;
    LEstourou := False;
    try
      try
        LConn.Execute('BLPOP', [Chave('fila-que-nao-existe'), 2]);
      except
        on E: ERedisTimeout do
          LEstourou := True;
      end;
    finally
      LPool.Release(LConn);
    end;
    TAssert.AssertTrue('o BLPOP devia ter estourado o timeout', LEstourou);
    TAssert.AssertEquals('a conexao com resposta a caminho foi descartada',
      Int64(1), LPool.DiscardedCount);
    TAssert.AssertEquals('e nao ficou ociosa', 0, LPool.IdleCount);

    // Conexao nova, do zero: o valor tem de ser o que este comando gravou, e
    // nao a resposta atrasada do BLPOP anterior.
    LConn := LPool.Acquire;
    try
      LConn.Execute('SET', [Chave('depois'), 'valor-novo']);
      LReply := LConn.Execute('GET', [Chave('depois')]);
      TAssert.AssertTrue('a resposta tem de ser uma bulk string',
        LReply.Kind = rkBulkString);
      TAssert.AssertEquals('valor-novo', LReply.AsString);
      TAssert.AssertFalse('e a conexao nova esta limpa', LConn.IsDirty);
      Limpa(LConn, ['depois']);
    finally
      LPool.Release(LConn);
    end;
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolIntegrationTests.ConexaoDerrubadaPeloServidor_ETrocadaNoHealthCheck;
var
  LPool: TRedisPool;
  LPoolParams: TRedisPoolParams;
  LConn, LCarrasco: TRedisConnection;
  LId: Int64;
begin
  // Conexao ociosa derrubada pelo servidor e' a falha classica de pool: ela
  // parece boa (o cliente nao foi avisado) e so' quebra no comando do usuario.
  // Com HealthCheckAfterIdleMs = 0, o pool confere antes de emprestar e troca.
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.HealthCheckAfterIdleMs := 0;
  LPool := TRedisPool.Create(ParamsDeTeste, LPoolParams);
  LCarrasco := nil;
  try
    LConn := LPool.Acquire;
    LId := LConn.Execute('CLIENT', ['ID']).AsInteger;
    LPool.Release(LConn);

    LCarrasco := AbreConexao;
    LCarrasco.Execute('CLIENT', ['KILL', 'ID', LId]);

    // O usuario nao pode ver nada disso: o Acquire devolve uma conexao boa.
    LConn := LPool.Acquire;
    try
      TAssert.AssertTrue('a conexao emprestada tem de funcionar', LConn.Ping);
      TAssert.AssertEquals('a morta foi descartada', Int64(1),
        LPool.DiscardedCount);
      TAssert.AssertEquals('e outra foi aberta no lugar', Int64(2),
        LPool.CreatedCount);
      TAssert.AssertEquals('sem inflar o pool', 1, LPool.TotalCount);
    finally
      LPool.Release(LConn);
    end;
  finally
    LCarrasco.Free;
    LPool.Free;
  end;
end;

procedure TRedisPoolIntegrationTests.VariasThreads_CompartilhamOPool;
const
  THREADS = 4;
  RODADAS = 25;
var
  LPool: TRedisPool;
  LPoolParams: TRedisPoolParams;
  LThreads: array[0..THREADS - 1] of TRedisPoolWorkerThread;
  LConn: TRedisConnection;
  LSufixos: array[0..THREADS - 1] of string;
  I: Integer;
  LTotalOk: Integer;
begin
  // Duas coisas de uma vez: que o pool aguenta concorrencia de verdade (lock,
  // espera, devolucao) e que o teto e' teto — com MaxSize menor que o numero
  // de threads, algumas TEM de esperar pela devolucao das outras.
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 2;
  LPoolParams.AcquireTimeoutMs := 5000;
  LPool := TRedisPool.Create(ParamsDeTeste, LPoolParams);
  try
    for I := 0 to THREADS - 1 do
      LThreads[I] := TRedisPoolWorkerThread.Create(LPool, I, RODADAS);
    try
      for I := 0 to THREADS - 1 do
        LThreads[I].Start;
      for I := 0 to THREADS - 1 do
        LThreads[I].WaitFor;

      LTotalOk := 0;
      for I := 0 to THREADS - 1 do
      begin
        TAssert.AssertEquals('thread ' + IntToStr(I), '', LThreads[I].Erro);
        Inc(LTotalOk, LThreads[I].Ok);
      end;
      TAssert.AssertEquals('todas as rodadas completaram',
        THREADS * RODADAS, LTotalOk);
      TAssert.AssertTrue('o teto foi respeitado: ' + IntToStr(LPool.TotalCount),
        LPool.TotalCount <= LPoolParams.MaxSize);
      TAssert.AssertEquals('ninguem ficou emprestada', 0, LPool.InUseCount);
    finally
      for I := 0 to THREADS - 1 do
        LThreads[I].Free;
    end;

    LConn := LPool.Acquire;
    try
      for I := 0 to THREADS - 1 do
        LSufixos[I] := 'thread' + IntToStr(I);
      Limpa(LConn, LSufixos);
    finally
      LPool.Release(LConn);
    end;
  finally
    LPool.Free;
  end;
end;

{ --- M4: fachada e familias de comandos --- }

// Cliente com pool proprio contra o servidor de teste. Quem chama e' dono.
function NovoCliente: TRedisClient;
begin
  Result := TRedisClient.Create(ParamsDeTeste);
end;

// Cliente falando RESP3 (HELLO 3). Exige Redis 6+.
function NovoClienteResp3: TRedisClient;
var
  LParams: TRedisParams;
begin
  LParams := ParamsDeTeste;
  LParams.Protocol := rpRESP3;
  Result := TRedisClient.Create(LParams);
end;

// Apaga as chaves do teste pela fachada, sem tocar no resto do banco.
procedure LimpaChaves(AClient: TRedisClient; const ASufixos: array of string);
var
  LArgs: TRedisArgs;
  I: Integer;
begin
  if Length(ASufixos) = 0 then
    Exit;
  SetLength(LArgs, Length(ASufixos));
  for I := 0 to High(ASufixos) do
    LArgs[I] := Chave(ASufixos[I]);
  AClient.Keys.DelMany(LArgs);
end;

{ TRedisClientIntegrationTests }

procedure TRedisClientIntegrationTests.Cliente_PorPool_ExecutaEDevolveAConexao;
var
  LClient: TRedisClient;
begin
  LClient := NovoCliente;
  try
    LClient.Strings.SetValue(Chave('cli'), 'valor');
    TAssert.AssertEquals('valor', LClient.Strings.GetString(Chave('cli')));

    // Cada comando pega uma conexao e a DEVOLVE antes de retornar. Se o
    // finally do Execute falhasse, o pool esvaziaria comando a comando ate'
    // estourar o MaxSize — e o sintoma so' apareceria em producao, sob carga.
    TAssert.AssertEquals(0, LClient.Pool.InUseCount);
    TAssert.AssertEquals(1, LClient.Pool.IdleCount);

    LimpaChaves(LClient, ['cli']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientIntegrationTests.Cliente_FamiliasContraServidorReal;
var
  LClient: TRedisClient;
begin
  LClient := NovoCliente;
  try
    LClient.Strings.SetValue(Chave('fam:str'), 'v');
    LClient.Hashes.HSet(Chave('fam:hash'), 'campo', 'v');
    LClient.Lists.RPush(Chave('fam:list'), 'v');
    LClient.Sets.SAdd(Chave('fam:set'), 'v');
    LClient.ZSets.ZAdd(Chave('fam:zset'), 1, 'v');

    // O TYPE de cada chave prova que a familia certa criou a estrutura certa —
    // um argumento fora de ordem daria WRONGTYPE ou tipo trocado.
    TAssert.AssertEquals('string', LClient.Keys.KeyType(Chave('fam:str')));
    TAssert.AssertEquals('hash', LClient.Keys.KeyType(Chave('fam:hash')));
    TAssert.AssertEquals('list', LClient.Keys.KeyType(Chave('fam:list')));
    TAssert.AssertEquals('set', LClient.Keys.KeyType(Chave('fam:set')));
    TAssert.AssertEquals('zset', LClient.Keys.KeyType(Chave('fam:zset')));

    LimpaChaves(LClient,
      ['fam:str', 'fam:hash', 'fam:list', 'fam:set', 'fam:zset']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientIntegrationTests.Cliente_PipelinePelaFachada;
var
  LClient: TRedisClient;
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
begin
  LClient := NovoCliente;
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('DEL', [Chave('pipe')]);
    LPipe.Queue('RPUSH', [Chave('pipe'), 'a']);
    LPipe.Queue('LLEN', [Chave('pipe')]);
    LReplies := LClient.ExecutePipeline(LPipe);

    TAssert.AssertEquals(3, Length(LReplies));
    TAssert.AssertEquals(Int64(1), LReplies[2].AsInteger);
    // O lote inteiro saiu por UMA conexao, que voltou limpa ao pool.
    TAssert.AssertEquals(0, LClient.Pool.InUseCount);

    LimpaChaves(LClient, ['pipe']);
  finally
    LPipe.Free;
    LClient.Free;
  end;
end;

procedure TRedisClientIntegrationTests.Cliente_ConexaoDedicada_MantemOEstadoDaConexao;
var
  LClient, LDedicado: TRedisClient;
  LConn: TRedisConnection;
begin
  LClient := NovoCliente;
  try
    LConn := LClient.Acquire;
    try
      LDedicado := TRedisClient.CreateOnConnection(LConn);
      try
        TAssert.AssertTrue('e a conexao emprestada', LDedicado.Connection = LConn);
        // CLIENT SETNAME vive NA conexao. Ler o nome de volta so' funciona se
        // os dois comandos sairem pelo mesmo socket — que e' a razao de o modo
        // conexao unica existir.
        LDedicado.Execute('CLIENT', ['SETNAME', 'pascal-redis-faa-dedicado']);
        TAssert.AssertEquals('pascal-redis-faa-dedicado',
          LDedicado.Execute('CLIENT', ['GETNAME']).AsString);
        // Devolver a conexao pelo cliente dedicado e' no-op: quem a emprestou
        // foi o pool do outro cliente, e e' para ele que ela volta.
        LDedicado.Release(LConn);
        TAssert.AssertTrue('continua aberta', LConn.IsOpen);
      finally
        LDedicado.Free;
      end;
    finally
      LClient.Release(LConn);
    end;
    TAssert.AssertEquals(0, LClient.Pool.InUseCount);
  finally
    LClient.Free;
  end;
end;

{ TRedisKeysIntegrationTests }

procedure TRedisKeysIntegrationTests.Expiracao_TtlPersistETtlDeChaveAusente;
var
  LClient: TRedisClient;
  LTtl: Int64;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['exp']);
    // Chave ausente e chave sem prazo respondem valores DIFERENTES, e e' o
    // unico jeito de distinguir as duas com um comando so'.
    TAssert.AssertEquals(Int64(REDIS_TTL_NO_KEY), LClient.Keys.Ttl(Chave('exp')));

    LClient.Strings.SetValue(Chave('exp'), 'v');
    TAssert.AssertEquals(Int64(REDIS_TTL_NO_EXPIRY),
      LClient.Keys.Ttl(Chave('exp')));

    TAssert.AssertTrue('marcou o prazo', LClient.Keys.Expire(Chave('exp'), 60));
    LTtl := LClient.Keys.Ttl(Chave('exp'));
    TAssert.AssertTrue('ttl entre 1 e 60', (LTtl > 0) and (LTtl <= 60));

    TAssert.AssertTrue('tirou o prazo', LClient.Keys.Persist(Chave('exp')));
    TAssert.AssertEquals(Int64(REDIS_TTL_NO_EXPIRY),
      LClient.Keys.Ttl(Chave('exp')));

    LimpaChaves(LClient, ['exp']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysIntegrationTests.TipoRenomeacaoECopia;
var
  LClient: TRedisClient;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['k:a', 'k:b', 'k:c']);
    LClient.Strings.SetValue(Chave('k:a'), 'valor');
    TAssert.AssertEquals('string', LClient.Keys.KeyType(Chave('k:a')));
    TAssert.AssertEquals('none', LClient.Keys.KeyType(Chave('k:b')));

    LClient.Keys.Rename(Chave('k:a'), Chave('k:b'));
    TAssert.AssertFalse('a origem sumiu', LClient.Keys.Exists(Chave('k:a')));
    TAssert.AssertEquals('valor', LClient.Strings.GetString(Chave('k:b')));

    TAssert.AssertTrue('copiou', LClient.Keys.CopyKey(Chave('k:b'), Chave('k:c')));
    TAssert.AssertEquals('valor', LClient.Strings.GetString(Chave('k:c')));
    // Sem REPLACE a copia sobre chave existente NAO sobrescreve: devolve False
    // em vez de levantar.
    TAssert.AssertFalse('destino ocupado',
      LClient.Keys.CopyKey(Chave('k:b'), Chave('k:c')));

    TAssert.AssertEquals(Int64(2),
      LClient.Keys.Unlink([Chave('k:b'), Chave('k:c')]));
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysIntegrationTests.Scan_VarreTodasAsChavesDoPrefixo;
const
  QUANTAS = 30;
var
  LClient: TRedisClient;
  LArgs: TRedisArgs;
  LVistas: TStringList;
  LLote: TRedisStringArray;
  LCursor: Int64;
  I, LPassos: Integer;
begin
  LClient := NovoCliente;
  LVistas := TStringList.Create;
  try
    LVistas.Sorted := True;
    LVistas.Duplicates := dupIgnore;

    SetLength(LArgs, QUANTAS);
    for I := 1 to QUANTAS do
    begin
      LClient.Strings.SetValue(Chave('scan:' + IntToStr(I)), 'v');
      LArgs[I - 1] := Chave('scan:' + IntToStr(I));
    end;

    LCursor := 0;
    LPassos := 0;
    repeat
      // COUNT baixo de proposito: forca varios passos, que e' onde o cursor
      // precisa mesmo estar certo.
      LLote := LClient.Keys.Scan(LCursor, PREFIXO + 'scan:*', 5);
      for I := 0 to High(LLote) do
        LVistas.Add(LLote[I]);
      Inc(LPassos);
      TAssert.AssertTrue('scan sem fim a vista', LPassos < 1000);
    until LCursor = 0;

    // O SCAN pode repetir chave entre passos e pode devolver lote vazio com
    // cursor diferente de zero — o que ele garante e' que toda chave que
    // existia do inicio ao fim aparece pelo menos uma vez.
    TAssert.AssertEquals(QUANTAS, LVistas.Count);
    TAssert.AssertEquals(Int64(QUANTAS), LClient.Keys.DelMany(LArgs));
  finally
    LVistas.Free;
    LClient.Free;
  end;
end;

{ TRedisStringsIntegrationTests }

procedure TRedisStringsIntegrationTests.SetGet_ComBinarioEAcentuacao;
var
  LClient: TRedisClient;
  LOriginal, LVolta: TBytes;
  LTexto: string;
begin
  LClient := NovoCliente;
  try
    // CRLF e zero no meio do valor: o que quebraria um comando inline.
    LOriginal := MakeBytes([65, 13, 10, 66, 0, 255, 200]);
    LClient.Strings.SetValue(Chave('s:bin'), LOriginal);
    TAssert.AssertTrue('existe',
      LClient.Strings.TryGetBytes(Chave('s:bin'), LVolta));
    TAssert.AssertEquals(Hex(LOriginal), Hex(LVolta));

    LTexto := 'ação e coração';
    LClient.Strings.SetValue(Chave('s:utf8'), LTexto);
    TAssert.AssertEquals(LTexto, LClient.Strings.GetString(Chave('s:utf8')));
    // STRLEN conta BYTES: cada caractere acentuado do texto acima ocupa 2.
    TAssert.AssertEquals(Int64(Length(RedisUtf8Encode(LTexto))),
      LClient.Strings.StrLen(Chave('s:utf8')));

    LimpaChaves(LClient, ['s:bin', 's:utf8']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsIntegrationTests.SetWithOptions_NxNaoSobrescreve_EGetDevolveOAnterior;
var
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
  LReply: IRedisReply;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['s:lock']);

    LOpc := RedisDefaultSetOptions;
    LOpc.Condition := scNotExists;
    LOpc.Expiry := seSeconds;
    LOpc.ExpiryValue := 30;

    LReply := LClient.Strings.SetWithOptions(Chave('s:lock'), 'token-1', LOpc);
    TAssert.AssertFalse('a primeira gravou', LReply.IsNull);
    // A segunda tentativa e' barrada: e' assim que o lock distribuido diz
    // "outro ja' tem a posse".
    LReply := LClient.Strings.SetWithOptions(Chave('s:lock'), 'token-2', LOpc);
    TAssert.AssertTrue('a segunda foi barrada', LReply.IsNull);
    TAssert.AssertEquals('token-1', LClient.Strings.GetString(Chave('s:lock')));

    // Com GET, o SET devolve o valor ANTERIOR em vez de OK.
    LOpc := RedisDefaultSetOptions;
    LOpc.ReturnOldValue := True;
    LReply := LClient.Strings.SetWithOptions(Chave('s:lock'), 'token-3', LOpc);
    TAssert.AssertEquals('token-1', LReply.AsString);
    TAssert.AssertEquals('token-3', LClient.Strings.GetString(Chave('s:lock')));

    LimpaChaves(LClient, ['s:lock']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsIntegrationTests.SetSemKeepTtl_ApagaOPrazo_ComKeepTtl_Preserva;
var
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
begin
  LClient := NovoCliente;
  try
    LClient.Strings.SetEx(Chave('s:ttl'), 60, 'v1');
    TAssert.AssertTrue('nasceu com prazo', LClient.Keys.Ttl(Chave('s:ttl')) > 0);

    // Esta e' a armadilha: reescrever o valor com SET simples APAGA o prazo, e
    // o cache com TTL vira vazamento permanente sem nenhum erro pelo caminho.
    LClient.Strings.SetValue(Chave('s:ttl'), 'v2');
    TAssert.AssertEquals(Int64(REDIS_TTL_NO_EXPIRY),
      LClient.Keys.Ttl(Chave('s:ttl')));

    LClient.Strings.SetEx(Chave('s:ttl'), 60, 'v3');
    LOpc := RedisDefaultSetOptions;
    LOpc.Expiry := seKeepTtl;
    LClient.Strings.SetWithOptions(Chave('s:ttl'), 'v4', LOpc);
    TAssert.AssertTrue('o prazo sobreviveu',
      LClient.Keys.Ttl(Chave('s:ttl')) > 0);
    TAssert.AssertEquals('v4', LClient.Strings.GetString(Chave('s:ttl')));

    LimpaChaves(LClient, ['s:ttl']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsIntegrationTests.ContadoresInteiroEFlutuante;
var
  LClient: TRedisClient;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['s:cont', 's:saldo']);

    // Chave ausente conta como zero: nao precisa inicializar contador.
    TAssert.AssertEquals(Int64(1), LClient.Strings.Incr(Chave('s:cont')));
    TAssert.AssertEquals(Int64(11), LClient.Strings.IncrBy(Chave('s:cont'), 10));
    TAssert.AssertEquals(Int64(10), LClient.Strings.Decr(Chave('s:cont')));

    // O delta vai com ponto decimal mesmo numa maquina configurada em pt-BR;
    // com virgula o servidor recusaria com "value is not a valid float".
    TAssert.AssertEquals(1.5,
      LClient.Strings.IncrByFloat(Chave('s:saldo'), 1.5), 0.0001);
    TAssert.AssertEquals(1.75,
      LClient.Strings.IncrByFloat(Chave('s:saldo'), 0.25), 0.0001);

    LimpaChaves(LClient, ['s:cont', 's:saldo']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsIntegrationTests.MGet_ChaveAusenteVemComoNulo;
var
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['s:m1', 's:m2', 's:m3']);
    LClient.Strings.MSet([Chave('s:m1'), 'um', Chave('s:m3'), 'tres']);

    LReply := LClient.Strings.MGet(
      [Chave('s:m1'), Chave('s:m2'), Chave('s:m3')]);
    TAssert.AssertEquals(3, LReply.Count);
    TAssert.AssertEquals('um', LReply[0].AsString);
    // A do meio nunca foi gravada: vem NULA, e nao string vazia.
    TAssert.AssertTrue('a do meio nao existe', LReply[1].IsNull);
    TAssert.AssertEquals('tres', LReply[2].AsString);

    LimpaChaves(LClient, ['s:m1', 's:m2', 's:m3']);
  finally
    LClient.Free;
  end;
end;

{ TRedisHashesIntegrationTests }

procedure TRedisHashesIntegrationTests.HashCompleto_SetGetAllScanEDel;
var
  LClient: TRedisClient;
  LReply: IRedisReply;
  LCursor: Int64;
  LLote: TRedisStringArray;
  LValor: string;
  LTotal: Integer;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['h:sess']);

    TAssert.AssertEquals(Int64(3), LClient.Hashes.HSetMany(Chave('h:sess'),
      ['ip', '10.0.0.1', 'user', 'ana', 'hits', '1']));
    // Sobrescrever campo existente devolve 0 campos criados — nao e' erro.
    TAssert.AssertEquals(Int64(0),
      LClient.Hashes.HSet(Chave('h:sess'), 'user', 'ana maria'));
    TAssert.AssertEquals(Int64(3), LClient.Hashes.HLen(Chave('h:sess')));

    LReply := LClient.Hashes.HGetAll(Chave('h:sess'));
    TAssert.AssertEquals(6, LReply.Count);
    TAssert.AssertEquals('10.0.0.1', LReply.ValueByKey('ip').AsString);
    TAssert.AssertEquals('ana maria', LReply.ValueByKey('user').AsString);

    TAssert.AssertEquals(Int64(2),
      LClient.Hashes.HIncrBy(Chave('h:sess'), 'hits', 1));
    TAssert.AssertTrue('campo existe',
      LClient.Hashes.HTryGet(Chave('h:sess'), 'ip', LValor));
    TAssert.AssertEquals('10.0.0.1', LValor);
    TAssert.AssertFalse('campo ausente',
      LClient.Hashes.HTryGet(Chave('h:sess'), 'nada', LValor));

    LTotal := 0;
    LCursor := 0;
    repeat
      LLote := LClient.Hashes.HScan(Chave('h:sess'), LCursor);
      // O HSCAN devolve campo e valor ACHATADOS: o lote sempre vem em pares.
      TAssert.AssertFalse('lote em pares', Odd(Length(LLote)));
      Inc(LTotal, Length(LLote) div 2);
    until LCursor = 0;
    TAssert.AssertEquals(3, LTotal);

    TAssert.AssertTrue('apagou o campo',
      LClient.Hashes.HDel(Chave('h:sess'), 'hits'));
    TAssert.AssertFalse('nao apaga duas vezes',
      LClient.Hashes.HDel(Chave('h:sess'), 'hits'));

    LimpaChaves(LClient, ['h:sess']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesIntegrationTests.HGetAll_MesmaFormaEmResp2EResp3;
var
  LResp2, LResp3: TRedisClient;
  LA, LB: IRedisReply;
begin
  LResp2 := NovoCliente;
  LResp3 := NovoClienteResp3;
  try
    LimpaChaves(LResp2, ['h:proto']);
    LResp2.Hashes.HSetMany(Chave('h:proto'), ['a', '1', 'b', '2']);

    LA := LResp2.Hashes.HGetAll(Chave('h:proto'));
    LB := LResp3.Hashes.HGetAll(Chave('h:proto'));

    // Em RESP2 chega array, em RESP3 chega mapa — mas o leitor guarda mapa
    // ACHATADO, entao Count e ValueByKey dao o mesmo resultado nos dois. E' o
    // que permite a aplicacao trocar de protocolo sem tocar no codigo.
    TAssert.AssertEquals(4, LA.Count);
    TAssert.AssertEquals(LA.Count, LB.Count);
    TAssert.AssertEquals('1', LB.ValueByKey('a').AsString);
    TAssert.AssertEquals('2', LB.ValueByKey('b').AsString);

    LimpaChaves(LResp2, ['h:proto']);
  finally
    LResp3.Free;
    LResp2.Free;
  end;
end;

{ TRedisListsIntegrationTests }

procedure TRedisListsIntegrationTests.Lista_PushPopRangeEMove;
var
  LClient: TRedisClient;
  LItens: TRedisStringArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['l:fila', 'l:proc']);

    // LPUSH com varios valores INVERTE a ordem: cada um entra na frente do
    // anterior. E' a pegadinha que faz a fila sair ao contrario.
    LClient.Lists.LPushMany(Chave('l:fila'), ['a', 'b', 'c']);
    LItens := LClient.Lists.LRange(Chave('l:fila'), 0, -1);
    TAssert.AssertEquals(3, Length(LItens));
    TAssert.AssertEquals('c', LItens[0]);
    TAssert.AssertEquals('a', LItens[2]);

    TAssert.AssertEquals(Int64(3), LClient.Lists.LLen(Chave('l:fila')));
    TAssert.AssertEquals('c', LClient.Lists.LPop(Chave('l:fila')).AsString);

    // LMOVE tira do fim da fila e poe no inicio da lista de processamento,
    // atomico: e' o padrao da fila confiavel.
    TAssert.AssertEquals('a',
      LClient.Lists.LMove(Chave('l:fila'), Chave('l:proc'), leRight, leLeft).AsString);
    TAssert.AssertEquals(Int64(1), LClient.Lists.LLen(Chave('l:proc')));

    LClient.Lists.LTrim(Chave('l:fila'), 1, 0);
    TAssert.AssertEquals(Int64(0), LClient.Lists.LLen(Chave('l:fila')));
    // Lista vazia nao existe no Redis: o POP devolve NULO, nao erro.
    TAssert.AssertTrue('vazia', LClient.Lists.LPop(Chave('l:fila')).IsNull);

    LimpaChaves(LClient, ['l:fila', 'l:proc']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsIntegrationTests.BLPop_ValorJaEnfileirado_VoltaNaHora;
var
  LClient: TRedisClient;
  LChave, LValor: string;
  LInicio: UInt64;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['l:b1', 'l:b2']);
    LClient.Lists.RPush(Chave('l:b2'), 'tarefa');

    LInicio := RedisTickMs;
    TAssert.AssertTrue('achou',
      LClient.Lists.BLPop([Chave('l:b1'), Chave('l:b2')], 5, LChave, LValor));
    // Com valor disponivel o BLPOP nem chega a bloquear.
    TAssert.AssertTrue('voltou na hora', RedisTickMs - LInicio < 2000);
    // Com varias chaves na chamada, so' a resposta diz de qual delas veio.
    TAssert.AssertEquals(Chave('l:b2'), LChave);
    TAssert.AssertEquals('tarefa', LValor);

    LimpaChaves(LClient, ['l:b1', 'l:b2']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsIntegrationTests.BLPop_PrazoMaiorQueOReceiveTimeout_NaoEstouraOSocket;
var
  LParams: TRedisParams;
  LClient: TRedisClient;
  LChave, LValor: string;
  LInicio, LGasto: UInt64;
begin
  // Read timeout de 1 s e BLPOP de 2 s: se o comando bloqueante saisse pelo
  // pool comum, o socket desistiria no meio e levantaria ERedisTimeout com a
  // resposta ainda a caminho. E' exatamente o cenario que motivou a conexao
  // separada com o prazo esticado.
  LParams := ParamsDeTeste;
  LParams.ReceiveTimeoutMs := 1000;
  LClient := TRedisClient.Create(LParams);
  try
    LimpaChaves(LClient, ['l:vazia']);

    LInicio := RedisTickMs;
    TAssert.AssertFalse('nada chegou',
      LClient.Lists.BLPop([Chave('l:vazia')], 2, LChave, LValor));
    LGasto := RedisTickMs - LInicio;

    // Esperou o prazo do COMANDO (2 s), nao o do socket (1 s).
    TAssert.AssertTrue('esperou os 2 s do comando', LGasto >= 1800);
    TAssert.AssertTrue('e nao esperou demais', LGasto < 6000);

    // E o pool comum continua servindo: o bloqueante nao passou por ele.
    LClient.Strings.SetValue(Chave('l:apos'), 'ok');
    TAssert.AssertEquals('ok', LClient.Strings.GetString(Chave('l:apos')));

    LimpaChaves(LClient, ['l:vazia', 'l:apos']);
  finally
    LClient.Free;
  end;
end;

{ TRedisSetsIntegrationTests }

procedure TRedisSetsIntegrationTests.Conjunto_AddPertinenciaEOperacoes;
var
  LClient: TRedisClient;
  LPertence: TRedisBooleanArray;
  LInter: TRedisStringArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['t:a', 't:b', 't:i']);

    TAssert.AssertEquals(Int64(3),
      LClient.Sets.SAddMany(Chave('t:a'), ['redis', 'pascal', 'delphi']));
    // Membro repetido devolve False, e isso NAO e' falha.
    TAssert.AssertFalse('ja estava', LClient.Sets.SAdd(Chave('t:a'), 'redis'));
    TAssert.AssertEquals(Int64(3), LClient.Sets.SCard(Chave('t:a')));

    LClient.Sets.SAddMany(Chave('t:b'), ['redis', 'lazarus']);

    LPertence := LClient.Sets.SMIsMember(Chave('t:a'), ['redis', 'lazarus']);
    TAssert.AssertEquals(2, Length(LPertence));
    TAssert.AssertTrue('redis pertence', LPertence[0]);
    TAssert.AssertFalse('lazarus nao pertence', LPertence[1]);

    LInter := LClient.Sets.SInter([Chave('t:a'), Chave('t:b')]);
    TAssert.AssertEquals(1, Length(LInter));
    TAssert.AssertEquals('redis', LInter[0]);

    TAssert.AssertEquals(Int64(1),
      LClient.Sets.SInterStore(Chave('t:i'), [Chave('t:a'), Chave('t:b')]));
    TAssert.AssertEquals(Int64(1), LClient.Sets.SCard(Chave('t:i')));
    TAssert.AssertEquals(Int64(1),
      LClient.Sets.SInterCard([Chave('t:a'), Chave('t:b')]));

    LimpaChaves(LClient, ['t:a', 't:b', 't:i']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsIntegrationTests.SScan_VarreOConjuntoInteiro;
const
  QUANTOS = 40;
var
  LClient: TRedisClient;
  LMembros: TRedisArgs;
  LVistos: TStringList;
  LLote: TRedisStringArray;
  LCursor: Int64;
  I: Integer;
begin
  LClient := NovoCliente;
  LVistos := TStringList.Create;
  try
    LVistos.Sorted := True;
    LVistos.Duplicates := dupIgnore;
    LimpaChaves(LClient, ['t:scan']);

    SetLength(LMembros, QUANTOS);
    for I := 1 to QUANTOS do
      LMembros[I - 1] := 'm' + IntToStr(I);
    LClient.Sets.SAddMany(Chave('t:scan'), LMembros);

    LCursor := 0;
    repeat
      LLote := LClient.Sets.SScan(Chave('t:scan'), LCursor, '', 7);
      for I := 0 to High(LLote) do
        LVistos.Add(LLote[I]);
    until LCursor = 0;

    TAssert.AssertEquals(QUANTOS, LVistos.Count);

    LimpaChaves(LClient, ['t:scan']);
  finally
    LVistos.Free;
    LClient.Free;
  end;
end;

{ TRedisZSetsIntegrationTests }

procedure TRedisZSetsIntegrationTests.Ranking_AddRangeEIncr;
var
  LClient: TRedisClient;
  LTopo: TRedisStringArray;
  LScore: Double;
  LRank: Int64;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['z:rank']);

    TAssert.AssertEquals(Int64(3), LClient.ZSets.ZAddMany(Chave('z:rank'),
      [100, 'ana', 300, 'bob', 200, 'cida']));
    TAssert.AssertEquals(Int64(3), LClient.ZSets.ZCard(Chave('z:rank')));

    // ZREVRANGE 0..1 e' o top 2 — o maior score primeiro.
    LTopo := LClient.ZSets.ZRevRange(Chave('z:rank'), 0, 1);
    TAssert.AssertEquals(2, Length(LTopo));
    TAssert.AssertEquals('bob', LTopo[0]);
    TAssert.AssertEquals('cida', LTopo[1]);

    TAssert.AssertTrue('ana tem score',
      LClient.ZSets.ZTryScore(Chave('z:rank'), 'ana', LScore));
    TAssert.AssertEquals(100, LScore, 0.0001);
    TAssert.AssertFalse('quem nao esta no conjunto nao tem score',
      LClient.ZSets.ZTryScore(Chave('z:rank'), 'ninguem', LScore));

    TAssert.AssertEquals(150.5,
      LClient.ZSets.ZIncrBy(Chave('z:rank'), 50.5, 'ana'), 0.0001);
    // A posicao 0-based na ordem CRESCENTE: com 150.5 a ana continua na base.
    TAssert.AssertTrue('ana tem posicao',
      LClient.ZSets.ZTryRank(Chave('z:rank'), 'ana', LRank));
    TAssert.AssertEquals(Int64(0), LRank);

    LimpaChaves(LClient, ['z:rank']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsIntegrationTests.ZRangeWithScores_MesmoResultadoEmResp2EResp3;
var
  LResp2, LResp3: TRedisClient;
  LA, LB: TRedisScoreMemberArray;
begin
  LResp2 := NovoCliente;
  LResp3 := NovoClienteResp3;
  try
    LimpaChaves(LResp2, ['z:proto']);
    LResp2.ZSets.ZAddMany(Chave('z:proto'), [1.5, 'ana', 2.5, 'bob']);

    LA := LResp2.ZSets.ZRangeWithScores(Chave('z:proto'), 0, -1);
    // Em RESP3 o MESMO comando responde uma lista de PARES [membro, score],
    // com o score como double nativo, enquanto o RESP2 responde uma lista
    // achatada. Sem a conversao da familia, a aplicacao teria de ramificar por
    // protocolo justo no comando mais usado do tipo.
    LB := LResp3.ZSets.ZRangeWithScores(Chave('z:proto'), 0, -1);

    TAssert.AssertEquals(2, Length(LA));
    TAssert.AssertEquals(Length(LA), Length(LB));
    TAssert.AssertEquals('ana', LB[0].Member);
    TAssert.AssertEquals(1.5, LB[0].Score, 0.0001);
    TAssert.AssertEquals('bob', LB[1].Member);
    TAssert.AssertEquals(2.5, LB[1].Score, 0.0001);

    LimpaChaves(LResp2, ['z:proto']);
  finally
    LResp3.Free;
    LResp2.Free;
  end;
end;

procedure TRedisZSetsIntegrationTests.ZRangeByScore_FaixaAbertaELimite;
var
  LClient: TRedisClient;
  LFaixa: TRedisStringArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['z:faixa']);
    LClient.ZSets.ZAddMany(Chave('z:faixa'),
      [10, 'a', 20, 'b', 30, 'c', 40, 'd']);

    // '(20' e' extremo ABERTO: maior que 20, sem incluir o 20.
    LFaixa := LClient.ZSets.ZRangeByScore(Chave('z:faixa'),
      RedisScoreBound(20, True), REDIS_SCORE_MAX);
    TAssert.AssertEquals(2, Length(LFaixa));
    TAssert.AssertEquals('c', LFaixa[0]);

    // Fechado inclui o extremo.
    LFaixa := LClient.ZSets.ZRangeByScore(Chave('z:faixa'),
      RedisScoreBound(20), REDIS_SCORE_MAX);
    TAssert.AssertEquals(3, Length(LFaixa));

    // LIMIT offset count, como no SQL.
    LFaixa := LClient.ZSets.ZRangeByScore(Chave('z:faixa'),
      REDIS_SCORE_MIN, REDIS_SCORE_MAX, 1, 2);
    TAssert.AssertEquals(2, Length(LFaixa));
    TAssert.AssertEquals('b', LFaixa[0]);

    TAssert.AssertEquals(Int64(4), LClient.ZSets.ZCount(Chave('z:faixa'),
      REDIS_SCORE_MIN, REDIS_SCORE_MAX));

    LimpaChaves(LClient, ['z:faixa']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsIntegrationTests.ZPopMin_EsvaziaDoMenorParaOMaior;
var
  LClient: TRedisClient;
  LMembro: string;
  LScore: Double;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['z:pop']);
    LClient.ZSets.ZAddMany(Chave('z:pop'), [2, 'b', 1, 'a']);

    TAssert.AssertTrue('tirou o menor',
      LClient.ZSets.ZPopMin(Chave('z:pop'), LMembro, LScore));
    TAssert.AssertEquals('a', LMembro);
    TAssert.AssertEquals(1, LScore, 0.0001);

    TAssert.AssertTrue('tirou o proximo',
      LClient.ZSets.ZPopMin(Chave('z:pop'), LMembro, LScore));
    TAssert.AssertEquals('b', LMembro);

    // Conjunto vazio deixa de existir: o POP devolve False, nao erro.
    TAssert.AssertFalse('acabou',
      LClient.ZSets.ZPopMin(Chave('z:pop'), LMembro, LScore));
    TAssert.AssertFalse('e a chave sumiu', LClient.Keys.Exists(Chave('z:pop')));
  finally
    LClient.Free;
  end;
end;

{ --- M6: transacoes e scripting --- }

const
  { O release de lock que NAO existe sem script: comparar o token e apagar tem
    de acontecer na mesma passagem. Com GET seguido de DEL, o lock pode expirar
    entre os dois e ser tomado por outra pessoa — e o DEL apagaria o lock DELA. }
  SCRIPT_LIBERA_LOCK =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then' + #10 +
    '  return redis.call("DEL", KEYS[1])' + #10 +
    'else' + #10 +
    '  return 0' + #10 +
    'end';

{ TRedisTransactionIntegrationTests }

procedure TRedisTransactionIntegrationTests.Transacao_RodaOBlocoInteiro;
var
  LClient: TRedisClient;
  LTx: TRedisTransaction;
  LRespostas: TRedisReplyArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['tx:n']);
    LTx := LClient.BeginTransaction;
    try
      LTx.Queue('INCR', [Chave('tx:n')]);
      LTx.Queue('INCRBY', [Chave('tx:n'), 10]);
      LTx.Queue('GET', [Chave('tx:n')]);
      LRespostas := LTx.Commit;
    finally
      LTx.Free;
    end;

    TAssert.AssertEquals(3, Length(LRespostas));
    TAssert.AssertEquals(Int64(1), LRespostas[0].AsInteger);
    TAssert.AssertEquals(Int64(11), LRespostas[1].AsInteger);
    TAssert.AssertEquals('11', LRespostas[2].AsString);

    LimpaChaves(LClient, ['tx:n']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisTransactionIntegrationTests.Watch_AbortaQuandoOutraConexaoMexeNaChave;
var
  LClient: TRedisClient;
  LTx: TRedisTransaction;
  LIntruso: TRedisConnection;
  LRespostas: TRedisReplyArray;
  LSaldo: Int64;
begin
  LClient := NovoCliente;
  try
    LClient.Strings.SetValue(Chave('tx:saldo'), '500');

    LTx := LClient.BeginTransaction;
    try
      LTx.Watch([Chave('tx:saldo')]);
      // A leitura acontece FORA do bloco — dentro dele nao ha' leitura, e e'
      // por isso que o WATCH existe.
      LSaldo := LTx.Connection.Execute('GET', [Chave('tx:saldo')]).AsInteger;

      // Outra conexao mexe na chave vigiada, entre o WATCH e o EXEC.
      LIntruso := AbreConexao;
      try
        LIntruso.Execute('SET', [Chave('tx:saldo'), '999']);
      finally
        LIntruso.Free;
      end;

      LTx.Queue('SET', [Chave('tx:saldo'), LSaldo - 100]);
      // False, e nao excecao: sob concorrencia isto e' o funcionamento normal
      // do check-and-set, e quem chama recomeca o ciclo.
      TAssert.AssertFalse('o EXEC tem de abortar', LTx.TryCommit(LRespostas));
      TAssert.AssertEquals(0, Length(LRespostas));
    finally
      LTx.Free;
    end;

    // O valor do intruso sobreviveu: a transacao nao gravou nada.
    TAssert.AssertEquals('999', LClient.Strings.GetString(Chave('tx:saldo')));

    LimpaChaves(LClient, ['tx:saldo']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisTransactionIntegrationTests.Watch_NaoAbortaQuandoNinguemMexe;
var
  LClient: TRedisClient;
  LTx: TRedisTransaction;
  LRespostas: TRedisReplyArray;
  LSaldo: Int64;
begin
  LClient := NovoCliente;
  try
    LClient.Strings.SetValue(Chave('tx:saldo'), '500');

    LTx := LClient.BeginTransaction;
    try
      LTx.Watch([Chave('tx:saldo')]);
      LSaldo := LTx.Connection.Execute('GET', [Chave('tx:saldo')]).AsInteger;
      LTx.Queue('SET', [Chave('tx:saldo'), LSaldo - 100]);
      TAssert.AssertTrue('sem interferencia, commita',
        LTx.TryCommit(LRespostas));
      TAssert.AssertEquals(1, Length(LRespostas));
    finally
      LTx.Free;
    end;

    TAssert.AssertEquals('400', LClient.Strings.GetString(Chave('tx:saldo')));

    LimpaChaves(LClient, ['tx:saldo']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisTransactionIntegrationTests.ErroNoMeioDoBloco_NaoDesfazOsOutros;
var
  LClient: TRedisClient;
  LTx: TRedisTransaction;
  LRespostas: TRedisReplyArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['tx:a', 'tx:b', 'tx:str']);
    LClient.Strings.SetValue(Chave('tx:str'), 'texto');

    LTx := LClient.BeginTransaction;
    try
      LTx.Queue('SET', [Chave('tx:a'), '1']);
      LTx.Queue('LPUSH', [Chave('tx:str'), 'x']);   // WRONGTYPE na execucao
      LTx.Queue('SET', [Chave('tx:b'), '2']);
      LRespostas := LTx.Commit;
    finally
      LTx.Free;
    end;

    TAssert.AssertEquals(3, Length(LRespostas));
    TAssert.AssertTrue('o do meio falhou', LRespostas[1].IsError);
    TAssert.AssertEquals('WRONGTYPE', LRespostas[1].ErrorCode);

    // **Nao existe rollback no Redis.** Os outros dois comandos rodaram e
    // ficaram gravados. Quem precisa de tudo-ou-nada de verdade usa um script
    // Lua, nao MULTI/EXEC.
    TAssert.AssertEquals('1', LClient.Strings.GetString(Chave('tx:a')));
    TAssert.AssertEquals('2', LClient.Strings.GetString(Chave('tx:b')));

    LimpaChaves(LClient, ['tx:a', 'tx:b', 'tx:str']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisTransactionIntegrationTests.Transacao_DevolveAConexaoAoPool;
var
  LClient: TRedisClient;
  LTx: TRedisTransaction;
begin
  LClient := NovoCliente;
  try
    LClient.Ping;   // forca o pool a abrir a primeira conexao
    LTx := LClient.BeginTransaction;
    try
      // Enquanto a transacao vive, a conexao esta' FORA de circulacao: e' o
      // preco da afinidade de conexao que o MULTI exige.
      TAssert.AssertEquals(1, LClient.Pool.InUseCount);
      LTx.Queue('PING');
      LTx.Commit;
    finally
      LTx.Free;
    end;
    // E o destrutor devolve. Sem isso, cada transacao vazaria uma conexao do
    // pool ate' o MaxSize estourar.
    TAssert.AssertEquals(0, LClient.Pool.InUseCount);
    TAssert.AssertEquals(1, LClient.Pool.IdleCount);
  finally
    LClient.Free;
  end;
end;

procedure TRedisTransactionIntegrationTests.WatchPendente_NaoContaminaAProximaTransacao;
var
  LParams: TRedisParams;
  LPoolParams: TRedisPoolParams;
  LClient: TRedisClient;
  LTx: TRedisTransaction;
  LIntruso: TRedisConnection;
  LRespostas: TRedisReplyArray;
begin
  // MaxSize = 1 obriga o pool a reusar a MESMA conexao — que e' o que torna a
  // contaminacao observavel.
  LParams := ParamsDeTeste;
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 1;
  LClient := TRedisClient.Create(LParams, LPoolParams);
  try
    LimpaChaves(LClient, ['tx:vigiada', 'tx:destino']);

    // Primeira transacao: vigia e desiste SEM commitar.
    LTx := LClient.BeginTransaction;
    try
      LTx.Watch([Chave('tx:vigiada')]);
    finally
      LTx.Free;   // o destrutor tem de mandar UNWATCH
    end;

    // Alguem mexe na chave que a transacao ABANDONADA vigiava.
    LIntruso := AbreConexao;
    try
      LIntruso.Execute('SET', [Chave('tx:vigiada'), 'mudou']);
    finally
      LIntruso.Free;
    end;

    // Segunda transacao, pela MESMA conexao, sem watch nenhum. Se o UNWATCH
    // nao tivesse saido, este EXEC abortaria por causa de uma chave que esta
    // transacao nunca vigiou — e quem escreveu este codigo nao teria como
    // descobrir por que.
    LTx := LClient.BeginTransaction;
    try
      LTx.Queue('SET', [Chave('tx:destino'), 'ok']);
      TAssert.AssertTrue('watch abandonado nao pode abortar isto',
        LTx.TryCommit(LRespostas));
    finally
      LTx.Free;
    end;
    TAssert.AssertEquals('ok', LClient.Strings.GetString(Chave('tx:destino')));

    LimpaChaves(LClient, ['tx:vigiada', 'tx:destino']);
  finally
    LClient.Free;
  end;
end;

{ TRedisScriptingIntegrationTests }

procedure TRedisScriptingIntegrationTests.Eval_ExecutaLuaEConverteOsTipos;
var
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  LClient := NovoCliente;
  try
    // Inteiro, texto e lista: os tres tipos que o Lua devolve ao RESP.
    TAssert.AssertEquals(Int64(3),
      LClient.Scripting.Eval('return 1 + 2', [], []).AsInteger);
    TAssert.AssertEquals('ola',
      LClient.Scripting.Eval('return "ola"', [], []).AsString);

    LReply := LClient.Scripting.Eval('return {KEYS[1], ARGV[1]}',
      ['achave'], ['oarg']);
    TAssert.AssertEquals(2, LReply.Count);
    // Separar KEYS de ARGV nao e' burocracia: e' o que diz ao servidor quais
    // chaves o script toca.
    TAssert.AssertEquals('achave', LReply[0].AsString);
    TAssert.AssertEquals('oarg', LReply[1].AsString);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingIntegrationTests.ScriptLoad_OShaDoServidorBateComOLocal;
var
  LClient: TRedisClient;
  LDoServidor, LLocal: string;
begin
  LClient := NovoCliente;
  try
    LDoServidor := LClient.Scripting.ScriptLoad(SCRIPT_LIBERA_LOCK);
    LLocal := RedisScriptSha(SCRIPT_LIBERA_LOCK);
    // E' a premissa do cache inteiro: se o SHA calculado aqui nao fosse o
    // mesmo que o servidor calcula, todo EVALSHA responderia NOSCRIPT e o
    // cache trabalharia sem nunca acertar.
    TAssert.AssertEquals(LDoServidor, LLocal);
    TAssert.AssertTrue('o servidor conhece o sha',
      LClient.Scripting.ScriptExists(LLocal));
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingIntegrationTests.Run_SegundaChamadaUsaOCache;
var
  LClient: TRedisClient;
begin
  LClient := NovoCliente;
  try
    TAssert.AssertEquals(0, LClient.Scripting.CachedCount);
    TAssert.AssertEquals(Int64(7),
      LClient.Scripting.Run('return 7', [], []).AsInteger);
    TAssert.AssertEquals(1, LClient.Scripting.CachedCount);
    // A segunda vai por EVALSHA — o resultado tem de ser o mesmo, e e' isso
    // que o teste garante de fora.
    TAssert.AssertEquals(Int64(7),
      LClient.Scripting.Run('return 7', [], []).AsInteger);
    TAssert.AssertEquals(1, LClient.Scripting.CachedCount);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingIntegrationTests.Run_AposScriptFlush_SeRecuperaSozinho;
var
  LClient, LOutro: TRedisClient;
begin
  LClient := NovoCliente;
  LOutro := NovoCliente;
  try
    LClient.Scripting.Run('return 42', [], []);
    TAssert.AssertEquals(1, LClient.Scripting.CachedCount);

    // Outro cliente esvazia o cache DO SERVIDOR. O nosso continua achando que
    // o script esta' la' — e' exatamente o estado que produz NOSCRIPT.
    LOutro.Execute('SCRIPT', ['FLUSH']);
    TAssert.AssertEquals(1, LClient.Scripting.CachedCount);

    // Nenhum erro chega a quem chamou: a lib reensina o script sozinha. E' o
    // que torna o cache uma otimizacao que se autocorrige, e nao uma suposicao
    // sobre o estado do servidor (que um failover para replica tambem quebra).
    TAssert.AssertEquals(Int64(42),
      LClient.Scripting.Run('return 42', [], []).AsInteger);
  finally
    LOutro.Free;
    LClient.Free;
  end;
end;

procedure TRedisScriptingIntegrationTests.LockDistribuido_SoQuemTemOTokenLibera;
var
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['tx:lock']);

    LOpc := RedisDefaultSetOptions;
    LOpc.Condition := scNotExists;
    LOpc.Expiry := seSeconds;
    LOpc.ExpiryValue := 30;
    TAssert.AssertFalse('tomou o lock',
      LClient.Strings.SetWithOptions(Chave('tx:lock'), 'token-A', LOpc).IsNull);

    // Token errado nao libera nada. Sem o script, um GET seguido de DEL faria
    // o portador do token B apagar o lock do token A no intervalo entre os
    // dois comandos — o erro classico de lock distribuido.
    TAssert.AssertEquals(Int64(0), LClient.Scripting.Run(
      SCRIPT_LIBERA_LOCK, [Chave('tx:lock')], ['token-B']).AsInteger);
    TAssert.AssertEquals('token-A',
      LClient.Strings.GetString(Chave('tx:lock')));

    // O dono libera.
    TAssert.AssertEquals(Int64(1), LClient.Scripting.Run(
      SCRIPT_LIBERA_LOCK, [Chave('tx:lock')], ['token-A']).AsInteger);
    TAssert.AssertFalse('o lock sumiu', LClient.Keys.Exists(Chave('tx:lock')));
  finally
    LClient.Free;
  end;
end;

{ TRedisMensagensRecebidas }

constructor TRedisMensagensRecebidas.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FItens := TStringList.Create;
end;

destructor TRedisMensagensRecebidas.Destroy;
begin
  FItens.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TRedisMensagensRecebidas.Mensagem(ASender: TObject;
  const AMessage: TRedisPubSubMessage);
begin
  FLock.Enter;
  try
    FItens.Add(AMessage.Channel + '=' + AMessage.Text);
    FUltimoPayload := Copy(AMessage.Payload, 0, Length(AMessage.Payload));
  finally
    FLock.Leave;
  end;
end;

procedure TRedisMensagensRecebidas.Reconectou(ASender: TObject);
begin
  FLock.Enter;
  try
    Inc(FReconexoes);
  finally
    FLock.Leave;
  end;
end;

function TRedisMensagensRecebidas.Espera(ACount, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    if Total >= ACount then
      Exit(True);
    Sleep(5);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

function TRedisMensagensRecebidas.EsperaReconexao(ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
  LTem: Integer;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    FLock.Enter;
    try
      LTem := FReconexoes;
    finally
      FLock.Leave;
    end;
    if LTem > 0 then
      Exit(True);
    Sleep(10);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

function TRedisMensagensRecebidas.Texto: string;
var
  I: Integer;
begin
  Result := '';
  FLock.Enter;
  try
    for I := 0 to FItens.Count - 1 do
    begin
      if I > 0 then
        Result := Result + '|';
      Result := Result + FItens[I];
    end;
  finally
    FLock.Leave;
  end;
end;

function TRedisMensagensRecebidas.UltimoPayload: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FUltimoPayload, 0, Length(FUltimoPayload));
  finally
    FLock.Leave;
  end;
end;

function TRedisMensagensRecebidas.Total: Integer;
begin
  FLock.Enter;
  try
    Result := FItens.Count;
  finally
    FLock.Leave;
  end;
end;

{ TRedisPubSubIntegrationTests }

// Nome de canal do teste. Canal nao e' chave: some sozinho quando o ultimo
// assinante sai, entao nao entra na limpeza.
function Canal(const ASufixo: string): string;
begin
  Result := PREFIXO + 'canal:' + ASufixo;
end;

// Espera o servidor contar ACount assinantes no canal. Depois de uma
// reconexao o SUBSCRIBE de volta ainda pode estar a caminho, e quem sabe
// quando ele valeu e' o servidor.
function EsperaAssinantes(AClient: TRedisClient; const ACanal: string;
  ACount, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
  LContagem: TRedisChannelCountArray;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    LContagem := AClient.PubSub.CountSubscribers([ACanal]);
    if (Length(LContagem) = 1) and (LContagem[0].Subscribers >= ACount) then
      Exit(True);
    Sleep(10);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

procedure TRedisPubSubIntegrationTests.Publica_EAssinanteRecebe;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LColetor: TRedisMensagensRecebidas;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LColetor := TRedisMensagensRecebidas.Create;
  LSub := LClient.CreateSubscriber;
  try
    LSub.OnMessage := LColetor.Mensagem;
    LSub.Start;
    LSub.Subscribe([Canal('a')]);
    // O Subscribe so' volta depois da confirmacao do servidor: quando o
    // PUBLISH abaixo sai, o assinante JA' esta' inscrito. Sem essa garantia o
    // teste seria uma corrida disfarcada de teste.
    TAssert.AssertEquals('um assinante recebeu', Int64(1),
      LClient.PubSub.Publish(Canal('a'), 'bom dia'));
    TAssert.AssertTrue('a mensagem chegou', LColetor.Espera(1, 5000));
    TAssert.AssertEquals(Canal('a') + '=bom dia', LColetor.Texto);
  finally
    LSub.Free;
    LColetor.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.PublishSemAssinante_DevolveZeroESePerde;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LColetor: TRedisMensagensRecebidas;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LColetor := TRedisMensagensRecebidas.Create;
  try
    // Pub/sub e' fire-and-forget: sem assinante no ar, a mensagem evapora.
    // Nao ha' fila esperando alguem chegar — e' a diferenca para Streams.
    TAssert.AssertEquals('ninguem recebeu', Int64(0),
      LClient.PubSub.Publish(Canal('perdida'), 'ninguem ouviu'));

    LSub := LClient.CreateSubscriber;
    try
      LSub.OnMessage := LColetor.Mensagem;
      LSub.Start;
      LSub.Subscribe([Canal('perdida')]);
      // Chegou depois: a mensagem anterior nao volta.
      TAssert.AssertFalse('nada foi entregue com atraso',
        LColetor.Espera(1, 500));
    finally
      LSub.Free;
    end;
  finally
    LColetor.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.Padrao_CasaVariosCanais;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LColetor: TRedisMensagensRecebidas;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LColetor := TRedisMensagensRecebidas.Create;
  LSub := LClient.CreateSubscriber;
  try
    LSub.OnMessage := LColetor.Mensagem;
    LSub.Start;
    LSub.PSubscribe([Canal('p') + '.*']);
    LClient.PubSub.Publish(Canal('p') + '.um', 'primeiro');
    LClient.PubSub.Publish(Canal('p') + '.dois', 'segundo');
    TAssert.AssertTrue('chegaram as duas', LColetor.Espera(2, 5000));
    // O pmessage traz o canal REAL, nao o padrao: e' o que permite ramificar.
    TAssert.AssertEquals(Canal('p') + '.um=primeiro|' +
      Canal('p') + '.dois=segundo', LColetor.Texto);
    TAssert.AssertTrue('o servidor conta o padrao',
      LClient.PubSub.NumPatterns >= 1);
  finally
    LSub.Free;
    LColetor.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.PubSubChannels_EnxergaAAssinatura;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LCanais: TRedisStringArray;
  LContagem: TRedisChannelCountArray;
  I: Integer;
  LAchou: Boolean;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LSub := LClient.CreateSubscriber;
  try
    LSub.Start;
    LSub.Subscribe([Canal('visivel')]);

    LCanais := LClient.PubSub.ActiveChannels(PREFIXO + '*');
    LAchou := False;
    for I := 0 to High(LCanais) do
      if LCanais[I] = Canal('visivel') then
        LAchou := True;
    TAssert.AssertTrue('o canal aparece no PUBSUB CHANNELS', LAchou);

    LContagem := LClient.PubSub.CountSubscribers([Canal('visivel'),
      Canal('ninguem')]);
    TAssert.AssertEquals(2, Length(LContagem));
    TAssert.AssertEquals(Canal('visivel'), LContagem[0].Channel);
    TAssert.AssertEquals(Int64(1), LContagem[0].Subscribers);
    // Canal sem assinante nenhum nao some da resposta: vem com zero.
    TAssert.AssertEquals(Canal('ninguem'), LContagem[1].Channel);
    TAssert.AssertEquals(Int64(0), LContagem[1].Subscribers);
  finally
    LSub.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.Unsubscribe_ParaDeReceber;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LColetor: TRedisMensagensRecebidas;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LColetor := TRedisMensagensRecebidas.Create;
  LSub := LClient.CreateSubscriber;
  try
    LSub.OnMessage := LColetor.Mensagem;
    LSub.Start;
    LSub.Subscribe([Canal('sai')]);
    LClient.PubSub.Publish(Canal('sai'), 'antes');
    TAssert.AssertTrue('chegou a primeira', LColetor.Espera(1, 5000));

    LSub.Unsubscribe([Canal('sai')]);
    // O servidor ja' esqueceu a assinatura: nao ha' mais para quem entregar.
    TAssert.AssertEquals('sem assinantes', Int64(0),
      LClient.PubSub.Publish(Canal('sai'), 'depois'));
    TAssert.AssertFalse('e nada chegou', LColetor.Espera(2, 500));
    TAssert.AssertEquals(0, LSub.SubscriptionCount);
  finally
    LSub.Free;
    LColetor.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.PayloadBinario_SobreviveAoRoundTrip;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LColetor: TRedisMensagensRecebidas;
  LBin: TBytes;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LColetor := TRedisMensagensRecebidas.Create;
  LSub := LClient.CreateSubscriber;
  try
    // Byte zero e CRLF no meio: mensagem de pub/sub e' binaria como qualquer
    // valor do Redis, e o caminho inteiro (PUBLISH, push, callback) tem de
    // preservar isso.
    LBin := MakeBytes([0, 13, 10, 255, 1, 65]);
    LSub.OnMessage := LColetor.Mensagem;
    LSub.Start;
    LSub.Subscribe([Canal('bin')]);
    LClient.PubSub.Publish(Canal('bin'), LBin);
    TAssert.AssertTrue('chegou', LColetor.Espera(1, 5000));
    TAssert.AssertEquals(Hex(LBin), Hex(LColetor.UltimoPayload));
  finally
    LSub.Free;
    LColetor.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.VariosAssinantes_TodosRecebem;
var
  LClient: TRedisClient;
  LSub1, LSub2: TRedisSubscriber;
  LColetor1, LColetor2: TRedisMensagensRecebidas;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LColetor1 := TRedisMensagensRecebidas.Create;
  LColetor2 := TRedisMensagensRecebidas.Create;
  LSub1 := LClient.CreateSubscriber;
  LSub2 := LClient.CreateSubscriber;
  try
    LSub1.OnMessage := LColetor1.Mensagem;
    LSub2.OnMessage := LColetor2.Mensagem;
    LSub1.Start;
    LSub2.Start;
    LSub1.Subscribe([Canal('fanout')]);
    LSub2.Subscribe([Canal('fanout')]);
    // Pub/sub e' difusao: uma publicacao, N entregas. O retorno do PUBLISH e'
    // a unica confirmacao que existe, e conta quem estava ouvindo AGORA.
    TAssert.AssertEquals('os dois receberam', Int64(2),
      LClient.PubSub.Publish(Canal('fanout'), 'para todos'));
    TAssert.AssertTrue('o primeiro', LColetor1.Espera(1, 5000));
    TAssert.AssertTrue('o segundo', LColetor2.Espera(1, 5000));
  finally
    LSub1.Free;
    LSub2.Free;
    LColetor1.Free;
    LColetor2.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.Resp2_ComandoComumComAssinatura_Levanta;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LLevantou: Boolean;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LSub := LClient.CreateSubscriber;
  try
    LSub.Start;
    // Sem assinatura ativa a conexao RESP2 ainda e' comum.
    TAssert.AssertTrue('PING antes de assinar', LSub.Ping);
    LSub.Subscribe([Canal('resp2')]);
    LLevantou := False;
    try
      LSub.Execute('GET', [Chave('resp2')]);
    except
      on E: ERedisPubSubError do
        LLevantou := True;
    end;
    // O servidor recusaria de qualquer jeito; recusar aqui rende uma mensagem
    // que diz o que fazer, em vez de um erro cru do Redis.
    TAssert.AssertTrue('comando comum e recusado antes de ir ao fio',
      LLevantou);
    // PING continua valendo: e' um dos comandos que o servidor aceita.
    TAssert.AssertTrue('PING com assinatura ativa', LSub.Ping);
  finally
    LSub.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.Resp3_ConexaoContinuaUtilizavel;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LColetor: TRedisMensagensRecebidas;
  LParams: TRedisParams;
begin
  LParams := ParamsDeTeste;
  LParams.Protocol := rpRESP3;
  LClient := TRedisClient.Create(LParams);
  LColetor := TRedisMensagensRecebidas.Create;
  LSub := LClient.CreateSubscriber;
  try
    LSub.OnMessage := LColetor.Mensagem;
    LSub.Start;
    LSub.Subscribe([Canal('resp3')]);

    // O ganho do RESP3: a mensagem vem por um tipo proprio (push), entao a
    // conexao NAO e' sequestrada e continua aceitando comando comum. Em RESP2
    // esta linha levantaria.
    LSub.Execute('SET', [Chave('resp3'), 'valor']);
    TAssert.AssertEquals('valor',
      LSub.Execute('GET', [Chave('resp3')]).AsString);

    // E as mensagens continuam chegando pela mesma conexao.
    LClient.PubSub.Publish(Canal('resp3'), 'push');
    TAssert.AssertTrue('a mensagem chegou', LColetor.Espera(1, 5000));
    TAssert.AssertEquals(Canal('resp3') + '=push', LColetor.Texto);

    LClient.Keys.Del(Chave('resp3'));
  finally
    LSub.Free;
    LColetor.Free;
    LClient.Free;
  end;
end;

procedure TRedisPubSubIntegrationTests.ConexaoDerrubada_ReconectaERefazAsAssinaturas;
var
  LClient: TRedisClient;
  LSub: TRedisSubscriber;
  LColetor: TRedisMensagensRecebidas;
  LCarrasco: TRedisConnection;
  LId: Int64;
begin
  LClient := TRedisClient.Create(ParamsDeTeste);
  LColetor := TRedisMensagensRecebidas.Create;
  LSub := LClient.CreateSubscriber;
  LCarrasco := nil;
  try
    LSub.ReconnectDelayMs := 100;
    LSub.OnMessage := LColetor.Mensagem;
    LSub.OnReconnected := LColetor.Reconectou;
    LSub.Start;
    // CLIENT ID antes de assinar: em RESP2 a conexao ainda aceita comando
    // comum, e depois do SUBSCRIBE nao aceitaria mais.
    LId := LSub.Execute('CLIENT', ['ID']).AsInteger;
    LSub.Subscribe([Canal('recon')]);

    // Derruba a conexao do assinante pelo lado do servidor — o que acontece
    // de verdade num restart ou num failover.
    LCarrasco := AbreConexao;
    LCarrasco.Execute('CLIENT', ['KILL', 'ID', LId]);

    TAssert.AssertTrue('reconectou sozinho', LColetor.EsperaReconexao(15000));
    // A unica topologia que o Redis tem para replayar sao as assinaturas — e
    // quem confirma que elas voltaram e' o SERVIDOR, nao o cliente.
    TAssert.AssertTrue('o servidor ve a assinatura de novo',
      EsperaAssinantes(LClient, Canal('recon'), 1, 15000));
    TAssert.AssertEquals('um assinante recebeu', Int64(1),
      LClient.PubSub.Publish(Canal('recon'), 'depois da queda'));
    TAssert.AssertTrue('e a mensagem chegou', LColetor.Espera(1, 5000));
  finally
    LCarrasco.Free;
    LSub.Free;
    LColetor.Free;
    LClient.Free;
  end;
end;

initialization
  RegisterTest(TRedisConnectionIntegrationTests);
  RegisterTest(TRedisPoolIntegrationTests);
  RegisterTest(TRedisClientIntegrationTests);
  RegisterTest(TRedisKeysIntegrationTests);
  RegisterTest(TRedisStringsIntegrationTests);
  RegisterTest(TRedisHashesIntegrationTests);
  RegisterTest(TRedisListsIntegrationTests);
  RegisterTest(TRedisSetsIntegrationTests);
  RegisterTest(TRedisZSetsIntegrationTests);
  RegisterTest(TRedisTransactionIntegrationTests);
  RegisterTest(TRedisPubSubIntegrationTests);
  RegisterTest(TRedisScriptingIntegrationTests);

end.
