unit Redis.IntegrationTests;

{ Testes de integracao (DUnitX). E' o mestre do espelho FPCUnit
  tests\Integration\fpc\Redis.IntegrationTests.pas, gerado daqui por
  tools/gen_fpc_mirror.py: mude este arquivo, nunca o espelho.

  PRECISAM DE UM REDIS DE VERDADE em localhost:6379 (docker/docker-compose.yml).
  Sem ele, tudo aqui falha por ERedisConnectionLost — e' o esperado, nao ha'
  como testar contra o servidor real sem o servidor real.

  O que so' da' para verificar aqui, e nao nas suites unitarias:

  - **Read timeout de verdade.** Um BLPOP mais longo que o timeout da conexao
    prova que o prazo de leitura foi mesmo aplicado no socket (o servidor falso
    nao tem socket, logo nao tem como estourar timeout de socket).
  - **Send timeout de verdade** (0.4.0, decisao T1 do transport-design.md da
    pascal-common-faa): um servidor que para de ler. O Redis nao tem como
    parar de ler sob comando (o DEBUG SLEEP vem desligado no 7), entao quem
    faz esse papel e' um listener no proprio processo que nunca aceita nem le:
    o kernel completa a conexao, os buffers enchem e a escrita espera o prazo.
    Inclui a forma do pub/sub: um leitor SEM prazo e um escritor COM prazo na
    mesma conexao, que um prazo unico por endpoint nao conseguiria dizer.
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

interface

uses
  DUnitX.TestFramework,
  Redis.DUnitXCompat,
  SysUtils,
  Classes,
  SyncObjs,
  Redis.Types,
  PascalCommon.Threading,
  PascalCommon.Tracing,
  PascalCommon.Transport,
  PascalCommon.Transport.Tcp,
  Redis.Connection,
  Redis.Pool,
  Redis.Client,
  Redis.Commands,
  Redis.Commands.Keys,
  Redis.Commands.Strings,
  Redis.Commands.Hashes,
  Redis.Commands.Lists,
  Redis.Commands.Sets,
  Redis.Commands.ZSets,
  Redis.Commands.Streams,
  Redis.Commands.Scripting,
  Redis.Commands.PubSub,
  Redis.Lock,
  PascalCommon.Cache,
  Redis.Cache,
  Redis.Transaction,
  Redis.PubSub;

type
  { Exportador que guarda os spans recebidos (fixture de tracing). }
  TRedisSpanColetorIt = class(TInterfacedObject, IPcSpanExporter)
  private
    FSpans: TPcSpanDataArray;
  public
    function ExportSpans(const ASpans: TPcSpanDataArray; out AError: string): Boolean;
    property Spans: TPcSpanDataArray read FSpans;
  end;

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


  { Fica parada num TRedisConnection.Receive (o papel da thread de leitura do
    pub/sub) e guarda a classe da excecao que a tirou de la'. }
  TRedisLeitorDuplexIt = class(TThread)
  private
    FConn: TRedisConnection;
    FClasse: string;
  protected
    procedure Execute; override;
  public
    Comecou: TEvent;
    Terminou: TEvent;
    constructor Create(AConn: TRedisConnection);
    destructor Destroy; override;
    property Classe: string read FClasse;
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

  [TestFixture]
  TRedisConnectionIntegrationTests = class
  public
    [Test] procedure Conecta_ExecutaEFecha;
    [Test] procedure Binario_EAcentuacao_SobrevivemAoRoundTrip;
    [Test] procedure Pipeline_ContraServidorReal;
    [Test] procedure Resp3_HandshakeCompleto;
    [Test] procedure BancoInexistente_FalhaNoHandshake;
    [Test] procedure ReadTimeout_LevantaERedisTimeoutEInvalida;
    [Test] procedure SendTimeout_ServidorQueParaDeLer_LevantaERedisTimeoutEInvalida;
    [Test] procedure SendTimeout_FullDuplex_LeitorSemPrazoNaoHerdaODoEscritor;
    [Test] procedure ServidorDerrubaAConexao_ViraConnectionLost;
  end;

  [TestFixture]
  TRedisPoolIntegrationTests = class
  public
    [Test] procedure Acquire_ReusaAMesmaConexao;
    [Test] procedure ConexaoQueSofreuTimeout_NaoContaminaAProxima;
    [Test] procedure ConexaoDerrubadaPeloServidor_ETrocadaNoHealthCheck;
    [Test] procedure VariasThreads_CompartilhamOPool;
  end;

  [TestFixture]
  TRedisClientIntegrationTests = class
  public
    [Test] procedure Cliente_PorPool_ExecutaEDevolveAConexao;
    [Test] procedure Cliente_FamiliasContraServidorReal;
    [Test] procedure Cliente_PipelinePelaFachada;
    [Test] procedure Cliente_ConexaoDedicada_MantemOEstadoDaConexao;
  end;

  [TestFixture]
  TRedisKeysIntegrationTests = class
  public
    [Test] procedure Expiracao_TtlPersistETtlDeChaveAusente;
    [Test] procedure TipoRenomeacaoECopia;
    [Test] procedure Scan_VarreTodasAsChavesDoPrefixo;
  end;

  [TestFixture]
  TRedisStringsIntegrationTests = class
  public
    [Test] procedure SetGet_ComBinarioEAcentuacao;
    [Test] procedure SetWithOptions_NxNaoSobrescreve_EGetDevolveOAnterior;
    [Test] procedure SetSemKeepTtl_ApagaOPrazo_ComKeepTtl_Preserva;
    [Test] procedure ContadoresInteiroEFlutuante;
    [Test] procedure MGet_ChaveAusenteVemComoNulo;
  end;

  [TestFixture]
  TRedisHashesIntegrationTests = class
  public
    [Test] procedure HashCompleto_SetGetAllScanEDel;
    [Test] procedure HGetAll_MesmaFormaEmResp2EResp3;
  end;

  [TestFixture]
  TRedisListsIntegrationTests = class
  public
    [Test] procedure Lista_PushPopRangeEMove;
    [Test] procedure BLPop_ValorJaEnfileirado_VoltaNaHora;
    [Test] procedure BLPop_PrazoMaiorQueOReceiveTimeout_NaoEstouraOSocket;
  end;

  [TestFixture]
  TRedisSetsIntegrationTests = class
  public
    [Test] procedure Conjunto_AddPertinenciaEOperacoes;
    [Test] procedure SScan_VarreOConjuntoInteiro;
  end;

  [TestFixture]
  TRedisZSetsIntegrationTests = class
  public
    [Test] procedure Ranking_AddRangeEIncr;
    [Test] procedure ZRangeWithScores_MesmoResultadoEmResp2EResp3;
    [Test] procedure ZRangeByScore_FaixaAbertaELimite;
    [Test] procedure ZPopMin_EsvaziaDoMenorParaOMaior;
  end;

  [TestFixture]
  TRedisTransactionIntegrationTests = class
  public
    [Test] procedure Transacao_RodaOBlocoInteiro;
    [Test] procedure Watch_AbortaQuandoOutraConexaoMexeNaChave;
    [Test] procedure Watch_NaoAbortaQuandoNinguemMexe;
    [Test] procedure ErroNoMeioDoBloco_NaoDesfazOsOutros;
    [Test] procedure Transacao_DevolveAConexaoAoPool;
    [Test] procedure WatchPendente_NaoContaminaAProximaTransacao;
  end;


  [TestFixture]
  TRedisPubSubIntegrationTests = class
  public
    [Test] procedure Publica_EAssinanteRecebe;
    [Test] procedure PublishSemAssinante_DevolveZeroESePerde;
    [Test] procedure Padrao_CasaVariosCanais;
    [Test] procedure PubSubChannels_EnxergaAAssinatura;
    [Test] procedure Unsubscribe_ParaDeReceber;
    [Test] procedure PayloadBinario_SobreviveAoRoundTrip;
    [Test] procedure VariosAssinantes_TodosRecebem;
    [Test] procedure Resp2_ComandoComumComAssinatura_Levanta;
    [Test] procedure Resp3_ConexaoContinuaUtilizavel;
    [Test] procedure ConexaoDerrubada_ReconectaERefazAsAssinaturas;
  end;

  [TestFixture]
  TRedisScriptingIntegrationTests = class
  public
    [Test] procedure Eval_ExecutaLuaEConverteOsTipos;
    [Test] procedure ScriptLoad_OShaDoServidorBateComOLocal;
    [Test] procedure Run_SegundaChamadaUsaOCache;
    [Test] procedure Run_AposScriptFlush_SeRecuperaSozinho;
    [Test] procedure LockDistribuido_SoQuemTemOTokenLibera;
  end;

  [TestFixture]
  TRedisStreamsIntegrationTests = class
  public
    [Test] procedure Stream_AddLenRangeEDel;
    [Test] procedure Campos_ComBinarioEAcentuacao;
    [Test] procedure XRead_LeSoOQueVeioDepoisDoId;
    [Test] procedure XReadBlocking_PrazoMaiorQueOReceiveTimeout_NaoEstouraOSocket;
    [Test] procedure XRead_MesmoResultadoEmResp2EResp3;
    [Test] procedure XTrimMaxLen_ExatoCortaOExcedente;
    [Test] procedure Grupo_EntregaConfirmaEZeraAPendencia;
    [Test] procedure Grupo_TryCreateEhIdempotente;
    [Test] procedure Grupo_DoisConsumidores_NaoRecebemAMesmaEntrada;
    [Test] procedure Grupo_ModoZero_ReleAPelDoProprioConsumidor;
    [Test] procedure PendenciaDeQuemMorreu_VoltaPorXAutoClaim;
    [Test] procedure XClaim_MinIdleAlto_NaoRoubaTrabalhoEmAndamento;
    [Test] procedure EntradaApagada_ContinuaNaPelSemCampos;
    [Test] procedure XInfoGroups_EnxergaOGrupoEAPendencia;
  end;

  [TestFixture]
  TRedisLockIntegrationTests = class
  public
    [Test] procedure Lock_UmSoTemAPosseEOTokenErradoNaoLibera;
    [Test] procedure Lock_PrazoVence_OutroAdquire;
    [Test] procedure Lock_Extend_SoODonoEstende;
    [Test] procedure Lock_RedisFora_IndisponivelNaoOcupado;
  end;

  [TestFixture]
  TRedisCacheIntegrationTests = class
  public
    [Test] procedure Cache_PutGetDelete;
    [Test] procedure Cache_PrazoVence;
    [Test] procedure Cache_TtlZeroOuNegativo_ApagaENaoGrava;
    [Test] procedure Cache_Generations_NasceDoRelogioComTtl;
    [Test] procedure Cache_Generations_LeituraNaoRenovaTtl;
    [Test] procedure Cache_Bump_AvancaEAusenteFicaAusente;
    [Test] procedure Cache_Bump_PreservaTtl;
    [Test] procedure Cache_ContadorApagado_VoltaMaior_EntradaAntigaNaoELida;
    [Test] procedure Cache_ContadorExpirado_VoltaMaior_EntradaAntigaNaoELida;
    [Test] procedure Cache_CounterTtlInvalido_Levanta;
    [Test] procedure Cache_TextoNaoAscii_IdaEVoltaByteAByte;
    [Test] procedure Cache_ErroDeTipoOuDeResposta_Sobe;
    [Test] procedure Cache_RedisFora_ViraMissEOnError;
    [Test] procedure Cache_PoolEsgotadoOuFechado_ViraIndisponivel;
    [Test] procedure Cache_Concorrente_CriacaoUnicaEBumpsContados;
  end;

  [TestFixture]
  TRedisTracingIntegrationTests = class
  public
    [Test] procedure Cliente_UmSpanPorComandoNaTraceDaRequisicao;
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

{ TRedisLeitorDuplexIt }

constructor TRedisLeitorDuplexIt.Create(AConn: TRedisConnection);
begin
  FConn := AConn;
  Comecou := TEvent.Create(nil, True, False, '');
  Terminou := TEvent.Create(nil, True, False, '');
  inherited Create(False);
end;

destructor TRedisLeitorDuplexIt.Destroy;
begin
  inherited Destroy;
  Comecou.Free;
  Terminou.Free;
end;

procedure TRedisLeitorDuplexIt.Execute;
begin
  try
    try
      Comecou.SetEvent;
      FConn.Receive;
    except
      on E: Exception do
        FClasse := E.ClassName;
    end;
  finally
    Terminou.SetEvent;
  end;
end;

{ Um servidor que nao le: escuta em 127.0.0.1, porta escolhida pelo sistema.
  O teste aceita a conexao e segura o endpoint sem nunca ler. Aceitar importa:
  no Windows, uma conexao deixada no backlog engoliu os 64 MB inteiros sem a
  escrita esperar (medido), e o prazo de escrita nunca era testado. }
function ListenerQueNaoLe: TPcListener;
var
  LOpts: TPcTcpOptions;
begin
  FillChar(LOpts, SizeOf(LOpts), 0);
  Result := PcTcpListen('127.0.0.1', 0, LOpts);
end;

{ Parametros contra o listener mudo: sem ClientName (o CLIENT SETNAME do
  handshake esperaria uma resposta que nunca vem) e em RESP2 sem senha, que
  nao emite byte nenhum no Open. }
function ParamsServidorMudo(APort: Word): TRedisParams;
begin
  Result := RedisDefaultParams;
  Result.Host := '127.0.0.1';
  Result.Port := APort;
end;

{ Manda SETs de 16 MB pela conexao, sem ler resposta (Send), ate' a escrita
  falhar, como o escritor sem fim dos testes de transporte da
  pascal-common-faa. Um valor fixo nao serve: no Windows o loopback engoliu
  64 MB sem a escrita esperar (medido), e quanto ele aceita nao e'
  documentado. Devolve a classe da excecao ('' se nunca travou) e quantos MB
  foram aceitos antes; o teto de 2 GB transforma um prazo que nao dispara em
  falha, e nao em teste pendurado. }
function EscreveAteTravar(AConn: TRedisConnection; out AMegas: Integer): string;
var
  LValor: TBytes;
  I: Integer;
begin
  Result := '';
  AMegas := 0;
  LValor := nil;
  SetLength(LValor, 16 * 1024 * 1024);
  try
    for I := 1 to 128 do
    begin
      AConn.Send('SET', [Chave('nao-cabe'), LValor]);
      Inc(AMegas, 16);
    end;
  except
    on E: Exception do
      Result := E.ClassName;
  end;
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
    LInicio := PcTickMs;
    LClasse := '';
    try
      LConn.Execute('BLPOP', [Chave('fila-que-nao-existe'), 2]);
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    LDecorridoMs := PcTickMs - LInicio;

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

procedure TRedisConnectionIntegrationTests.SendTimeout_ServidorQueParaDeLer_LevantaERedisTimeoutEInvalida;
var
  LListener: TPcListener;
  LServidor: TPcEndpoint;
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LClasse: string;
  LMegas: Integer;
begin
  // O servidor para de ler: os buffers enchem e a escrita fica esperando
  // espaco. Sem prazo de escrita, esta thread ficaria parada para sempre,
  // segurando o lock da conexao (e, pelo pool, a vaga dela).
  LListener := ListenerQueNaoLe;
  try
    LParams := ParamsServidorMudo(LListener.Port);
    LParams.SendTimeoutMs := 300;
    LServidor := nil;
    LConn := TRedisConnection.Create(LParams);
    try
      LConn.Open;
      LServidor := LListener.Accept;   // e nunca le
      LClasse := EscreveAteTravar(LConn, LMegas);
      TAssert.AssertEquals('depois de ' + IntToStr(LMegas) + ' MB aceitos',
        'ERedisTimeout', LClasse);
      // Metade do comando foi para o fio: a conexao nao tem conserto, e o
      // proximo comando diz isso em vez de tentar.
      TAssert.AssertTrue('devia estar invalidada', LConn.IsBroken);
      LClasse := '';
      try
        LConn.Execute('PING');
      except
        on E: Exception do
          LClasse := E.ClassName;
      end;
      TAssert.AssertEquals('ERedisConnectionLost', LClasse);
    finally
      LConn.Free;
      LServidor.Free;
    end;
  finally
    LListener.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.SendTimeout_FullDuplex_LeitorSemPrazoNaoHerdaODoEscritor;
var
  LListener: TPcListener;
  LServidor: TPcEndpoint;
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LLeitor: TRedisLeitorDuplexIt;
  LClasse: string;
  LMegas: Integer;
begin
  // A forma do assinante de pub/sub: uma thread parada no Receive, sem prazo
  // (silencio e' o estado normal de um canal), e outra mandando um comando
  // que o servidor nao le. O escritor tem de desistir pelo prazo DELE; o
  // leitor nao pode ter desistido antes por um prazo que nao e' o dele. So'
  // depois, quando o escritor derruba a conexao, o leitor sai — como queda.
  LListener := ListenerQueNaoLe;
  try
    LParams := ParamsServidorMudo(LListener.Port);
    LParams.ReceiveTimeoutMs := 0;
    LParams.SendTimeoutMs := 300;
    LServidor := nil;
    LConn := TRedisConnection.Create(LParams);
    try
      LConn.Open;
      LServidor := LListener.Accept;   // e nunca le nem escreve
      LLeitor := TRedisLeitorDuplexIt.Create(LConn);
      try
        LLeitor.Comecou.WaitFor(10000);
        LClasse := EscreveAteTravar(LConn, LMegas);
        if LLeitor.Terminou.WaitFor(10000) <> wrSignaled then
          LConn.Abort;
        LLeitor.WaitFor;
        // Com um prazo so' para as duas direcoes, o leitor estouraria junto
        // (ERedisTimeout nele) e, derrubando a conexao, faria o escritor sair
        // como queda: as duas classes abaixo trocariam.
        TAssert.AssertEquals('o escritor, depois de ' + IntToStr(LMegas) +
          ' MB aceitos', 'ERedisTimeout', LClasse);
        TAssert.AssertEquals('o leitor', 'ERedisConnectionLost', LLeitor.Classe);
      finally
        LLeitor.Free;
      end;
    finally
      LConn.Free;
      LServidor.Free;
    end;
  finally
    LListener.Free;
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

    LInicio := PcTickMs;
    TAssert.AssertTrue('achou',
      LClient.Lists.BLPop([Chave('l:b1'), Chave('l:b2')], 5, LChave, LValor));
    // Com valor disponivel o BLPOP nem chega a bloquear.
    TAssert.AssertTrue('voltou na hora', PcTickMs - LInicio < 2000);
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

    LInicio := PcTickMs;
    TAssert.AssertFalse('nada chegou',
      LClient.Lists.BLPop([Chave('l:vazia')], 2, LChave, LValor));
    LGasto := PcTickMs - LInicio;

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

{ --- 0.3.0: TRedisLock --- }

procedure TRedisLockIntegrationTests.Lock_UmSoTemAPosseEOTokenErradoNaoLibera;
var
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken, LOutro: string;
  LPttl: Int64;
begin
  LClient := NovoCliente;
  LLock := TRedisLock.Create(LClient, PREFIXO);
  try
    LimpaChaves(LClient, ['lock:a']);
    TAssert.AssertTrue('o primeiro adquire', LLock.Acquire('lock:a', 30000, LToken) = rlAcquired);
    TAssert.AssertTrue('o segundo encontra ocupado', LLock.Acquire('lock:a', 30000, LOutro) = rlBusy);
    LPttl := LClient.Keys.PTtl(Chave('lock:a'));
    TAssert.AssertTrue('o PX foi aplicado', (LPttl > 0) and (LPttl <= 30000));
    TAssert.AssertEquals(LToken, LClient.Strings.GetString(Chave('lock:a')));
    TAssert.AssertFalse('outro token nao libera', LLock.Release('lock:a', 'nao-e-meu'));
    TAssert.AssertTrue('o lock continua', LClient.Keys.Exists(Chave('lock:a')));
    TAssert.AssertTrue('o dono libera', LLock.Release('lock:a', LToken));
    TAssert.AssertFalse('o lock sumiu', LClient.Keys.Exists(Chave('lock:a')));
    TAssert.AssertFalse('liberar de novo: ja nao e seu', LLock.Release('lock:a', LToken));
  finally
    LimpaChaves(LClient, ['lock:a']);
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockIntegrationTests.Lock_PrazoVence_OutroAdquire;
var
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken, LOutro: string;
  LInicio: Int64;
  LPegou: Boolean;
begin
  LClient := NovoCliente;
  LLock := TRedisLock.Create(LClient, PREFIXO);
  try
    LimpaChaves(LClient, ['lock:b']);
    TAssert.AssertTrue(LLock.TryAcquire('lock:b', 150, LToken));
    // O dono "morreu" sem liberar: o PX devolve a chave. Espera por condicao,
    // com prazo, nunca um Sleep fixo do tamanho do PX.
    LPegou := False;
    LInicio := PcTickMs;
    while not LPegou and (PcTickMs - LInicio < 3000) do
    begin
      LPegou := LLock.TryAcquire('lock:b', 30000, LOutro);
      if not LPegou then
        Sleep(20);
    end;
    TAssert.AssertTrue('o PX venceu e outro adquiriu', LPegou);
    // O dono antigo nao apaga o lock do novo.
    TAssert.AssertFalse(LLock.Release('lock:b', LToken));
    TAssert.AssertEquals(LOutro, LClient.Strings.GetString(Chave('lock:b')));
  finally
    LimpaChaves(LClient, ['lock:b']);
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockIntegrationTests.Lock_Extend_SoODonoEstende;
var
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken: string;
begin
  LClient := NovoCliente;
  LLock := TRedisLock.Create(LClient, PREFIXO);
  try
    LimpaChaves(LClient, ['lock:c']);
    TAssert.AssertTrue(LLock.TryAcquire('lock:c', 1000, LToken));
    TAssert.AssertFalse('outro token nao estende', LLock.Extend('lock:c', 'nao-e-meu', 60000));
    TAssert.AssertTrue('o PX nao mudou', LClient.Keys.PTtl(Chave('lock:c')) <= 1000);
    TAssert.AssertTrue('o dono estende', LLock.Extend('lock:c', LToken, 60000));
    TAssert.AssertTrue('o novo PX', LClient.Keys.PTtl(Chave('lock:c')) > 1000);
  finally
    LimpaChaves(LClient, ['lock:c']);
    LLock.Free;
    LClient.Free;
  end;
end;

type
  TRedisLockAvisosIt = class
  public
    Total: Integer;
    Classe: string;
    procedure Aviso(ASender: TObject; const AOperation, AKey: string; AError: Exception);
  end;

procedure TRedisLockAvisosIt.Aviso(ASender: TObject; const AOperation, AKey: string;
  AError: Exception);
begin
  Inc(Total);
  Classe := AError.ClassName;
end;

procedure TRedisLockIntegrationTests.Lock_RedisFora_IndisponivelNaoOcupado;
var
  LParams: TRedisParams;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LAvisos: TRedisLockAvisosIt;
  LToken: string;
begin
  // Uma porta onde nao ha' Redis: o lock diz "indisponivel", nunca "ocupado",
  // e quem decide seguir sem ele e' a aplicacao.
  LParams := ParamsDeTeste;
  LParams.Port := 1;
  LParams.ConnectTimeoutMs := 500;
  LClient := TRedisClient.Create(LParams);
  LLock := TRedisLock.Create(LClient, PREFIXO);
  LAvisos := TRedisLockAvisosIt.Create;
  try
    LLock.OnUnavailable := LAvisos.Aviso;
    TAssert.AssertTrue(LLock.Acquire('lock:d', 1000, LToken) = rlUnavailable);
    TAssert.AssertEquals('', LToken);
    TAssert.AssertFalse(LLock.Release('lock:d', 'qualquer'));
    TAssert.AssertEquals(2, LAvisos.Total);
    TAssert.AssertEquals('ERedisConnectionLost', LAvisos.Classe);
  finally
    LAvisos.Free;
    LLock.Free;
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
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  repeat
    if Total >= ACount then
      Exit(True);
    Sleep(5);
  until PcTickMs >= LDeadline;
  Result := False;
end;

function TRedisMensagensRecebidas.EsperaReconexao(ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
  LTem: Integer;
begin
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
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
  until PcTickMs >= LDeadline;
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
  LDeadline := PcTickMs + UInt64(ATimeoutMs);
  repeat
    LContagem := AClient.PubSub.CountSubscribers([ACanal]);
    if (Length(LContagem) = 1) and (LContagem[0].Subscribers >= ACount) then
      Exit(True);
    Sleep(10);
  until PcTickMs >= LDeadline;
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

{ TRedisStreamsIntegrationTests }

// Cria (ou recria) um stream limpo com um grupo, e devolve os ids gravados.
// Apagar a chave antes e' o que torna cada teste independente do anterior:
// XADD nunca sobrescreve, so' acrescenta.
function PreparaGrupo(AClient: TRedisClient; const ASufixo, AGrupo: string;
  const AMensagens: array of string): TRedisStringArray;
var
  I: Integer;
begin
  LimpaChaves(AClient, [ASufixo]);
  AClient.Streams.XGroupTryCreate(Chave(ASufixo), AGrupo, '0', True);
  SetLength(Result, Length(AMensagens));
  for I := 0 to High(AMensagens) do
    Result[I] := AClient.Streams.XAdd(Chave(ASufixo), ['t', AMensagens[I]]);
end;

procedure TRedisStreamsIntegrationTests.Stream_AddLenRangeEDel;
var
  LClient: TRedisClient;
  LId1, LId2: string;
  LEntradas: TRedisStreamEntryArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['x:log']);

    LId1 := LClient.Streams.XAdd(Chave('x:log'), ['nivel', 'info', 'msg', 'um']);
    LId2 := LClient.Streams.XAdd(Chave('x:log'), ['nivel', 'erro', 'msg', 'dois']);
    // Id gerado pelo servidor: sempre crescente, e e' isso que da' a ordem
    // total do log.
    TAssert.AssertTrue('o segundo id e maior', LId2 > LId1);
    TAssert.AssertEquals(Int64(2), LClient.Streams.XLen(Chave('x:log')));

    LEntradas := LClient.Streams.XRange(Chave('x:log'),
      REDIS_STREAM_MIN_ID, REDIS_STREAM_MAX_ID);
    TAssert.AssertEquals(2, Length(LEntradas));
    TAssert.AssertEquals(LId1, LEntradas[0].Id);
    TAssert.AssertEquals('info', LEntradas[0].FieldValue('nivel'));
    TAssert.AssertEquals('dois', LEntradas[1].FieldValue('msg'));
    TAssert.AssertEquals(2, LEntradas[1].FieldCount);

    // XREVRANGE espera (fim, inicio) — a ordem inversa da do XRANGE.
    LEntradas := LClient.Streams.XRevRange(Chave('x:log'),
      REDIS_STREAM_MAX_ID, REDIS_STREAM_MIN_ID, 1);
    TAssert.AssertEquals(1, Length(LEntradas));
    TAssert.AssertEquals(LId2, LEntradas[0].Id);

    // Extremo aberto: pagina sem repetir a ultima entrada lida.
    LEntradas := LClient.Streams.XRange(Chave('x:log'),
      RedisStreamIdExclusive(LId1), REDIS_STREAM_MAX_ID);
    TAssert.AssertEquals(1, Length(LEntradas));
    TAssert.AssertEquals(LId2, LEntradas[0].Id);

    TAssert.AssertEquals(Int64(1), LClient.Streams.XDel(Chave('x:log'), [LId1]));
    TAssert.AssertEquals(Int64(1), LClient.Streams.XLen(Chave('x:log')));

    LimpaChaves(LClient, ['x:log']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.Campos_ComBinarioEAcentuacao;
var
  LClient: TRedisClient;
  LBin: TBytes;
  LEntradas: TRedisStreamEntryArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['x:bin']);
    // CRLF no meio: se o codec medisse a bulk string pelo delimitador em vez
    // do comprimento, o valor voltaria cortado aqui.
    LBin := MakeBytes([0, 13, 10, 255, 65, 0]);

    LClient.Streams.XAdd(Chave('x:bin'), ['blob', LBin, 'texto', 'coração']);
    LEntradas := LClient.Streams.XRange(Chave('x:bin'), '-', '+');
    TAssert.AssertEquals(1, Length(LEntradas));
    TAssert.AssertEquals(Hex(LBin), Hex(LEntradas[0].FieldBytes('blob')));
    TAssert.AssertEquals('coração', LEntradas[0].FieldValue('texto'));

    LimpaChaves(LClient, ['x:bin']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.XRead_LeSoOQueVeioDepoisDoId;
var
  LClient: TRedisClient;
  LId1: string;
  LDados: TRedisStreamDataArray;
  LIndice: Integer;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['x:read', 'x:outro']);
    LId1 := LClient.Streams.XAdd(Chave('x:read'), ['t', 'um']);
    LClient.Streams.XAdd(Chave('x:read'), ['t', 'dois']);

    // Do comeco: as duas.
    LDados := LClient.Streams.XRead([Chave('x:read')], ['0-0']);
    TAssert.AssertEquals(1, Length(LDados));
    TAssert.AssertEquals(2, Length(LDados[0].Entries));

    // Depois do primeiro id: so' a segunda. O id e' EXCLUSIVO no XREAD.
    LDados := LClient.Streams.XRead([Chave('x:read')], [LId1]);
    TAssert.AssertEquals(1, Length(LDados[0].Entries));
    TAssert.AssertEquals('dois', LDados[0].Entries[0].FieldValue('t'));

    // Duas chaves, uma sem novidade: a que nao tem nada NAO aparece na
    // resposta. Procurar pelo nome e' o certo; indexar pela posicao da
    // chamada leria a chave errada.
    LClient.Streams.XAdd(Chave('x:outro'), ['t', 'tres']);
    LDados := LClient.Streams.XRead([Chave('x:read'), Chave('x:outro')],
      [REDIS_STREAM_LAST, '0-0']);
    LIndice := RedisFindStreamData(LDados, Chave('x:outro'));
    TAssert.AssertTrue('a chave com novidade veio', LIndice >= 0);
    TAssert.AssertEquals(-1, RedisFindStreamData(LDados, Chave('x:read')));
    TAssert.AssertEquals('tres', LDados[LIndice].Entries[0].FieldValue('t'));

    LimpaChaves(LClient, ['x:read', 'x:outro']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.XReadBlocking_PrazoMaiorQueOReceiveTimeout_NaoEstouraOSocket;
var
  LParams: TRedisParams;
  LClient: TRedisClient;
  LInicio, LGasto: UInt64;
  LDados: TRedisStreamDataArray;
begin
  // Read timeout de 1 s e BLOCK de 2 s: se o bloqueante saisse pelo pool
  // comum, o socket desistiria no meio e levantaria ERedisTimeout com a
  // resposta ainda a caminho. Mesmo cenario do BLPOP no M4 — aqui vale
  // repetir porque a unidade e' outra (milissegundos, nao segundos), e um erro
  // de fator 1000 passaria despercebido num teste de fio.
  LParams := ParamsDeTeste;
  LParams.ReceiveTimeoutMs := 1000;
  LClient := TRedisClient.Create(LParams);
  try
    LimpaChaves(LClient, ['x:vazio']);
    LClient.Streams.XAdd(Chave('x:vazio'), ['t', 'marco']);

    LInicio := PcTickMs;
    LDados := LClient.Streams.XReadBlocking([Chave('x:vazio')],
      [REDIS_STREAM_LAST], 2000);
    LGasto := PcTickMs - LInicio;

    // Prazo vencido sem novidade e' o caso NORMAL de um leitor ocioso.
    TAssert.AssertEquals(0, Length(LDados));
    TAssert.AssertTrue('esperou os 2 s do comando', LGasto >= 1800);
    TAssert.AssertTrue('e nao esperou demais', LGasto < 6000);

    // E o pool comum continua servindo: o bloqueante nao passou por ele.
    LClient.Strings.SetValue(Chave('x:apos'), 'ok');
    TAssert.AssertEquals('ok', LClient.Strings.GetString(Chave('x:apos')));

    LimpaChaves(LClient, ['x:vazio', 'x:apos']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.XRead_MesmoResultadoEmResp2EResp3;
var
  LClient, LResp3: TRedisClient;
  LDados: TRedisStreamDataArray;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['x:proto']);
    LClient.Streams.XAdd(Chave('x:proto'), ['t', 'um']);

    // Em RESP2 o XREAD responde uma lista de pares; em RESP3, um mapa. A
    // fachada devolve a MESMA estrutura nos dois — sem isso a aplicacao teria
    // de ramificar por protocolo.
    LDados := LClient.Streams.XRead([Chave('x:proto')], ['0-0']);
    TAssert.AssertEquals(1, Length(LDados));
    TAssert.AssertEquals(Chave('x:proto'), LDados[0].Key);
    TAssert.AssertEquals('um', LDados[0].Entries[0].FieldValue('t'));

    LResp3 := NovoClienteResp3;
    try
      LDados := LResp3.Streams.XRead([Chave('x:proto')], ['0-0']);
      TAssert.AssertEquals(1, Length(LDados));
      TAssert.AssertEquals(Chave('x:proto'), LDados[0].Key);
      TAssert.AssertEquals('um', LDados[0].Entries[0].FieldValue('t'));
    finally
      LResp3.Free;
    end;

    LimpaChaves(LClient, ['x:proto']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.XTrimMaxLen_ExatoCortaOExcedente;
var
  LClient: TRedisClient;
  I: Integer;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['x:trim']);
    for I := 1 to 10 do
      LClient.Streams.XAdd(Chave('x:trim'), ['n', IntToStr(I)]);

    // Exato ('=') e' o unico que da' para afirmar num teste: com '~' o
    // servidor para no limite do no' interno e o tamanho fica "mil e pouco".
    TAssert.AssertEquals(Int64(7),
      LClient.Streams.XTrimMaxLen(Chave('x:trim'), 3, False));
    TAssert.AssertEquals(Int64(3), LClient.Streams.XLen(Chave('x:trim')));
    // O que sobra e' o FIM do log: stream corta pelo comeco.
    TAssert.AssertEquals('8',
      LClient.Streams.XRange(Chave('x:trim'), '-', '+')[0].FieldValue('n'));

    LimpaChaves(LClient, ['x:trim']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.Grupo_EntregaConfirmaEZeraAPendencia;
var
  LClient: TRedisClient;
  LIds: TRedisStringArray;
  LDados: TRedisStreamDataArray;
  LResumo: TRedisPendingSummary;
begin
  LClient := NovoCliente;
  try
    LIds := PreparaGrupo(LClient, 'x:g1', 'grupo', ['um', 'dois']);

    LDados := LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g1')],
      [REDIS_STREAM_NEW]);
    TAssert.AssertEquals(1, Length(LDados));
    TAssert.AssertEquals(2, Length(LDados[0].Entries));
    TAssert.AssertEquals('um', LDados[0].Entries[0].FieldValue('t'));

    // Entregue e nao confirmado: as duas estao na PEL. E' o que separa stream
    // de pub/sub — o servidor LEMBRA quem recebeu o que.
    LResumo := LClient.Streams.XPendingSummary(Chave('x:g1'), 'grupo');
    TAssert.AssertEquals(Int64(2), LResumo.Count);
    TAssert.AssertEquals(LIds[0], LResumo.MinId);
    TAssert.AssertEquals(LIds[1], LResumo.MaxId);
    TAssert.AssertEquals(1, Length(LResumo.Consumers));
    TAssert.AssertEquals('w1', LResumo.Consumers[0].Name);
    TAssert.AssertEquals(Int64(2), LResumo.Consumers[0].Count);

    TAssert.AssertEquals(Int64(2),
      LClient.Streams.XAck(Chave('x:g1'), 'grupo', [LIds[0], LIds[1]]));
    LResumo := LClient.Streams.XPendingSummary(Chave('x:g1'), 'grupo');
    TAssert.AssertEquals(Int64(0), LResumo.Count);
    TAssert.AssertEquals('', LResumo.MinId);

    // Confirmar de novo nao e' erro: o segundo XACK so' nao encontra nada.
    TAssert.AssertEquals(Int64(0),
      LClient.Streams.XAck(Chave('x:g1'), 'grupo', [LIds[0]]));

    LimpaChaves(LClient, ['x:g1']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.Grupo_TryCreateEhIdempotente;
var
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LClient := NovoCliente;
  try
    LimpaChaves(LClient, ['x:g2']);

    // MKSTREAM: sem ele, criar grupo num stream que ainda nao recebeu nada
    // falha — e e' exatamente o que acontece quando o consumidor sobe antes
    // do produtor.
    TAssert.AssertTrue('criou agora',
      LClient.Streams.XGroupTryCreate(Chave('x:g2'), 'grupo', '0', True));
    TAssert.AssertFalse('ja existia',
      LClient.Streams.XGroupTryCreate(Chave('x:g2'), 'grupo', '0', True));

    // A versao que nao perdoa continua levantando: quem quer saber, sabe.
    LLevantou := False;
    try
      LClient.Streams.XGroupCreate(Chave('x:g2'), 'grupo', '0');
    except
      on E: ERedisReplyError do
        LLevantou := E.Code = 'BUSYGROUP';
    end;
    TAssert.AssertTrue('XGroupCreate levanta BUSYGROUP', LLevantou);

    TAssert.AssertTrue('destruiu',
      LClient.Streams.XGroupDestroy(Chave('x:g2'), 'grupo'));
    TAssert.AssertFalse('ja nao existe',
      LClient.Streams.XGroupDestroy(Chave('x:g2'), 'grupo'));

    LimpaChaves(LClient, ['x:g2']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.Grupo_DoisConsumidores_NaoRecebemAMesmaEntrada;
var
  LClient: TRedisClient;
  LPrimeiro, LSegundo: TRedisStreamDataArray;
begin
  LClient := NovoCliente;
  try
    PreparaGrupo(LClient, 'x:g3', 'grupo', ['um', 'dois', 'tres']);

    // O grupo REPARTE o trabalho: cada entrada vai para um consumidor so'.
    // E' a diferenca para o pub/sub, em que todo assinante recebe tudo.
    LPrimeiro := LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g3')],
      [REDIS_STREAM_NEW], 2);
    LSegundo := LClient.Streams.XReadGroup('grupo', 'w2', [Chave('x:g3')],
      [REDIS_STREAM_NEW], 2);

    TAssert.AssertEquals(2, Length(LPrimeiro[0].Entries));
    TAssert.AssertEquals(1, Length(LSegundo[0].Entries));
    TAssert.AssertEquals('tres', LSegundo[0].Entries[0].FieldValue('t'));
    // Nada sobrou: o terceiro pedido volta vazio.
    TAssert.AssertEquals(0, Length(LClient.Streams.XReadGroup('grupo', 'w1',
      [Chave('x:g3')], [REDIS_STREAM_NEW])));

    LimpaChaves(LClient, ['x:g3']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.Grupo_ModoZero_ReleAPelDoProprioConsumidor;
var
  LClient: TRedisClient;
  LIds: TRedisStringArray;
  LDados: TRedisStreamDataArray;
begin
  LClient := NovoCliente;
  try
    LIds := PreparaGrupo(LClient, 'x:g4', 'grupo', ['um', 'dois']);
    LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g4')],
      [REDIS_STREAM_NEW]);
    LClient.Streams.XAck(Chave('x:g4'), 'grupo', [LIds[0]]);

    // '0' rele' a PEL DESTE consumidor: e' como um worker retoma o proprio
    // trabalho depois de reiniciar. So' o que nao foi confirmado volta.
    LDados := LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g4')],
      [REDIS_STREAM_PENDING]);
    TAssert.AssertEquals(1, Length(LDados));
    TAssert.AssertEquals(1, Length(LDados[0].Entries));
    TAssert.AssertEquals(LIds[1], LDados[0].Entries[0].Id);

    // E a PEL de OUTRO consumidor esta' vazia: a pendencia tem dono.
    LDados := LClient.Streams.XReadGroup('grupo', 'w2', [Chave('x:g4')],
      [REDIS_STREAM_PENDING]);
    TAssert.AssertEquals(0, Length(LDados[0].Entries));

    LimpaChaves(LClient, ['x:g4']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.PendenciaDeQuemMorreu_VoltaPorXAutoClaim;
var
  LClient: TRedisClient;
  LIds: TRedisStringArray;
  LEntradas: TRedisStreamEntryArray;
  LProximo: string;
  LPendencias: TRedisPendingEntryArray;
begin
  LClient := NovoCliente;
  try
    LIds := PreparaGrupo(LClient, 'x:g5', 'grupo', ['um', 'dois']);
    // w1 recebe e "morre" sem confirmar.
    LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g5')],
      [REDIS_STREAM_NEW]);

    // w2 recolhe o trabalho abandonado. Minimo de ociosidade 0 porque o teste
    // nao tem tempo a perder; em producao e' na casa dos minutos.
    LEntradas := LClient.Streams.XAutoClaim(Chave('x:g5'), 'grupo', 'w2', 0,
      '0-0', 10, LProximo);
    TAssert.AssertEquals(2, Length(LEntradas));
    TAssert.AssertEquals(LIds[0], LEntradas[0].Id);
    TAssert.AssertEquals('um', LEntradas[0].FieldValue('t'));
    // Cursor com a mecanica do SCAN: '0-0' significa que a varredura acabou.
    TAssert.AssertEquals('0-0', LProximo);

    // A pendencia trocou de dono — nao sumiu. Continua sendo obrigacao de
    // alguem ate' o XACK.
    LPendencias := LClient.Streams.XPendingRange(Chave('x:g5'), 'grupo',
      '-', '+', 10);
    TAssert.AssertEquals(2, Length(LPendencias));
    TAssert.AssertEquals('w2', LPendencias[0].Consumer);
    TAssert.AssertTrue('ja foi entregue mais de uma vez',
      LPendencias[0].DeliveryCount >= 2);

    // Filtrar por consumidor: w1 ja' nao tem nada.
    TAssert.AssertEquals(0, Length(LClient.Streams.XPendingRange(
      Chave('x:g5'), 'grupo', '-', '+', 10, 'w1')));

    LimpaChaves(LClient, ['x:g5']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.XClaim_MinIdleAlto_NaoRoubaTrabalhoEmAndamento;
var
  LClient: TRedisClient;
  LIds: TRedisStringArray;
  LEntradas: TRedisStreamEntryArray;
begin
  LClient := NovoCliente;
  try
    LIds := PreparaGrupo(LClient, 'x:g6', 'grupo', ['um']);
    LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g6')],
      [REDIS_STREAM_NEW]);

    // A entrada acabou de ser entregue: um XCLAIM de 10 minutos NAO a leva.
    // E' a protecao contra dois workers processando a mesma coisa.
    LEntradas := LClient.Streams.XClaim(Chave('x:g6'), 'grupo', 'w2', 600000,
      [LIds[0]]);
    TAssert.AssertEquals(0, Length(LEntradas));

    // Com minimo zero, leva.
    LEntradas := LClient.Streams.XClaim(Chave('x:g6'), 'grupo', 'w2', 0,
      [LIds[0]]);
    TAssert.AssertEquals(1, Length(LEntradas));
    TAssert.AssertEquals(LIds[0], LEntradas[0].Id);

    // JUSTID nao traz os campos — e nao conta como nova entrega.
    TAssert.AssertEquals(1, Length(LClient.Streams.XClaimJustId(
      Chave('x:g6'), 'grupo', 'w1', 0, [LIds[0]])));

    LimpaChaves(LClient, ['x:g6']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.EntradaApagada_ContinuaNaPelSemCampos;
var
  LClient: TRedisClient;
  LIds: TRedisStringArray;
  LDados: TRedisStreamDataArray;
begin
  LClient := NovoCliente;
  try
    LIds := PreparaGrupo(LClient, 'x:g7', 'grupo', ['um']);
    LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g7')],
      [REDIS_STREAM_NEW]);

    // XDEL tira do STREAM, nao da PEL: a pendencia continua, e o que volta e'
    // um id sem campos. E' o caso que faria um leitor ingenuo levantar.
    LClient.Streams.XDel(Chave('x:g7'), [LIds[0]]);
    LDados := LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g7')],
      [REDIS_STREAM_PENDING]);
    TAssert.AssertEquals(1, Length(LDados[0].Entries));
    TAssert.AssertEquals(LIds[0], LDados[0].Entries[0].Id);
    TAssert.AssertTrue('sem campos', LDados[0].Entries[0].IsDeleted);
    TAssert.AssertEquals(0, LDados[0].Entries[0].FieldCount);

    // Quem tira da PEL e' o XACK, e ele funciona mesmo com a entrada apagada.
    TAssert.AssertEquals(Int64(1),
      LClient.Streams.XAck(Chave('x:g7'), 'grupo', [LIds[0]]));

    LimpaChaves(LClient, ['x:g7']);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsIntegrationTests.XInfoGroups_EnxergaOGrupoEAPendencia;
var
  LClient: TRedisClient;
  LInfo: IRedisReply;
begin
  LClient := NovoCliente;
  try
    PreparaGrupo(LClient, 'x:g8', 'grupo', ['um']);
    LClient.Streams.XReadGroup('grupo', 'w1', [Chave('x:g8')],
      [REDIS_STREAM_NEW]);

    LInfo := LClient.Streams.XInfoStream(Chave('x:g8'));
    TAssert.AssertEquals(Int64(1), LInfo.ValueByKey('length').AsInteger);

    // XINFO GROUPS devolve uma entrada por grupo, cada uma um mapa achatado —
    // a mesma forma em RESP2 e RESP3.
    LInfo := LClient.Streams.XInfoGroups(Chave('x:g8'));
    TAssert.AssertEquals(1, LInfo.Count);
    TAssert.AssertEquals('grupo', LInfo[0].ValueByKey('name').AsString);
    TAssert.AssertEquals(Int64(1), LInfo[0].ValueByKey('pending').AsInteger);

    LInfo := LClient.Streams.XInfoConsumers(Chave('x:g8'), 'grupo');
    TAssert.AssertEquals(1, LInfo.Count);
    TAssert.AssertEquals('w1', LInfo[0].ValueByKey('name').AsString);

    LimpaChaves(LClient, ['x:g8']);
  finally
    LClient.Free;
  end;
end;

{ --- Redis.Cache: o IPcCacheStore da pascal-common-faa sobre o Redis --- }

const
  // Nome do TPcCache dos testes: so' [A-Za-z0-9_.-], entao nao cabe o
  // PREFIXO (que tem ':'). As chaves ficam '<nome>:v:...' e '<nome>:tag:...'.
  NOME_CACHE_IT = 'pascal-redis-faa.it';

type
  { Conta os avisos do OnError do TPcCache e serve de loader do GetOrLoad. }
  TRedisCacheAvisosIt = class
  public
    Total: Integer;
    Classe: string;
    Cargas: Integer;
    procedure Aviso(const ACacheName: string; AError: Exception);
    function Carrega(const AArg: string): string;
  end;

  { Uma das threads do teste concorrente: le os contadores novos (todas
    correndo para cria-los ao mesmo tempo) e depois alterna Bump e leitura do
    contador compartilhado, conferindo que ele so' anda para a frente. }
  TRedisCacheCorridaIt = class(TThread)
  private
    FStore: IPcCacheStore;
    FTags: array of string;
    FCompartilhado: string;
    FRodadas: Integer;
    FLargada: TEvent;
  protected
    procedure Execute; override;
  public
    Valores: array of Int64;
    Erro: string;
    constructor Create(const AStore: IPcCacheStore; const ATags: array of string;
      const ACompartilhado: string; ARodadas: Integer; ALargada: TEvent);
  end;

procedure TRedisCacheAvisosIt.Aviso(const ACacheName: string; AError: Exception);
begin
  Inc(Total);
  Classe := AError.ClassName;
end;

function TRedisCacheAvisosIt.Carrega(const AArg: string): string;
begin
  Inc(Cargas);
  Result := 'carregado:' + AArg;
end;

constructor TRedisCacheCorridaIt.Create(const AStore: IPcCacheStore;
  const ATags: array of string; const ACompartilhado: string; ARodadas: Integer;
  ALargada: TEvent);
var
  I: Integer;
begin
  FStore := AStore;
  SetLength(FTags, Length(ATags));
  for I := 0 to High(ATags) do
    FTags[I] := ATags[I];
  FCompartilhado := ACompartilhado;
  FRodadas := ARodadas;
  FLargada := ALargada;
  SetLength(Valores, Length(ATags));
  inherited Create(False);
end;

procedure TRedisCacheCorridaIt.Execute;
var
  I: Integer;
  LAnterior, LAtual: Int64;
begin
  try
    if FLargada.WaitFor(10000) <> wrSignaled then
    begin
      Erro := 'a largada nao veio';
      Exit;
    end;
    for I := 0 to High(FTags) do
      Valores[I] := FStore.Generations([FTags[I]])[0];
    LAnterior := FStore.Generations([FCompartilhado])[0];
    for I := 1 to FRodadas do
    begin
      FStore.Bump([FCompartilhado]);
      LAtual := FStore.Generations([FCompartilhado])[0];
      // O proprio Bump veio depois da leitura anterior: tem de ter andado.
      if LAtual <= LAnterior then
      begin
        Erro := Format('o contador voltou ou parou: %d depois de %d', [LAtual, LAnterior]);
        Exit;
      end;
      LAnterior := LAtual;
    end;
  except
    on E: Exception do
      Erro := E.ClassName + ': ' + E.Message;
  end;
end;

// Classe da excecao que uma operacao do store levanta ('' se nao levantou).
// AOperacao: 0 TryGet, 1 Put, 2 Delete, 3 Generations, 4 Bump.
function FalhaDoStore(const AStore: IPcCacheStore; AOperacao: Integer;
  const AKey: string): string;
var
  LValor: string;
begin
  Result := '';
  try
    case AOperacao of
      0: AStore.TryGet(AKey, LValor);
      1: AStore.Put(AKey, 'v', 1000);
      2: AStore.Delete(AKey);
      3: AStore.Generations([AKey]);
      4: AStore.Bump([AKey]);
    end;
  except
    on E: Exception do
      Result := E.ClassName;
  end;
end;

// Espera o relogio de RedisUnixTimeMs passar de AValor, com prazo: e' a
// condicao do C4 para um contador renascido ser maior que o antigo.
procedure EsperaRelogioPassar(AValor: Int64);
var
  LPrazo: UInt64;
begin
  LPrazo := PcTickMs + 5000;
  while (RedisUnixTimeMs <= AValor) and (PcTickMs < LPrazo) do
    Sleep(1);
end;

// PTTL de uma chave no servidor: -2 ausente, -1 sem prazo.
function PttlDe(AConn: TRedisConnection; const AKey: string): Int64;
begin
  Result := AConn.Execute('PTTL', [AKey]).AsInteger;
end;

// Espera a chave sumir do servidor (o TTL dela vencer), com prazo de 5 s.
// True se sumiu.
function EsperaChaveSumir(AConn: TRedisConnection; const AKey: string): Boolean;
var
  LPrazo: UInt64;
begin
  LPrazo := PcTickMs + 5000;
  repeat
    Result := AConn.Execute('EXISTS', [AKey]).AsInteger = 0;
    if not Result then
      Sleep(10);
  until Result or (PcTickMs >= LPrazo);
end;

// True se criar o store com esse TTL de contador levanta EArgumentException.
// AComCliente: a sobrecarga com TRedisClient, senao a com TRedisPool.
function CounterTtlLevanta(APool: TRedisPool; AClient: TRedisClient;
  AComCliente: Boolean; ATtlMs: Int64): Boolean;
var
  LStore: IPcCacheStore;
begin
  Result := False;
  try
    if AComCliente then
      LStore := TRedisCacheStore.Create(AClient, ATtlMs)
    else
      LStore := TRedisCacheStore.Create(APool, ATtlMs);
  except
    on E: EArgumentException do
      Result := True;
  end;
  LStore := nil;
end;

procedure TRedisCacheIntegrationTests.Cache_PutGetDelete;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LValor: string;
  LPttl: Int64;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    LStore := TRedisCacheStore.Create(LPool);
    LStore.Put(Chave('cache:a'), 'valor', 10000);
    TAssert.AssertTrue(LStore.TryGet(Chave('cache:a'), LValor));
    TAssert.AssertEquals('valor', LValor);
    // PSETEX: o prazo chega ao servidor em milissegundos.
    LPttl := LConn.Execute('PTTL', [Chave('cache:a')]).AsInteger;
    TAssert.AssertTrue('PTTL ' + IntToStr(LPttl), (LPttl > 0) and (LPttl <= 10000));

    LStore.Put(Chave('cache:a'), 'outro', 10000);
    TAssert.AssertTrue(LStore.TryGet(Chave('cache:a'), LValor));
    TAssert.AssertEquals('outro', LValor);

    LStore.Delete(Chave('cache:a'));
    TAssert.AssertFalse(LStore.TryGet(Chave('cache:a'), LValor));
    TAssert.AssertEquals('', LValor);
    LStore.Delete(Chave('cache:a'));  // ausente: nao levanta
    // '' guardado e' acerto, nao ausencia.
    LStore.Put(Chave('cache:a'), '', 10000);
    TAssert.AssertTrue(LStore.TryGet(Chave('cache:a'), LValor));
    TAssert.AssertEquals('', LValor);
  finally
    Limpa(LConn, ['cache:a']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_PrazoVence;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LValor: string;
  LPrazo: UInt64;
  LVivo: Boolean;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    LStore := TRedisCacheStore.Create(LPool);
    LStore.Put(Chave('cache:prazo'), 'v', 300);
    TAssert.AssertTrue(LStore.TryGet(Chave('cache:prazo'), LValor));
    LPrazo := PcTickMs + 5000;
    repeat
      LVivo := LStore.TryGet(Chave('cache:prazo'), LValor);
      if LVivo then
        Sleep(20);
    until (not LVivo) or (PcTickMs >= LPrazo);
    TAssert.AssertFalse('o prazo de 300 ms nao venceu em 5 s', LVivo);
    TAssert.AssertEquals(Int64(0), LConn.Execute('EXISTS', [Chave('cache:prazo')]).AsInteger);
  finally
    Limpa(LConn, ['cache:prazo']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_TtlZeroOuNegativo_ApagaENaoGrava;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LValor: string;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    LStore := TRedisCacheStore.Create(LPool);
    LStore.Put(Chave('cache:ttl0'), 'v', 10000);
    LStore.Put(Chave('cache:ttl0'), 'x', 0);
    TAssert.AssertFalse('TTL 0 apaga a que existia', LStore.TryGet(Chave('cache:ttl0'), LValor));
    LStore.Put(Chave('cache:ttl0'), 'y', -1);
    TAssert.AssertEquals('TTL negativo nao grava', Int64(0),
      LConn.Execute('EXISTS', [Chave('cache:ttl0')]).AsInteger);
  finally
    Limpa(LConn, ['cache:ttl0']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_Generations_NasceDoRelogioComTtl;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LAntes, LDepois, LPttl: Int64;
  LGeracoes, LDeNovo: TArray<Int64>;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    Limpa(LConn, ['cache:tag:a', 'cache:tag:b']);
    LStore := TRedisCacheStore.Create(LPool);
    TAssert.AssertEquals('sem tags, nenhum valor', 0, Length(LStore.Generations([])));

    LAntes := RedisUnixTimeMs;
    LGeracoes := LStore.Generations([Chave('cache:tag:a'), Chave('cache:tag:b')]);
    LDepois := RedisUnixTimeMs;
    TAssert.AssertEquals(2, Length(LGeracoes));
    // Nunca em 0: nasce no horario Unix em ms (C4).
    TAssert.AssertTrue('a nasceu do relogio', (LGeracoes[0] >= LAntes) and (LGeracoes[0] <= LDepois));
    TAssert.AssertTrue('b nasceu do relogio', (LGeracoes[1] >= LAntes) and (LGeracoes[1] <= LDepois));
    // SET NX PX: nasce com o TTL padrao, um dia (F2.1).
    TAssert.AssertEquals(Int64(86400000), REDIS_CACHE_COUNTER_TTL_MS);
    LPttl := PttlDe(LConn, Chave('cache:tag:a'));
    TAssert.AssertTrue('PTTL de a: ' + IntToStr(LPttl),
      (LPttl > 0) and (LPttl <= REDIS_CACHE_COUNTER_TTL_MS));
    LPttl := PttlDe(LConn, Chave('cache:tag:b'));
    TAssert.AssertTrue('PTTL de b: ' + IntToStr(LPttl),
      (LPttl > 0) and (LPttl <= REDIS_CACHE_COUNTER_TTL_MS));

    // Relido, nao recriado; e a ordem segue a de ATags.
    LDeNovo := LStore.Generations([Chave('cache:tag:b'), Chave('cache:tag:a')]);
    TAssert.AssertEquals(LGeracoes[1], LDeNovo[0]);
    TAssert.AssertEquals(LGeracoes[0], LDeNovo[1]);

    // Com um CounterTtlMs proprio, e' esse o prazo que vai ao servidor.
    LStore := TRedisCacheStore.Create(LPool, 50000);
    LStore.Generations([Chave('cache:tag:c')]);
    LPttl := PttlDe(LConn, Chave('cache:tag:c'));
    TAssert.AssertTrue('PTTL de c: ' + IntToStr(LPttl), (LPttl > 0) and (LPttl <= 50000));
  finally
    Limpa(LConn, ['cache:tag:a', 'cache:tag:b', 'cache:tag:c']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_Generations_LeituraNaoRenovaTtl;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LTag: string;
  LGeracao, LPttlCriado, LPttlAntes, LPttlDepois: Int64;
  LPrazo: UInt64;
begin
  LTag := Chave('cache:tag:leitura');
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    Limpa(LConn, ['cache:tag:leitura']);
    LStore := TRedisCacheStore.Create(LPool, 60000);
    LGeracao := LStore.Generations([LTag])[0];
    LPttlCriado := PttlDe(LConn, LTag);
    TAssert.AssertTrue('PTTL ' + IntToStr(LPttlCriado), LPttlCriado > 0);
    // Espera o PTTL andar, para que uma renovacao (que o levaria de volta a
    // 60000) se distinga de "nada mudou".
    LPrazo := PcTickMs + 5000;
    repeat
      LPttlAntes := PttlDe(LConn, LTag);
      if LPttlAntes >= LPttlCriado - 20 then
        Sleep(5);
    until (LPttlAntes < LPttlCriado - 20) or (PcTickMs >= LPrazo);
    TAssert.AssertTrue('o PTTL nao andou', LPttlAntes < LPttlCriado - 20);

    TAssert.AssertEquals(LGeracao, LStore.Generations([LTag])[0]);
    TAssert.AssertEquals(LGeracao, LStore.Generations([LTag])[0]);
    LPttlDepois := PttlDe(LConn, LTag);
    TAssert.AssertTrue(Format('a leitura renovou o TTL: %d antes, %d depois',
      [LPttlAntes, LPttlDepois]), (LPttlDepois > 0) and (LPttlDepois <= LPttlAntes));
  finally
    Limpa(LConn, ['cache:tag:leitura']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_Bump_AvancaEAusenteFicaAusente;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LGeracao: Int64;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    Limpa(LConn, ['cache:tag:x', 'cache:tag:ausente']);
    LStore := TRedisCacheStore.Create(LPool);
    LGeracao := LStore.Generations([Chave('cache:tag:x')])[0];
    LStore.Bump([Chave('cache:tag:x'), Chave('cache:tag:ausente')]);
    TAssert.AssertEquals(LGeracao + 1, LStore.Generations([Chave('cache:tag:x')])[0]);
    // O INCR puro criaria em 1; o ausente tem de continuar ausente.
    TAssert.AssertEquals('Bump nao cria contador', Int64(0),
      LConn.Execute('EXISTS', [Chave('cache:tag:ausente')]).AsInteger);
    LStore.Bump([]);  // nada a fazer, nao vai ao servidor
  finally
    Limpa(LConn, ['cache:tag:x', 'cache:tag:ausente']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_Bump_PreservaTtl;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LTag: string;
  LGeracao, LPttlAntes, LPttlDepois: Int64;
begin
  LTag := Chave('cache:tag:bumpttl');
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    Limpa(LConn, ['cache:tag:bumpttl']);
    LStore := TRedisCacheStore.Create(LPool, 60000);
    LGeracao := LStore.Generations([LTag])[0];
    LPttlAntes := PttlDe(LConn, LTag);
    TAssert.AssertTrue('PTTL ' + IntToStr(LPttlAntes), LPttlAntes > 0);
    LStore.Bump([LTag]);
    LStore.Bump([LTag]);
    TAssert.AssertEquals(LGeracao + 2, LStore.Generations([LTag])[0]);
    // O INCR do script nao mexe no TTL: nem tira (-1), nem renova (> antes).
    LPttlDepois := PttlDe(LConn, LTag);
    TAssert.AssertTrue(Format('PTTL %d antes do Bump, %d depois', [LPttlAntes, LPttlDepois]),
      (LPttlDepois > 0) and (LPttlDepois <= LPttlAntes));
  finally
    Limpa(LConn, ['cache:tag:bumpttl']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_ContadorApagado_VoltaMaior_EntradaAntigaNaoELida;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LCache: IPcCache;
  LConn: TRedisConnection;
  LTag, LChave1, LChave2, LChave3, LValor: string;
  LMaior, LRenascido: Int64;
begin
  LTag := NOME_CACHE_IT + ':tag:categoria';
  LChave1 := '';
  LChave2 := '';
  LChave3 := '';
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    LConn.Execute('DEL', [LTag]);
    LStore := TRedisCacheStore.Create(LPool);
    LCache := TPcCache.Create(LStore, NOME_CACHE_IT);

    LChave1 := LCache.Key('produto', ['categoria']);
    LCache.Put(LChave1, 'velho', 60);
    LCache.Invalidate(['categoria']);
    LCache.Invalidate(['categoria']);
    LChave2 := LCache.Key('produto', ['categoria']);
    TAssert.AssertTrue(LChave2 <> LChave1);
    LCache.Put(LChave2, 'velho2', 60);
    TAssert.AssertTrue(LCache.TryGet(LChave2, LValor));
    LMaior := LStore.Generations([LTag])[0];

    // Eviction do contador (ou restart sem persistencia).
    LConn.Execute('DEL', [LTag]);
    EsperaRelogioPassar(LMaior);

    LChave3 := LCache.Key('produto', ['categoria']);
    LRenascido := LStore.Generations([LTag])[0];
    TAssert.AssertTrue(Format('renasceu em %d, ja'' tinha sido %d', [LRenascido, LMaior]),
      LRenascido > LMaior);
    TAssert.AssertTrue(LChave3 <> LChave1);
    TAssert.AssertTrue(LChave3 <> LChave2);
    TAssert.AssertFalse('a entrada antiga nao volta', LCache.TryGet(LChave3, LValor));
    // Ela continua no Redis: quem a esconde e' a geracao, nao um DEL.
    TAssert.AssertTrue(LStore.TryGet(LChave2, LValor));
    TAssert.AssertEquals('velho2', LValor);
  finally
    LConn.Execute('DEL', [LTag]);
    if LChave1 <> '' then
      LConn.Execute('DEL', [LChave1]);
    if LChave2 <> '' then
      LConn.Execute('DEL', [LChave2]);
    if LChave3 <> '' then
      LConn.Execute('DEL', [LChave3]);
    LConn.Free;
    LCache := nil;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_ContadorExpirado_VoltaMaior_EntradaAntigaNaoELida;
const
  TTL_CONTADOR_MS = 200;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LCache: IPcCache;
  LConn: TRedisConnection;
  LTag, LChave1, LChave2, LChave3, LValor: string;
  LMaior, LRenascido, LPttl: Int64;
begin
  LTag := NOME_CACHE_IT + ':tag:categoria.ttl';
  LChave1 := '';
  LChave2 := '';
  LChave3 := '';
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    LConn.Execute('DEL', [LTag]);
    LStore := TRedisCacheStore.Create(LPool, TTL_CONTADOR_MS);
    LCache := TPcCache.Create(LStore, NOME_CACHE_IT);

    // Entradas com 60 s de TTL, muito mais que os 200 ms do contador.
    LChave1 := LCache.Key('produto', ['categoria.ttl']);
    LCache.Put(LChave1, 'velho', 60);
    LCache.Invalidate(['categoria.ttl']);
    LCache.Invalidate(['categoria.ttl']);
    LChave2 := LCache.Key('produto', ['categoria.ttl']);
    TAssert.AssertTrue(LChave2 <> LChave1);
    LCache.Put(LChave2, 'velho2', 60);
    TAssert.AssertTrue(LCache.TryGet(LChave2, LValor));
    LMaior := LStore.Generations([LTag])[0];

    // O contador expira sozinho: nenhum DEL aqui.
    TAssert.AssertTrue('o contador nao expirou em 5 s', EsperaChaveSumir(LConn, LTag));
    EsperaRelogioPassar(LMaior);

    LChave3 := LCache.Key('produto', ['categoria.ttl']);
    LRenascido := LStore.Generations([LTag])[0];
    TAssert.AssertTrue(Format('renasceu em %d, ja'' tinha sido %d', [LRenascido, LMaior]),
      LRenascido > LMaior);
    // Renasceu com TTL de novo (-2: ja' expirou outra vez, numa maquina lenta).
    LPttl := PttlDe(LConn, LTag);
    TAssert.AssertTrue('PTTL do renascido: ' + IntToStr(LPttl),
      (LPttl = -2) or ((LPttl > 0) and (LPttl <= TTL_CONTADOR_MS)));
    TAssert.AssertTrue(LChave3 <> LChave1);
    TAssert.AssertTrue(LChave3 <> LChave2);
    TAssert.AssertFalse('a entrada antiga nao volta', LCache.TryGet(LChave3, LValor));
    // Ela continua no Redis: quem a esconde e' a geracao.
    TAssert.AssertTrue(LStore.TryGet(LChave2, LValor));
    TAssert.AssertEquals('velho2', LValor);
  finally
    LConn.Execute('DEL', [LTag]);
    if LChave1 <> '' then
      LConn.Execute('DEL', [LChave1]);
    if LChave2 <> '' then
      LConn.Execute('DEL', [LChave2]);
    if LChave3 <> '' then
      LConn.Execute('DEL', [LChave3]);
    LConn.Free;
    LCache := nil;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_CounterTtlInvalido_Levanta;
var
  LPool: TRedisPool;
  LClient: TRedisClient;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LClient := TRedisClient.Create(ParamsDeTeste);
  try
    TAssert.AssertTrue('pool, 0', CounterTtlLevanta(LPool, LClient, False, 0));
    TAssert.AssertTrue('pool, -1', CounterTtlLevanta(LPool, LClient, False, -1));
    TAssert.AssertTrue('cliente, 0', CounterTtlLevanta(LPool, LClient, True, 0));
    TAssert.AssertTrue('cliente, -1', CounterTtlLevanta(LPool, LClient, True, -1));
    TAssert.AssertFalse('pool, 1', CounterTtlLevanta(LPool, LClient, False, 1));
    TAssert.AssertFalse('cliente, 1', CounterTtlLevanta(LPool, LClient, True, 1));
  finally
    LClient.Free;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_TextoNaoAscii_IdaEVoltaByteAByte;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LValor: string;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    LStore := TRedisCacheStore.Create(LPool);
    LStore.Put(Chave('cache:sp'), 'São Paulo', 10000);
    // O que foi para o servidor: os bytes UTF-8, nem um a mais.
    TAssert.AssertEquals('53C3A36F205061756C6F',
      Hex(LConn.Execute('GET', [Chave('cache:sp')]).AsBytes));
    TAssert.AssertTrue(LStore.TryGet(Chave('cache:sp'), LValor));
    TAssert.AssertEquals('São Paulo', LValor);

    // E o caminho inverso: bytes UTF-8 gravados por outro cliente.
    LConn.Execute('SET', [Chave('cache:sp'),
      MakeBytes([$53, $C3, $A3, $6F, $20, $50, $61, $75, $6C, $6F])]);
    TAssert.AssertTrue(LStore.TryGet(Chave('cache:sp'), LValor));
    TAssert.AssertEquals('São Paulo', LValor);
  finally
    Limpa(LConn, ['cache:sp']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_ErroDeTipoOuDeResposta_Sobe;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
begin
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  try
    Limpa(LConn, ['cache:naonum', 'cache:hash']);
    LStore := TRedisCacheStore.Create(LPool);
    // Bug de quem reusou o nome, nao indisponibilidade: sobe como esta'.
    LConn.Execute('SET', [Chave('cache:naonum'), 'abc']);
    TAssert.AssertEquals('ERedisTypeError', FalhaDoStore(LStore, 3, Chave('cache:naonum')));
    TAssert.AssertEquals('ERedisReplyError', FalhaDoStore(LStore, 4, Chave('cache:naonum')));
    LConn.Execute('HSET', [Chave('cache:hash'), 'campo', 'v']);
    TAssert.AssertEquals('ERedisTypeError', FalhaDoStore(LStore, 3, Chave('cache:hash')));
    TAssert.AssertEquals('ERedisReplyError', FalhaDoStore(LStore, 4, Chave('cache:hash')));
    TAssert.AssertEquals('ERedisReplyError', FalhaDoStore(LStore, 0, Chave('cache:hash')));
  finally
    Limpa(LConn, ['cache:naonum', 'cache:hash']);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_RedisFora_ViraMissEOnError;
var
  LParams: TRedisParams;
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LCache: IPcCache;
  LAvisos: TRedisCacheAvisosIt;
  LChave, LValor: string;
  I: Integer;
begin
  // Uma porta onde nao ha' Redis, como no teste do lock.
  LParams := ParamsDeTeste;
  LParams.Port := 1;
  LParams.ConnectTimeoutMs := 500;
  LPool := TRedisPool.Create(LParams);
  LAvisos := TRedisCacheAvisosIt.Create;
  try
    LStore := TRedisCacheStore.Create(LPool);
    for I := 0 to 4 do
      TAssert.AssertEquals('operacao ' + IntToStr(I), 'EPcCacheUnavailable',
        FalhaDoStore(LStore, I, Chave('cache:fora')));

    // Pelo TPcCache: miss, loader e nenhuma excecao. RetryAfterMs = 0 para
    // cada chamada ir ao store (sem a pausa, que o teste da common cobre).
    LCache := TPcCache.Create(LStore, NOME_CACHE_IT, LAvisos.Aviso, 0);
    TAssert.AssertEquals('sem contadores, sem chave', '', LCache.Key('produto', ['categoria']));
    LChave := LCache.Key('produto', []);
    TAssert.AssertEquals(NOME_CACHE_IT + ':v:produto|', LChave);
    TAssert.AssertFalse(LCache.TryGet(LChave, LValor));
    TAssert.AssertEquals('carregado:7', LCache.GetOrLoad(LChave, 60, LAvisos.Carrega, '7'));
    TAssert.AssertEquals(1, LAvisos.Cargas);
    LCache.Put(LChave, 'x', 60);
    LCache.Invalidate(['categoria']);
    LCache.Remove(LChave);
    // Key, TryGet, GetOrLoad (TryGet + Put), Put, Invalidate, Remove.
    TAssert.AssertEquals(7, LAvisos.Total);
    TAssert.AssertEquals('EPcCacheUnavailable', LAvisos.Classe);
  finally
    LCache := nil;
    LStore := nil;
    LAvisos.Free;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_PoolEsgotadoOuFechado_ViraIndisponivel;
var
  LPoolParams: TRedisPoolParams;
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
begin
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 1;
  LPoolParams.AcquireTimeoutMs := 0;
  LPool := TRedisPool.Create(ParamsDeTeste, LPoolParams);
  try
    LStore := TRedisCacheStore.Create(LPool);
    LConn := LPool.Acquire;
    try
      TAssert.AssertEquals('pool esgotado', 'EPcCacheUnavailable',
        FalhaDoStore(LStore, 0, Chave('cache:esgotado')));
    finally
      LPool.Release(LConn);
    end;
    LPool.Close;
    TAssert.AssertEquals('pool fechado', 'EPcCacheUnavailable',
      FalhaDoStore(LStore, 0, Chave('cache:esgotado')));
  finally
    LStore := nil;
    LPool.Free;
  end;
end;

procedure TRedisCacheIntegrationTests.Cache_Concorrente_CriacaoUnicaEBumpsContados;
const
  THREADS = 8;
  TAGS = 20;
  RODADAS = 25;
var
  LPool: TRedisPool;
  LStore: IPcCacheStore;
  LConn: TRedisConnection;
  LTags: array of string;
  LSufixos: array of string;
  LThreads: array of TRedisCacheCorridaIt;
  LLargada: TEvent;
  LInicial: Int64;
  I, J: Integer;
begin
  SetLength(LTags, TAGS);
  SetLength(LSufixos, TAGS + 1);
  for I := 0 to TAGS - 1 do
  begin
    LSufixos[I] := 'cache:corrida:' + IntToStr(I);
    LTags[I] := Chave(LSufixos[I]);
  end;
  LSufixos[TAGS] := 'cache:corrida:compartilhado';
  SetLength(LThreads, THREADS);
  LPool := TRedisPool.Create(ParamsDeTeste);
  LConn := AbreConexao;
  LLargada := TEvent.Create(nil, True, False, '');
  try
    Limpa(LConn, LSufixos);
    LStore := TRedisCacheStore.Create(LPool);
    LInicial := LStore.Generations([Chave(LSufixos[TAGS])])[0];
    for I := 0 to THREADS - 1 do
      LThreads[I] := TRedisCacheCorridaIt.Create(LStore, LTags, Chave(LSufixos[TAGS]),
        RODADAS, LLargada);
    LLargada.SetEvent;
    for I := 0 to THREADS - 1 do
      LThreads[I].WaitFor;
    for I := 0 to THREADS - 1 do
      TAssert.AssertEquals('thread ' + IntToStr(I), '', LThreads[I].Erro);
    // Todas correram para criar o mesmo contador: um SET NX ganhou, e todas
    // leram o valor dele.
    for J := 0 to TAGS - 1 do
      for I := 1 to THREADS - 1 do
        TAssert.AssertEquals('tag ' + IntToStr(J), LThreads[0].Valores[J], LThreads[I].Valores[J]);
    // Nenhum Bump se perdeu nem foi contado duas vezes.
    TAssert.AssertEquals(LInicial + THREADS * RODADAS,
      LStore.Generations([Chave(LSufixos[TAGS])])[0]);
  finally
    for I := 0 to THREADS - 1 do
      LThreads[I].Free;
    LLargada.Free;
    Limpa(LConn, LSufixos);
    LConn.Free;
    LStore := nil;
    LPool.Free;
  end;
end;

{ --- Fase D da observabilidade: spans por comando --- }

function TRedisSpanColetorIt.ExportSpans(const ASpans: TPcSpanDataArray;
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

function AtributoIt(const ASpan: TPcSpanData; const AKey: string): string;
var
  I: Integer;
begin
  Result := '<ausente>';
  for I := 0 to High(ASpan.Attributes) do
    if ASpan.Attributes[I].Key = AKey then
    begin
      if ASpan.Attributes[I].ValueType = satInt then
        Result := IntToStr(ASpan.Attributes[I].IntValue)
      else
        Result := ASpan.Attributes[I].StringValue;
      Exit;
    end;
end;

procedure TRedisTracingIntegrationTests.Cliente_UmSpanPorComandoNaTraceDaRequisicao;
var
  LColetor: TRedisSpanColetorIt;
  LRef: IPcSpanExporter;
  LClient: TRedisClient;
  LPai: IPcSpan;
  LTraceId, LPaiId, LChaveLida, LValor, LNomes: string;
  I: Integer;
begin
  // O caminho inteiro de uma aplicacao: fachada, pool, conexao de verdade e o
  // pool separado dos bloqueantes. Cada comando vira um span filho do span
  // corrente (o de servidor de uma requisicao HTTP, aqui simulado), e o
  // handshake das conexoes novas e o health check nao aparecem.
  LColetor := TRedisSpanColetorIt.Create;
  LRef := LColetor;
  TPcTracing.Start(TPcTracingOptions.Default('redis-it', '0'), LRef, False);
  try
    LClient := NovoCliente;
    try
      LPai := TPcTracing.StartSpan('GET /pedidos', skServer);
      LTraceId := LPai.TraceId;
      LPaiId := LPai.SpanId;
      LClient.Strings.SetValue(Chave('tr:1'), 'segredo');
      TAssert.AssertEquals('segredo', LClient.Strings.Get(Chave('tr:1')).AsString);
      TAssert.AssertFalse('lista vazia: o BLPOP vence o prazo',
        LClient.Lists.BLPop([Chave('tr:fila')], 0.2, LChaveLida, LValor));
      LClient.Keys.Del(Chave('tr:1'));
      LPai.Finish;
      LPai := nil;
    finally
      LClient.Free;
    end;
    TPcTracing.FlushNow;

    LNomes := '';
    for I := 0 to High(LColetor.Spans) do
      LNomes := LNomes + LColetor.Spans[I].Name + '|';
    TAssert.AssertEquals('SET|GET|BLPOP|DEL|GET /pedidos|', LNomes);
    for I := 0 to 3 do
    begin
      TAssert.AssertEquals(LTraceId, LColetor.Spans[I].TraceId);
      TAssert.AssertEquals(LPaiId, LColetor.Spans[I].ParentSpanId);
      TAssert.AssertTrue('sem erro', LColetor.Spans[I].Status = ssUnset);
      TAssert.AssertEquals('redis', AtributoIt(LColetor.Spans[I], 'db.system.name'));
    end;
    TAssert.AssertEquals('o valor nao vai para o span', 'SET ' + Chave('tr:1') + ' ?',
      AtributoIt(LColetor.Spans[0], 'db.query.text'));
    TAssert.AssertEquals('BLPOP ' + Chave('tr:fila') + ' ?',
      AtributoIt(LColetor.Spans[2], 'db.query.text'));
  finally
    TPcTracing.Shutdown;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisConnectionIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisPoolIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisClientIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisKeysIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisStringsIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisHashesIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisListsIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisSetsIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisZSetsIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisTransactionIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisPubSubIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisScriptingIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisStreamsIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisLockIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisCacheIntegrationTests);
  TDUnitX.RegisterTestFixture(TRedisTracingIntegrationTests);

end.
