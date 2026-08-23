unit Redis.CommandsTests;

{ Testes das fachadas por familia e do TRedisClient (FPCUnit). Mesma cobertura
  do tests\Unit\Redis.CommandsTests.pas (DUnitX/Delphi) — as duas suites sao
  mantidas linha a linha equivalentes, entao toda mudanca aqui vai para la' na
  mesma sessao.

  Sem servidor: o cliente e' amarrado a uma conexao sobre o
  TRedisFakeServerStream da Redis.ConnectionTests. Isso permite verificar as
  DUAS metades de cada metodo tipado, que sao onde moram os erros de uma
  fachada de comandos:

  1. **O que foi para o fio.** A ordem dos modificadores de SET, ZADD e
     ZRANGEBYSCORE nao e' livre — o servidor recusa argumento fora de ordem — e
     um teste de integracao que so' olha o resultado nao distingue "montou
     certo" de "montou errado e o servidor perdoou".
  2. **Como a resposta foi convertida.** Nulo virando '' ou 0, WITHSCORES
     mudando de forma entre RESP2 e RESP3, cursor de SCAN lido do lugar errado.

  As respostas roteirizadas usam so' ASCII: o helper Wire mede a chave com
  Length, que conta bytes no FPC e caracteres no Delphi. Binario e acentuacao
  sao cobertos pelos testes de integracao, contra o servidor de verdade. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  Redis.Types, Redis.Resp, Redis.Connection, Redis.Client, Redis.Commands,
  Redis.Commands.Keys, Redis.Commands.Strings, Redis.Commands.Hashes,
  Redis.Commands.Lists, Redis.Commands.Sets, Redis.Commands.ZSets,
  Redis.Commands.Streams,
  Redis.ConnectionTests;

type
  TRedisCommandHelpersTests = class(TTestCase)
  published
    procedure RedisArgs_ConcatenaNaOrdem;
    procedure RedisArgs_ListaVaziaDosDoisLados;
    procedure RedisAddArg_AcrescentaNoFim;
    procedure ReplyToStrings_NuloViraListaVazia;
    procedure ReplyToStrings_ItemNuloViraStringVazia;
    procedure ReplyToStrings_EscalarLevanta;
    procedure ReplyToBooleans_ZeroEUm;
    procedure ScoreBound_ExclusivoGanhaParentese;
    procedure ScoreBound_UsaPontoDecimal;
  end;

  TRedisClientTests = class(TTestCase)
  published
    procedure Execute_SaiPelaConexaoAmarrada;
    procedure ExecuteRaw_ErroDoServidor_NaoLevanta;
    procedure Execute_ErroDoServidor_Levanta;
    procedure Pipeline_PelaFachada;
    procedure Ping_PelaFachada;
    procedure Acquire_ConexaoUnica_DevolveSempreAMesma;
    procedure Release_ConexaoUnica_NaoDestroiAConexao;
    procedure Familias_UsamOMesmoExecutor;
  end;

  TRedisKeysCommandsTests = class(TTestCase)
  published
    procedure Del_UmaChave_CodificaEDevolveBooleano;
    procedure DelMany_ContaAsApagadas;
    procedure DelMany_SemChave_NaoVaiAoServidor;
    procedure Exists_ZeroEhFalse;
    procedure ExistsMany_ContaRepeticoes;
    procedure Expire_CodificaOsSegundos;
    procedure Ttl_ChaveAusente_DevolveMenosDois;
    procedure KeyType_DevolveOTipo;
    procedure Rename_NaoDevolveNada;
    procedure RenameNx_ZeroEhFalse;
    procedure CopyKey_ComReplace_MandaOModificador;
    procedure CopyKey_SemReplace_NaoMandaOModificador;
    procedure Keys_ListaAsChaves;
    procedure Scan_SemFiltro_MandaSoOCursor;
    procedure Scan_ComMatchCountETipo_NaOrdem;
    procedure Scan_AtualizaOCursorEDevolveOLote;
    procedure Scan_RespostaForaDoFormato_Levanta;
  end;

  TRedisStringsCommandsTests = class(TTestCase)
  published
    procedure SetValue_CodificaSetSimples;
    procedure SetWithOptions_NxGetEExpiracao_NaOrdemDaEspecificacao;
    procedure SetWithOptions_KeepTtl_NaoMandaNumero;
    procedure SetWithOptions_NxBarrado_DevolveNulo;
    procedure SetWithOptions_XxEExAt;
    procedure Get_ChaveAusente_EhNuloENaoVazio;
    procedure TryGet_ChaveAusente_DevolveFalse;
    procedure TryGet_ValorVazio_DevolveTrue;
    procedure GetString_ChaveAusente_DevolveVazio;
    procedure SetNx_ZeroEhFalse;
    procedure SetEx_CodificaNaOrdemChavePrazoValor;
    procedure Incr_DevolveOInteiro;
    procedure IncrByFloat_MandaPontoDecimal;
    procedure IncrByFloat_LeODoubleDaResposta;
    procedure MSet_QuantidadeImpar_Levanta;
    procedure MSet_CodificaOsPares;
    procedure MGet_MantemONuloNoMeio;
    procedure MGet_SemChave_Levanta;
    procedure GetRange_CodificaOsIndices;
    procedure StrLen_DevolveOTamanho;
  end;

  TRedisHashesCommandsTests = class(TTestCase)
  published
    procedure HSet_DevolveQuantosCamposNasceram;
    procedure HSetMany_CodificaOsPares;
    procedure HSetMany_QuantidadeImpar_Levanta;
    procedure HGetAll_Resp2Array_AcessoPorCampo;
    procedure HGetAll_Resp3Mapa_MesmaFormaQueOResp2;
    procedure HTryGet_CampoAusente_DevolveFalse;
    procedure HMGet_MantemONuloNoMeio;
    procedure HDel_ZeroEhFalse;
    procedure HIncrByFloat_MandaPontoDecimal;
    procedure HKeys_ListaOsCampos;
    procedure HScan_MontaMatchECount;
  end;

  TRedisListsCommandsTests = class(TTestCase)
  published
    procedure LPushMany_MandaTudoNumComandoSo;
    procedure LPushMany_SemValor_Levanta;
    procedure LRange_CodificaEDevolveALista;
    procedure LPop_ListaVazia_EhNulo;
    procedure LInsert_Before_MandaAPalavraCerta;
    procedure LInsert_After_MandaAPalavraCerta;
    procedure LMove_MandaAsDuasPontas;
    procedure BLPop_PrazoVencido_DevolveFalse;
    procedure BLPop_DevolveChaveEValor;
    procedure BLPop_CodificaOTimeoutNoFim;
    procedure BLPop_SemChave_Levanta;
    procedure BLPop_RestauraOTimeoutDaConexao;
    procedure BLMove_CodificaPontasETimeout;
  end;

  TRedisSetsCommandsTests = class(TTestCase)
  published
    procedure SAdd_MembroNovo_DevolveTrue;
    procedure SAdd_MembroRepetido_DevolveFalse;
    procedure SAddMany_CodificaTodos;
    procedure SMembers_ListaOsMembros;
    procedure SMIsMember_MapeiaZeroEUm;
    procedure SInterStore_DestinoAntesDasChaves;
    procedure SInterCard_MandaAQuantidadeDeChaves;
    procedure SInterCard_ComLimite;
    procedure SScan_MontaMatchECount;
    procedure SInter_SemChave_Levanta;
  end;

  TRedisZSetsCommandsTests = class(TTestCase)
  published
    procedure ZAdd_FormataOScoreComPonto;
    procedure ZAddOpt_NxECh_NaOrdemDaEspecificacao;
    procedure ZAddOpt_SemCondicao_NaoMandaModificador;
    procedure ZAddMany_QuantidadeImpar_Levanta;
    procedure ZRangeWithScores_Resp2Achatado;
    procedure ZRangeWithScores_Resp3Pares_MesmoResultado;
    procedure ZRangeWithScores_TamanhoImpar_Levanta;
    procedure ZRangeByScore_SemLimite_NaoMandaLimit;
    procedure ZRangeByScore_ComLimite_MandaOffsetECount;
    procedure ZRevRangeByScore_InverteMaxEMin;
    procedure ZTryScore_MembroAusente_DevolveFalse;
    procedure ZTryScore_LeODoubleDoResp3;
    procedure ZTryRank_MembroAusente_DevolveFalse;
    procedure ZPopMin_ConjuntoVazio_DevolveFalse;
    procedure ZPopMin_DevolveMembroEScore;
    procedure ZIncrBy_MandaDeltaComPonto;
    procedure ZScan_MontaMatchECount;
  end;

  TRedisStreamHelpersTests = class(TTestCase)
  published
    procedure StreamId_MontaMsEHifenESequencia;
    procedure TryParseStreamId_QuebraNasDuasPartes;
    procedure TryParseStreamId_SemHifen_DevolveFalse;
    procedure TryParseStreamId_AceitaOParenteseDeExtremoAberto;
    procedure StreamIdExclusive_AcrescentaOParentese;
    procedure StreamIdExclusive_NaoDuplicaOParentese;
    procedure ReplyToStreamEntries_NuloViraListaVazia;
    procedure ReplyToStreamEntries_ForaDoFormato_Levanta;
    procedure ReplyToStreamData_Resp2_ListaDePares;
    procedure ReplyToStreamData_Resp3_MapaAchatado;
    procedure FindStreamData_AchaPeloNomeENaoPelaPosicao;
  end;

  TRedisStreamsCommandsTests = class(TTestCase)
  published
    procedure XAdd_UsaAsteriscoEDevolveOId;
    procedure XAdd_SemCampo_Levanta;
    procedure XAdd_QuantidadeImpar_Levanta;
    procedure XAddId_MandaOIdEscolhido;
    procedure XAddMaxLen_Aproximado_MandaOTilAntesDoNumero;
    procedure XAddMaxLen_Exato_MandaOIgual;
    procedure XAddMinId_MandaMinIdAntesDoAsterisco;
    procedure XTrimMaxLen_AproximadoPorPadrao;
    procedure XTrimMinId_Exato;
    procedure XLen_DevolveOTamanho;
    procedure XDel_ContaOsQueExistiam;
    procedure XDel_SemId_NaoVaiAoServidor;
    procedure XRange_SemCount_NaoMandaCount;
    procedure XRange_ComCount_MandaOModificador;
    procedure XRange_LeIdECamposPorNome;
    procedure XRange_CampoAusente_TryFieldValueDevolveFalse;
    procedure XRange_EntradaApagada_VemSemCampos;
    procedure XRevRange_InverteOsExtremos;
    procedure XRead_MontaOBlocoStreams;
    procedure XRead_ComCount_CountAntesDeStreams;
    procedure XRead_ChavesEIdsDesbalanceados_Levanta;
    procedure XRead_SemChave_Levanta;
    procedure XRead_NadaNovo_NuloViraListaVazia;
    procedure XRead_Resp2_UmaEntradaPorChave;
    procedure XRead_Resp3_MapaAchatado_MesmoResultado;
    procedure XReadBlocking_MandaBlockEmMilissegundos;
    procedure XReadBlocking_RestauraOTimeoutDaConexao;
    procedure XGroupCreate_ComMkStream;
    procedure XGroupTryCreate_BusyGroup_DevolveFalseSemLevantar;
    procedure XGroupTryCreate_OutroErro_Levanta;
    procedure XGroupDestroy_ZeroEhFalse;
    procedure XGroupDelConsumer_DevolveAsPendenciasPerdidas;
    procedure XReadGroup_MontaGroupCountENoack;
    procedure XReadGroup_ModoNovo_MandaOMaiorQue;
    procedure XReadGroupBlocking_BlockEntreCountENoack;
    procedure XAck_ContaOsConfirmados;
    procedure XAck_SemId_NaoVaiAoServidor;
    procedure XPendingSummary_LeTotalFaixaEConsumidores;
    procedure XPendingSummary_SemPendencia_VemZeradoESemIds;
    procedure XPendingRange_MandaFaixaEContagem;
    procedure XPendingRange_ComConsumidor_VaiNoFim;
    procedure XPendingRange_LeOciosidadeEEntregas;
    procedure XPendingIdle_MandaIdleAntesDaFaixa;
    procedure XClaim_MandaMinIdleEOsIds;
    procedure XClaim_SemId_Levanta;
    procedure XClaimJustId_MandaJustIdNoFim;
    procedure XAutoClaim_LeCursorEntradasEApagados;
    procedure XAutoClaim_RespostaDeDoisItens_AindaFunciona;
    procedure XInfoStream_MapaAchatado_AcessoPorChave;
  end;

implementation

const
  CRLF = #13#10;

{ Helpers compartilhados }

// Monta o unified request protocol esperado no fio. So' ASCII: no Delphi
// Length conta caracteres, nao bytes.
function Wire(const AArgs: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AArgs)) + CRLF;
  for I := 0 to High(AArgs) do
    Result := Result + '$' + IntToStr(Length(AArgs[I])) + CRLF + AArgs[I] + CRLF;
end;

// Bulk string RESP2.
function Bulk(const AValue: string): string;
begin
  Result := '$' + IntToStr(Length(AValue)) + CRLF + AValue + CRLF;
end;

// Array RESP2 de bulk strings.
function Arr(const AItems: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AItems)) + CRLF;
  for I := 0 to High(AItems) do
    Result := Result + Bulk(AItems[I]);
end;

// Cliente amarrado a uma conexao sobre o servidor falso. Quem chama e' dono do
// cliente; o cliente e' dono da conexao, e a conexao, do stream.
function NovoCliente(out AFake: TRedisFakeServerStream;
  const AResponses: array of string): TRedisClient;
var
  LConn: TRedisConnection;
begin
  AFake := TRedisFakeServerStream.Create(AResponses);
  LConn := TRedisConnection.CreateOnStream(AFake, RedisDefaultParams);
  try
    // Com os parametros padrao (RESP2, sem senha, sem nome, banco 0) o
    // handshake nao emite nada: o primeiro byte no fio ja' e' o comando do
    // teste.
    LConn.Open;
    Result := TRedisClient.CreateOnConnection(LConn, True);
  except
    LConn.Free;
    raise;
  end;
end;

{ TRedisCommandHelpersTests }

procedure TRedisCommandHelpersTests.RedisArgs_ConcatenaNaOrdem;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs(['chave'], ['a', 'b']);
  TAssert.AssertEquals(3, Length(LArgs));
  TAssert.AssertEquals('chave', RedisUtf8Decode(LArgs[0].Bytes));
  TAssert.AssertEquals('a', RedisUtf8Decode(LArgs[1].Bytes));
  TAssert.AssertEquals('b', RedisUtf8Decode(LArgs[2].Bytes));
end;

procedure TRedisCommandHelpersTests.RedisArgs_ListaVaziaDosDoisLados;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([], []);
  TAssert.AssertEquals(0, Length(LArgs));
end;

procedure TRedisCommandHelpersTests.RedisAddArg_AcrescentaNoFim;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs(['chave'], []);
  RedisAddArg(LArgs, 'COUNT');
  RedisAddArg(LArgs, 10);
  TAssert.AssertEquals(3, Length(LArgs));
  TAssert.AssertEquals('COUNT', RedisUtf8Decode(LArgs[1].Bytes));
  TAssert.AssertEquals('10', RedisUtf8Decode(LArgs[2].Bytes));
end;

procedure TRedisCommandHelpersTests.ReplyToStrings_NuloViraListaVazia;
begin
  // Chave ausente responde nulo, nao array vazio. Levantar aqui obrigaria todo
  // chamador a testar antes; devolver lista vazia e' o que ele faria de
  // qualquer jeito.
  TAssert.AssertEquals(0, Length(RedisReplyToStrings(RedisNull)));
end;

procedure TRedisCommandHelpersTests.ReplyToStrings_ItemNuloViraStringVazia;
var
  LLista: TRedisStringArray;
begin
  LLista := RedisReplyToStrings(RedisArrayOf([RedisBulk('a'), RedisNull]));
  TAssert.AssertEquals(2, Length(LLista));
  TAssert.AssertEquals('a', LLista[0]);
  TAssert.AssertEquals('', LLista[1]);
end;

procedure TRedisCommandHelpersTests.ReplyToStrings_EscalarLevanta;
var
  LLevantou: Boolean;
begin
  LLevantou := False;
  try
    RedisReplyToStrings(RedisInteger(7));
  except
    on E: ERedisTypeError do
      LLevantou := True;
  end;
  TAssert.AssertTrue('inteiro nao e lista', LLevantou);
end;

procedure TRedisCommandHelpersTests.ReplyToBooleans_ZeroEUm;
var
  LLista: TRedisBooleanArray;
begin
  LLista := RedisReplyToBooleans(
    RedisArrayOf([RedisInteger(1), RedisInteger(0), RedisInteger(1)]));
  TAssert.AssertEquals(3, Length(LLista));
  TAssert.AssertTrue('primeiro', LLista[0]);
  TAssert.AssertFalse('segundo', LLista[1]);
  TAssert.AssertTrue('terceiro', LLista[2]);
end;

procedure TRedisCommandHelpersTests.ScoreBound_ExclusivoGanhaParentese;
begin
  TAssert.AssertEquals('10', RedisScoreBound(10));
  TAssert.AssertEquals('(10', RedisScoreBound(10, True));
end;

procedure TRedisCommandHelpersTests.ScoreBound_UsaPontoDecimal;
begin
  // Numa maquina em pt-BR o FloatToStr soltaria '1,5' e o servidor recusaria a
  // faixa. E' o bug que so' aparece na maquina do cliente.
  TAssert.AssertEquals('1.5', RedisScoreBound(1.5));
end;

{ TRedisClientTests }

procedure TRedisClientTests.Execute_SaiPelaConexaoAmarrada;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('valor')]);
  try
    TAssert.AssertEquals('valor', LClient.Execute('GET', ['chave']).AsString);
    TAssert.AssertEquals(Wire(['GET', 'chave']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientTests.ExecuteRaw_ErroDoServidor_NaoLevanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  LClient := NovoCliente(LFake, ['-WRONGTYPE tipo errado' + CRLF]);
  try
    LReply := LClient.ExecuteRaw('LPUSH', ['chave', 'v']);
    TAssert.AssertTrue('devia ser erro', LReply.IsError);
    TAssert.AssertEquals('WRONGTYPE', LReply.ErrorCode);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientTests.Execute_ErroDoServidor_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCodigo: string;
begin
  LCodigo := '';
  LClient := NovoCliente(LFake, ['-WRONGTYPE tipo errado' + CRLF]);
  try
    try
      LClient.Execute('LPUSH', ['chave', 'v']);
    except
      on E: ERedisReplyError do
        LCodigo := E.Code;
    end;
    TAssert.AssertEquals('WRONGTYPE', LCodigo);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientTests.Pipeline_PelaFachada;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF + ':2' + CRLF]);
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('SET', ['a', '1']);
    LPipe.Queue('INCR', ['b']);
    LReplies := LClient.ExecutePipeline(LPipe);
    TAssert.AssertEquals(2, Length(LReplies));
    TAssert.AssertEquals('OK', LReplies[0].AsString);
    TAssert.AssertEquals(Int64(2), LReplies[1].AsInteger);
  finally
    LPipe.Free;
    LClient.Free;
  end;
end;

procedure TRedisClientTests.Ping_PelaFachada;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['+PONG' + CRLF]);
  try
    TAssert.AssertTrue('PING', LClient.Ping);
    TAssert.AssertEquals(Wire(['PING']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientTests.Acquire_ConexaoUnica_DevolveSempreAMesma;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LPrimeira, LSegunda: TRedisConnection;
begin
  LClient := NovoCliente(LFake, []);
  try
    LPrimeira := LClient.Acquire;
    LClient.Release(LPrimeira);
    LSegunda := LClient.Acquire;
    LClient.Release(LSegunda);
    TAssert.AssertTrue('mesma conexao', LPrimeira = LSegunda);
    TAssert.AssertTrue('e e a do cliente', LPrimeira = LClient.Connection);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientTests.Release_ConexaoUnica_NaoDestroiAConexao;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LConn: TRedisConnection;
begin
  LClient := NovoCliente(LFake, [Bulk('v')]);
  try
    LConn := LClient.Acquire;
    // No modo conexao unica o Release e' no-op: se destruisse, o comando
    // seguinte cairia num ponteiro morto. E' o que permite escrever o
    // try/finally identico nos dois modos.
    LClient.Release(LConn);
    TAssert.AssertTrue('continua aberta', LConn.IsOpen);
    TAssert.AssertEquals('v', LClient.Execute('GET', ['k']).AsString);
  finally
    LClient.Free;
  end;
end;

procedure TRedisClientTests.Familias_UsamOMesmoExecutor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, []);
  try
    TAssert.AssertTrue('keys', LClient.Keys.Executor = LClient);
    TAssert.AssertTrue('strings', LClient.Strings.Executor = LClient);
    TAssert.AssertTrue('hashes', LClient.Hashes.Executor = LClient);
    TAssert.AssertTrue('lists', LClient.Lists.Executor = LClient);
    TAssert.AssertTrue('sets', LClient.Sets.Executor = LClient);
    TAssert.AssertTrue('zsets', LClient.ZSets.Executor = LClient);
  finally
    LClient.Free;
  end;
end;

{ TRedisKeysCommandsTests }

procedure TRedisKeysCommandsTests.Del_UmaChave_CodificaEDevolveBooleano;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    TAssert.AssertTrue('apagou', LClient.Keys.Del('chave'));
    TAssert.AssertEquals(Wire(['DEL', 'chave']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.DelMany_ContaAsApagadas;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':2' + CRLF]);
  try
    TAssert.AssertEquals(Int64(2), LClient.Keys.DelMany(['a', 'b', 'c']));
    TAssert.AssertEquals(Wire(['DEL', 'a', 'b', 'c']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.DelMany_SemChave_NaoVaiAoServidor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  // DEL sem chave seria erro de sintaxe no servidor. Devolver 0 sem sair de
  // casa e' o que o chamador esperaria de "apague estas zero chaves".
  LClient := NovoCliente(LFake, []);
  try
    TAssert.AssertEquals(Int64(0), LClient.Keys.DelMany([]));
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Exists_ZeroEhFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':0' + CRLF]);
  try
    TAssert.AssertFalse('nao existe', LClient.Keys.Exists('chave'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.ExistsMany_ContaRepeticoes;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':2' + CRLF]);
  try
    TAssert.AssertEquals(Int64(2), LClient.Keys.ExistsMany(['k', 'k']));
    TAssert.AssertEquals(Wire(['EXISTS', 'k', 'k']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Expire_CodificaOsSegundos;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    TAssert.AssertTrue('marcou', LClient.Keys.Expire('chave', 60));
    TAssert.AssertEquals(Wire(['EXPIRE', 'chave', '60']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Ttl_ChaveAusente_DevolveMenosDois;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':-2' + CRLF]);
  try
    TAssert.AssertEquals(Int64(REDIS_TTL_NO_KEY), LClient.Keys.Ttl('chave'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.KeyType_DevolveOTipo;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['+hash' + CRLF]);
  try
    TAssert.AssertEquals('hash', LClient.Keys.KeyType('chave'));
    TAssert.AssertEquals(Wire(['TYPE', 'chave']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Rename_NaoDevolveNada;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LClient.Keys.Rename('velha', 'nova');
    TAssert.AssertEquals(Wire(['RENAME', 'velha', 'nova']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.RenameNx_ZeroEhFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':0' + CRLF]);
  try
    TAssert.AssertFalse('destino ocupado', LClient.Keys.RenameNx('a', 'b'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.CopyKey_ComReplace_MandaOModificador;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    TAssert.AssertTrue('copiou', LClient.Keys.CopyKey('a', 'b', True));
    TAssert.AssertEquals(Wire(['COPY', 'a', 'b', 'REPLACE']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.CopyKey_SemReplace_NaoMandaOModificador;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':0' + CRLF]);
  try
    LClient.Keys.CopyKey('a', 'b');
    TAssert.AssertEquals(Wire(['COPY', 'a', 'b']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Keys_ListaAsChaves;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LChaves: TRedisStringArray;
begin
  LClient := NovoCliente(LFake, [Arr(['user:1', 'user:2'])]);
  try
    LChaves := LClient.Keys.Keys('user:*');
    TAssert.AssertEquals(2, Length(LChaves));
    TAssert.AssertEquals('user:1', LChaves[0]);
    TAssert.AssertEquals('user:2', LChaves[1]);
    TAssert.AssertEquals(Wire(['KEYS', 'user:*']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Scan_SemFiltro_MandaSoOCursor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCursor: Int64;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + Bulk('0') + Arr([])]);
  try
    LCursor := 0;
    LClient.Keys.Scan(LCursor);
    TAssert.AssertEquals(Wire(['SCAN', '0']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Scan_ComMatchCountETipo_NaOrdem;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCursor: Int64;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + Bulk('0') + Arr([])]);
  try
    LCursor := 17;
    LClient.Keys.Scan(LCursor, 'user:*', 100, 'hash');
    TAssert.AssertEquals(
      Wire(['SCAN', '17', 'MATCH', 'user:*', 'COUNT', '100', 'TYPE', 'hash']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Scan_AtualizaOCursorEDevolveOLote;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCursor: Int64;
  LLote: TRedisStringArray;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + Bulk('42') + Arr(['a', 'b'])]);
  try
    LCursor := 0;
    LLote := LClient.Keys.Scan(LCursor);
    // O cursor volta pelo parametro: e' ele, e nao a quantidade de itens, que
    // diz se a varredura acabou.
    TAssert.AssertEquals(Int64(42), LCursor);
    TAssert.AssertEquals(2, Length(LLote));
    TAssert.AssertEquals('a', LLote[0]);
  finally
    LClient.Free;
  end;
end;

procedure TRedisKeysCommandsTests.Scan_RespostaForaDoFormato_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCursor: Int64;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, [':7' + CRLF]);
  try
    LCursor := 0;
    try
      LClient.Keys.Scan(LCursor);
    except
      on E: ERedisTypeError do
        LLevantou := True;
    end;
    TAssert.AssertTrue('inteiro nao e par [cursor, lote]', LLevantou);
  finally
    LClient.Free;
  end;
end;

{ TRedisStringsCommandsTests }

procedure TRedisStringsCommandsTests.SetValue_CodificaSetSimples;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LClient.Strings.SetValue('chave', 'valor');
    TAssert.AssertEquals(Wire(['SET', 'chave', 'valor']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.SetWithOptions_NxGetEExpiracao_NaOrdemDaEspecificacao;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LOpc := RedisDefaultSetOptions;
    LOpc.Condition := scNotExists;
    LOpc.Expiry := seSeconds;
    LOpc.ExpiryValue := 30;
    LOpc.ReturnOldValue := True;
    LClient.Strings.SetWithOptions('lock', 'token', LOpc);
    // NX antes de GET antes da expiracao: fora dessa ordem o servidor recusa
    // com "syntax error", e o teste de integracao so' veria o erro, nao o
    // motivo.
    TAssert.AssertEquals(
      Wire(['SET', 'lock', 'token', 'NX', 'GET', 'EX', '30']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.SetWithOptions_KeepTtl_NaoMandaNumero;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LOpc := RedisDefaultSetOptions;
    LOpc.Expiry := seKeepTtl;
    LOpc.ExpiryValue := 99;  // ignorado de proposito
    LClient.Strings.SetWithOptions('chave', 'valor', LOpc);
    TAssert.AssertEquals(Wire(['SET', 'chave', 'valor', 'KEEPTTL']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.SetWithOptions_NxBarrado_DevolveNulo;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
  LReply: IRedisReply;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    LOpc := RedisDefaultSetOptions;
    LOpc.Condition := scNotExists;
    LReply := LClient.Strings.SetWithOptions('lock', 'token', LOpc);
    // Nulo aqui significa "a chave ja' existia e eu nao gravei" — no lock
    // distribuido e' a resposta mais importante das duas.
    TAssert.AssertTrue('nao gravou', LReply.IsNull);
    TAssert.AssertFalse('e AsBoolean concorda', LReply.AsBoolean);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.SetWithOptions_XxEExAt;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LOpc := RedisDefaultSetOptions;
    LOpc.Condition := scExists;
    LOpc.Expiry := seUnixSeconds;
    LOpc.ExpiryValue := 1700000000;
    LClient.Strings.SetWithOptions('chave', 'valor', LOpc);
    TAssert.AssertEquals(
      Wire(['SET', 'chave', 'valor', 'XX', 'EXAT', '1700000000']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.Get_ChaveAusente_EhNuloENaoVazio;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    TAssert.AssertTrue('nulo', LClient.Strings.Get('chave').IsNull);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.TryGet_ChaveAusente_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LValor: string;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    TAssert.AssertFalse('ausente', LClient.Strings.TryGet('chave', LValor));
    TAssert.AssertEquals('', LValor);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.TryGet_ValorVazio_DevolveTrue;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LValor: string;
begin
  LClient := NovoCliente(LFake, [Bulk('')]);
  try
    // Chave que existe e vale '' NAO e' chave ausente. E' a diferenca que o
    // GetString sozinho nao consegue mostrar.
    TAssert.AssertTrue('existe', LClient.Strings.TryGet('chave', LValor));
    TAssert.AssertEquals('', LValor);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.GetString_ChaveAusente_DevolveVazio;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    TAssert.AssertEquals('', LClient.Strings.GetString('chave'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.SetNx_ZeroEhFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':0' + CRLF]);
  try
    TAssert.AssertFalse('ja existia', LClient.Strings.SetNx('chave', 'v'));
    TAssert.AssertEquals(Wire(['SETNX', 'chave', 'v']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.SetEx_CodificaNaOrdemChavePrazoValor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LClient.Strings.SetEx('cache', 60, 'json');
    // O prazo vem ANTES do valor no SETEX, ao contrario de quase todo comando
    // do Redis. Trocar os dois grava o prazo como valor.
    TAssert.AssertEquals(Wire(['SETEX', 'cache', '60', 'json']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.Incr_DevolveOInteiro;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':43' + CRLF]);
  try
    TAssert.AssertEquals(Int64(43), LClient.Strings.Incr('contador'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.IncrByFloat_MandaPontoDecimal;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('1.5')]);
  try
    LClient.Strings.IncrByFloat('saldo', 1.5);
    TAssert.AssertEquals(Wire(['INCRBYFLOAT', 'saldo', '1.5']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.IncrByFloat_LeODoubleDaResposta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  // Em RESP2 o INCRBYFLOAT responde bulk string; o AsDouble entende os dois.
  LClient := NovoCliente(LFake, [Bulk('10.25')]);
  try
    TAssert.AssertEquals(10.25, LClient.Strings.IncrByFloat('saldo', 0.25),
      0.0001);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.MSet_QuantidadeImpar_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Strings.MSet(['a', '1', 'b']);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('par incompleto', LLevantou);
    // E o comando torto nao chegou a sair: o servidor responderia com um erro
    // de sintaxe que nao ajudaria ninguem a achar o par faltante.
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.MSet_CodificaOsPares;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LClient.Strings.MSet(['a', '1', 'b', '2']);
    TAssert.AssertEquals(Wire(['MSET', 'a', '1', 'b', '2']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.MGet_MantemONuloNoMeio;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  LClient := NovoCliente(LFake,
    ['*3' + CRLF + Bulk('um') + '$-1' + CRLF + Bulk('tres')]);
  try
    LReply := LClient.Strings.MGet(['a', 'b', 'c']);
    TAssert.AssertEquals(3, LReply.Count);
    TAssert.AssertEquals('um', LReply[0].AsString);
    // Rebaixar este nulo para '' apagaria a unica informacao que o MGET tem a
    // dar sobre a chave 'b'.
    TAssert.AssertTrue('a do meio nao existe', LReply[1].IsNull);
    TAssert.AssertEquals('tres', LReply[2].AsString);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.MGet_SemChave_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Strings.MGet([]);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('MGET sem chave', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.GetRange_CodificaOsIndices;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('abc')]);
  try
    TAssert.AssertEquals('abc', LClient.Strings.GetRange('chave', 0, -1));
    TAssert.AssertEquals(Wire(['GETRANGE', 'chave', '0', '-1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStringsCommandsTests.StrLen_DevolveOTamanho;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':7' + CRLF]);
  try
    TAssert.AssertEquals(Int64(7), LClient.Strings.StrLen('chave'));
  finally
    LClient.Free;
  end;
end;

{ TRedisHashesCommandsTests }

procedure TRedisHashesCommandsTests.HSet_DevolveQuantosCamposNasceram;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    TAssert.AssertEquals(Int64(1), LClient.Hashes.HSet('h', 'campo', 'v'));
    TAssert.AssertEquals(Wire(['HSET', 'h', 'campo', 'v']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HSetMany_CodificaOsPares;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':2' + CRLF]);
  try
    LClient.Hashes.HSetMany('h', ['a', '1', 'b', '2']);
    TAssert.AssertEquals(Wire(['HSET', 'h', 'a', '1', 'b', '2']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HSetMany_QuantidadeImpar_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Hashes.HSetMany('h', ['a', '1', 'b']);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('par incompleto', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HGetAll_Resp2Array_AcessoPorCampo;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  LClient := NovoCliente(LFake, [Arr(['ip', '10.0.0.1', 'user', 'ana'])]);
  try
    LReply := LClient.Hashes.HGetAll('session:42');
    TAssert.AssertEquals(4, LReply.Count);
    TAssert.AssertEquals('10.0.0.1', LReply.ValueByKey('ip').AsString);
    TAssert.AssertEquals('ana', LReply.ValueByKey('user').AsString);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HGetAll_Resp3Mapa_MesmaFormaQueOResp2;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  // O mesmo HGETALL respondido como MAPA do RESP3. A leitura e' agnostica de
  // protocolo, e o mapa fica achatado — entao Count e ValueByKey dao o MESMO
  // resultado do teste anterior. E' o que permite a aplicacao trocar de
  // protocolo sem tocar no codigo. Ver a decisao 13 em docs/DECISOES.md.
  LClient := NovoCliente(LFake,
    ['%2' + CRLF + Bulk('ip') + Bulk('10.0.0.1') + Bulk('user') + Bulk('ana')]);
  try
    LReply := LClient.Hashes.HGetAll('session:42');
    TAssert.AssertEquals(4, LReply.Count);
    TAssert.AssertEquals('10.0.0.1', LReply.ValueByKey('ip').AsString);
    TAssert.AssertEquals('ana', LReply.ValueByKey('user').AsString);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HTryGet_CampoAusente_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LValor: string;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    TAssert.AssertFalse('ausente', LClient.Hashes.HTryGet('h', 'x', LValor));
    TAssert.AssertEquals('', LValor);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HMGet_MantemONuloNoMeio;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + Bulk('v') + '$-1' + CRLF]);
  try
    LReply := LClient.Hashes.HMGet('h', ['a', 'b']);
    TAssert.AssertEquals(2, LReply.Count);
    TAssert.AssertTrue('campo ausente', LReply[1].IsNull);
    TAssert.AssertEquals(Wire(['HMGET', 'h', 'a', 'b']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HDel_ZeroEhFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':0' + CRLF]);
  try
    TAssert.AssertFalse('campo nao existia', LClient.Hashes.HDel('h', 'x'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HIncrByFloat_MandaPontoDecimal;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('2.5')]);
  try
    TAssert.AssertEquals(2.5, LClient.Hashes.HIncrByFloat('h', 'saldo', 0.5),
      0.0001);
    TAssert.AssertEquals(Wire(['HINCRBYFLOAT', 'h', 'saldo', '0.5']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HKeys_ListaOsCampos;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCampos: TRedisStringArray;
begin
  LClient := NovoCliente(LFake, [Arr(['ip', 'user'])]);
  try
    LCampos := LClient.Hashes.HKeys('h');
    TAssert.AssertEquals(2, Length(LCampos));
    TAssert.AssertEquals('ip', LCampos[0]);
  finally
    LClient.Free;
  end;
end;

procedure TRedisHashesCommandsTests.HScan_MontaMatchECount;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCursor: Int64;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + Bulk('0') + Arr([])]);
  try
    LCursor := 0;
    LClient.Hashes.HScan('h', LCursor, 'a*', 50);
    TAssert.AssertEquals(
      Wire(['HSCAN', 'h', '0', 'MATCH', 'a*', 'COUNT', '50']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

{ TRedisListsCommandsTests }

procedure TRedisListsCommandsTests.LPushMany_MandaTudoNumComandoSo;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':3' + CRLF]);
  try
    TAssert.AssertEquals(Int64(3), LClient.Lists.LPushMany('fila', ['a', 'b', 'c']));
    TAssert.AssertEquals(Wire(['LPUSH', 'fila', 'a', 'b', 'c']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.LPushMany_SemValor_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Lists.LPushMany('fila', []);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('LPUSH sem valor', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.LRange_CodificaEDevolveALista;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LItens: TRedisStringArray;
begin
  LClient := NovoCliente(LFake, [Arr(['a', 'b'])]);
  try
    LItens := LClient.Lists.LRange('fila', 0, -1);
    TAssert.AssertEquals(2, Length(LItens));
    TAssert.AssertEquals('b', LItens[1]);
    TAssert.AssertEquals(Wire(['LRANGE', 'fila', '0', '-1']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.LPop_ListaVazia_EhNulo;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    TAssert.AssertTrue('vazia', LClient.Lists.LPop('fila').IsNull);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.LInsert_Before_MandaAPalavraCerta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':4' + CRLF]);
  try
    LClient.Lists.LInsert('fila', iwBefore, 'pivo', 'novo');
    TAssert.AssertEquals(Wire(['LINSERT', 'fila', 'BEFORE', 'pivo', 'novo']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.LInsert_After_MandaAPalavraCerta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':4' + CRLF]);
  try
    LClient.Lists.LInsert('fila', iwAfter, 'pivo', 'novo');
    TAssert.AssertEquals(Wire(['LINSERT', 'fila', 'AFTER', 'pivo', 'novo']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.LMove_MandaAsDuasPontas;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('tarefa')]);
  try
    LClient.Lists.LMove('fila', 'processando', leRight, leLeft);
    TAssert.AssertEquals(
      Wire(['LMOVE', 'fila', 'processando', 'RIGHT', 'LEFT']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.BLPop_PrazoVencido_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LChave, LValor: string;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    // Prazo vencido nao e' erro: e' o estado normal de um worker ocioso.
    TAssert.AssertFalse('nada chegou',
      LClient.Lists.BLPop(['fila'], 1, LChave, LValor));
    TAssert.AssertEquals('', LChave);
    TAssert.AssertEquals('', LValor);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.BLPop_DevolveChaveEValor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LChave, LValor: string;
begin
  LClient := NovoCliente(LFake, [Arr(['fila:b', 'tarefa'])]);
  try
    TAssert.AssertTrue('chegou',
      LClient.Lists.BLPop(['fila:a', 'fila:b'], 5, LChave, LValor));
    // Com varias chaves na chamada, so' a resposta diz de qual delas veio.
    TAssert.AssertEquals('fila:b', LChave);
    TAssert.AssertEquals('tarefa', LValor);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.BLPop_CodificaOTimeoutNoFim;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LChave, LValor: string;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LClient.Lists.BLPop(['fila:a', 'fila:b'], 2, LChave, LValor);
    TAssert.AssertEquals(Wire(['BLPOP', 'fila:a', 'fila:b', '2']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.BLPop_SemChave_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LChave, LValor: string;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Lists.BLPop([], 1, LChave, LValor);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('BLPOP sem chave', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.BLPop_RestauraOTimeoutDaConexao;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LChave, LValor: string;
  LAntes: Integer;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LAntes := LClient.Connection.Params.ReceiveTimeoutMs;
    LClient.Lists.BLPop(['fila'], 30, LChave, LValor);
    // O prazo esticado vale so' durante o comando. Deixa-lo esticado faria o
    // proximo GET esperar 32 s por um servidor que ja' morreu.
    TAssert.AssertEquals(LAntes, LClient.Connection.Params.ReceiveTimeoutMs);
  finally
    LClient.Free;
  end;
end;

procedure TRedisListsCommandsTests.BLMove_CodificaPontasETimeout;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    LClient.Lists.BLMove('fila', 'processando', leRight, leLeft, 3);
    TAssert.AssertEquals(
      Wire(['BLMOVE', 'fila', 'processando', 'RIGHT', 'LEFT', '3']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

{ TRedisSetsCommandsTests }

procedure TRedisSetsCommandsTests.SAdd_MembroNovo_DevolveTrue;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    TAssert.AssertTrue('entrou', LClient.Sets.SAdd('tags', 'redis'));
    TAssert.AssertEquals(Wire(['SADD', 'tags', 'redis']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SAdd_MembroRepetido_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':0' + CRLF]);
  try
    // Zero aqui nao e' falha: e' "ja' estava no conjunto".
    TAssert.AssertFalse('ja estava', LClient.Sets.SAdd('tags', 'redis'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SAddMany_CodificaTodos;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':2' + CRLF]);
  try
    TAssert.AssertEquals(Int64(2), LClient.Sets.SAddMany('tags', ['a', 'b']));
    TAssert.AssertEquals(Wire(['SADD', 'tags', 'a', 'b']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SMembers_ListaOsMembros;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LMembros: TRedisStringArray;
begin
  LClient := NovoCliente(LFake, [Arr(['a', 'b', 'c'])]);
  try
    LMembros := LClient.Sets.SMembers('tags');
    TAssert.AssertEquals(3, Length(LMembros));
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SMIsMember_MapeiaZeroEUm;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LResp: TRedisBooleanArray;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + ':1' + CRLF + ':0' + CRLF]);
  try
    LResp := LClient.Sets.SMIsMember('tags', ['a', 'b']);
    TAssert.AssertEquals(2, Length(LResp));
    TAssert.AssertTrue('a pertence', LResp[0]);
    TAssert.AssertFalse('b nao pertence', LResp[1]);
    TAssert.AssertEquals(Wire(['SMISMEMBER', 'tags', 'a', 'b']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SInterStore_DestinoAntesDasChaves;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':2' + CRLF]);
  try
    LClient.Sets.SInterStore('destino', ['a', 'b']);
    TAssert.AssertEquals(Wire(['SINTERSTORE', 'destino', 'a', 'b']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SInterCard_MandaAQuantidadeDeChaves;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    LClient.Sets.SInterCard(['a', 'b']);
    // O numero de chaves vai explicito: sem ele o servidor nao sabe onde a
    // lista termina e o LIMIT comeca.
    TAssert.AssertEquals(Wire(['SINTERCARD', '2', 'a', 'b']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SInterCard_ComLimite;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':5' + CRLF]);
  try
    LClient.Sets.SInterCard(['a', 'b'], 5);
    TAssert.AssertEquals(Wire(['SINTERCARD', '2', 'a', 'b', 'LIMIT', '5']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SScan_MontaMatchECount;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCursor: Int64;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + Bulk('0') + Arr([])]);
  try
    LCursor := 0;
    LClient.Sets.SScan('tags', LCursor, 'r*', 20);
    TAssert.AssertEquals(
      Wire(['SSCAN', 'tags', '0', 'MATCH', 'r*', 'COUNT', '20']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisSetsCommandsTests.SInter_SemChave_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Sets.SInter([]);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('SINTER sem chave', LLevantou);
  finally
    LClient.Free;
  end;
end;

{ TRedisZSetsCommandsTests }

procedure TRedisZSetsCommandsTests.ZAdd_FormataOScoreComPonto;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    LClient.ZSets.ZAdd('ranking', 1500.5, 'fabiano');
    TAssert.AssertEquals(Wire(['ZADD', 'ranking', '1500.5', 'fabiano']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZAddOpt_NxECh_NaOrdemDaEspecificacao;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    LClient.ZSets.ZAddOpt('ranking', 10, 'ana', zaNotExists, True);
    // Os modificadores vem ANTES do par score/membro.
    TAssert.AssertEquals(Wire(['ZADD', 'ranking', 'NX', 'CH', '10', 'ana']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZAddOpt_SemCondicao_NaoMandaModificador;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    LClient.ZSets.ZAddOpt('ranking', 10, 'ana', zaAlways);
    TAssert.AssertEquals(Wire(['ZADD', 'ranking', '10', 'ana']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZAddMany_QuantidadeImpar_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.ZSets.ZAddMany('ranking', [1, 'ana', 2]);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('par incompleto', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZRangeWithScores_Resp2Achatado;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LPares: TRedisScoreMemberArray;
begin
  LClient := NovoCliente(LFake, [Arr(['ana', '1.5', 'bob', '2.5'])]);
  try
    LPares := LClient.ZSets.ZRangeWithScores('ranking', 0, -1);
    TAssert.AssertEquals(2, Length(LPares));
    TAssert.AssertEquals('ana', LPares[0].Member);
    TAssert.AssertEquals(1.5, LPares[0].Score, 0.0001);
    TAssert.AssertEquals('bob', LPares[1].Member);
    TAssert.AssertEquals(2.5, LPares[1].Score, 0.0001);
    TAssert.AssertEquals(Wire(['ZRANGE', 'ranking', '0', '-1', 'WITHSCORES']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZRangeWithScores_Resp3Pares_MesmoResultado;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LPares: TRedisScoreMemberArray;
begin
  // Em RESP3 o mesmo comando responde uma lista de PARES, com o score como
  // double nativo. Sem esta conversao, o codigo da aplicacao teria de ramificar
  // por protocolo — que e' exatamente o que a lib promete evitar.
  LClient := NovoCliente(LFake,
    ['*2' + CRLF +
     '*2' + CRLF + Bulk('ana') + ',1.5' + CRLF +
     '*2' + CRLF + Bulk('bob') + ',2.5' + CRLF]);
  try
    LPares := LClient.ZSets.ZRangeWithScores('ranking', 0, -1);
    TAssert.AssertEquals(2, Length(LPares));
    TAssert.AssertEquals('ana', LPares[0].Member);
    TAssert.AssertEquals(1.5, LPares[0].Score, 0.0001);
    TAssert.AssertEquals('bob', LPares[1].Member);
    TAssert.AssertEquals(2.5, LPares[1].Score, 0.0001);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZRangeWithScores_TamanhoImpar_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, [Arr(['ana', '1.5', 'bob'])]);
  try
    try
      LClient.ZSets.ZRangeWithScores('ranking', 0, -1);
    except
      on E: ERedisTypeError do
        LLevantou := True;
    end;
    TAssert.AssertTrue('membro sem score', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZRangeByScore_SemLimite_NaoMandaLimit;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Arr([])]);
  try
    LClient.ZSets.ZRangeByScore('ranking', REDIS_SCORE_MIN, '(100');
    TAssert.AssertEquals(Wire(['ZRANGEBYSCORE', 'ranking', '-inf', '(100']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZRangeByScore_ComLimite_MandaOffsetECount;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Arr([])]);
  try
    LClient.ZSets.ZRangeByScore('ranking', '0', REDIS_SCORE_MAX, 20, 10);
    TAssert.AssertEquals(
      Wire(['ZRANGEBYSCORE', 'ranking', '0', '+inf', 'LIMIT', '20', '10']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZRevRangeByScore_InverteMaxEMin;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Arr([])]);
  try
    // No ZREVRANGEBYSCORE o MAIOR vem primeiro. Passar (min, max) por habito
    // devolve lista vazia em silencio, sem erro nenhum.
    LClient.ZSets.ZRevRangeByScore('ranking', REDIS_SCORE_MAX, '0');
    TAssert.AssertEquals(Wire(['ZREVRANGEBYSCORE', 'ranking', '+inf', '0']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZTryScore_MembroAusente_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LScore: Double;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    // Ausente e' diferente de score zero — por isso o retorno nao e' Double
    // puro.
    TAssert.AssertFalse('nao esta no conjunto',
      LClient.ZSets.ZTryScore('ranking', 'ninguem', LScore));
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZTryScore_LeODoubleDoResp3;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LScore: Double;
begin
  LClient := NovoCliente(LFake, [',1500.5' + CRLF]);
  try
    TAssert.AssertTrue('esta la',
      LClient.ZSets.ZTryScore('ranking', 'ana', LScore));
    TAssert.AssertEquals(1500.5, LScore, 0.0001);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZTryRank_MembroAusente_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LRank: Int64;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  try
    TAssert.AssertFalse('sem posicao',
      LClient.ZSets.ZTryRank('ranking', 'ninguem', LRank));
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZPopMin_ConjuntoVazio_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LMembro: string;
  LScore: Double;
begin
  LClient := NovoCliente(LFake, [Arr([])]);
  try
    TAssert.AssertFalse('vazio',
      LClient.ZSets.ZPopMin('ranking', LMembro, LScore));
    TAssert.AssertEquals('', LMembro);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZPopMin_DevolveMembroEScore;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LMembro: string;
  LScore: Double;
begin
  LClient := NovoCliente(LFake, [Arr(['ana', '1.5'])]);
  try
    TAssert.AssertTrue('tirou',
      LClient.ZSets.ZPopMin('ranking', LMembro, LScore));
    TAssert.AssertEquals('ana', LMembro);
    TAssert.AssertEquals(1.5, LScore, 0.0001);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZIncrBy_MandaDeltaComPonto;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('11.5')]);
  try
    TAssert.AssertEquals(11.5, LClient.ZSets.ZIncrBy('ranking', 1.5, 'ana'),
      0.0001);
    TAssert.AssertEquals(Wire(['ZINCRBY', 'ranking', '1.5', 'ana']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisZSetsCommandsTests.ZScan_MontaMatchECount;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCursor: Int64;
begin
  LClient := NovoCliente(LFake, ['*2' + CRLF + Bulk('0') + Arr([])]);
  try
    LCursor := 0;
    LClient.ZSets.ZScan('ranking', LCursor, 'a*', 30);
    TAssert.AssertEquals(
      Wire(['ZSCAN', 'ranking', '0', 'MATCH', 'a*', 'COUNT', '30']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

{ Helpers das fixtures de stream }

// Um inteiro RESP2 no fio.
function Inteiro(AValue: Int64): string;
begin
  Result := ':' + IntToStr(AValue) + CRLF;
end;

// Agregado ja' montado a partir de pedacos prontos (array de arrays).
function Agregado(const AItens: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AItens)) + CRLF;
  for I := 0 to High(AItens) do
    Result := Result + AItens[I];
end;

// Uma entrada de stream no fio: [id, [campo, valor, ...]].
function Entrada(const AId: string; const ACampos: array of string): string;
begin
  Result := '*2' + CRLF + Bulk(AId) + Arr(ACampos);
end;

// Entrada que sobrou so' na PEL: o id existe, os campos vem NULOS porque a
// entrada ja' saiu do stream.
function EntradaApagada(const AId: string): string;
begin
  Result := '*2' + CRLF + Bulk(AId) + '*-1' + CRLF;
end;

// A resposta de um XREAD em RESP2: lista de pares [chave, entradas].
function StreamResp2(const AChave, AEntradas: string): string;
begin
  Result := Agregado([('*2' + CRLF) + Bulk(AChave) + AEntradas]);
end;

// A mesma resposta em RESP3: mapa chave -> entradas.
function StreamResp3(const AChave, AEntradas: string): string;
begin
  Result := '%1' + CRLF + Bulk(AChave) + AEntradas;
end;

{ TRedisStreamHelpersTests }

procedure TRedisStreamHelpersTests.StreamId_MontaMsEHifenESequencia;
begin
  TAssert.AssertEquals('1700000000000-0', RedisStreamId(1700000000000, 0));
  TAssert.AssertEquals('5-3', RedisStreamId(5, 3));
end;

procedure TRedisStreamHelpersTests.TryParseStreamId_QuebraNasDuasPartes;
var
  LMs, LSeq: Int64;
begin
  TAssert.AssertTrue('id valido',
    RedisTryParseStreamId('1700000000000-7', LMs, LSeq));
  TAssert.AssertEquals(Int64(1700000000000), LMs);
  TAssert.AssertEquals(Int64(7), LSeq);
end;

procedure TRedisStreamHelpersTests.TryParseStreamId_SemHifen_DevolveFalse;
var
  LMs, LSeq: Int64;
begin
  // '1700000000000' sozinho e' extremo VALIDO num XRANGE, mas nao e' um id
  // completo — quem precisa das duas partes tem de saber a diferenca.
  TAssert.AssertFalse('so o milissegundo',
    RedisTryParseStreamId('1700000000000', LMs, LSeq));
  TAssert.AssertFalse('texto qualquer',
    RedisTryParseStreamId('nao-numero', LMs, LSeq));
end;

procedure TRedisStreamHelpersTests.TryParseStreamId_AceitaOParenteseDeExtremoAberto;
var
  LMs, LSeq: Int64;
begin
  TAssert.AssertTrue('extremo aberto',
    RedisTryParseStreamId('(5-1', LMs, LSeq));
  TAssert.AssertEquals(Int64(5), LMs);
  TAssert.AssertEquals(Int64(1), LSeq);
end;

procedure TRedisStreamHelpersTests.StreamIdExclusive_AcrescentaOParentese;
begin
  TAssert.AssertEquals('(5-1', RedisStreamIdExclusive('5-1'));
end;

procedure TRedisStreamHelpersTests.StreamIdExclusive_NaoDuplicaOParentese;
begin
  // Paginar chamando duas vezes seguidas nao pode virar '((5-1', que o
  // servidor recusaria.
  TAssert.AssertEquals('(5-1', RedisStreamIdExclusive('(5-1'));
end;

procedure TRedisStreamHelpersTests.ReplyToStreamEntries_NuloViraListaVazia;
begin
  // Chave inexistente responde nulo; para quem le', "nao ha' entrada" e
  // "lista vazia" sao a mesma coisa.
  TAssert.AssertEquals(0, Length(RedisReplyToStreamEntries(RedisNull)));
end;

procedure TRedisStreamHelpersTests.ReplyToStreamEntries_ForaDoFormato_Levanta;
var
  LLevantou: Boolean;
begin
  LLevantou := False;
  try
    // Entrada precisa ser [id, campos]; um escalar solto nao e' entrada.
    RedisReplyToStreamEntries(RedisArrayOf([RedisBulk('5-1')]));
  except
    on E: ERedisTypeError do
      LLevantou := True;
  end;
  TAssert.AssertTrue('entrada malformada', LLevantou);
end;

procedure TRedisStreamHelpersTests.ReplyToStreamData_Resp2_ListaDePares;
var
  LDados: TRedisStreamDataArray;
begin
  LDados := RedisReplyToStreamData(RedisArrayOf([
    RedisArrayOf([RedisBulk('s:a'),
      RedisArrayOf([RedisArrayOf([RedisBulk('5-1'),
        RedisArrayOf([RedisBulk('c'), RedisBulk('1')])])])])]));
  TAssert.AssertEquals(1, Length(LDados));
  TAssert.AssertEquals('s:a', LDados[0].Key);
  TAssert.AssertEquals(1, Length(LDados[0].Entries));
  TAssert.AssertEquals('5-1', LDados[0].Entries[0].Id);
  TAssert.AssertEquals('1', LDados[0].Entries[0].FieldValue('c'));
end;

procedure TRedisStreamHelpersTests.ReplyToStreamData_Resp3_MapaAchatado;
var
  LDados: TRedisStreamDataArray;
begin
  // Em RESP3 o mesmo XREAD responde um MAPA, que o leitor guarda achatado:
  // chave, entradas, chave, entradas. O resultado tem de ser identico ao do
  // teste anterior — e' o que dispensa a aplicacao de ramificar por protocolo.
  LDados := RedisReplyToStreamData(RedisArrayOf([
    RedisBulk('s:a'),
    RedisArrayOf([RedisArrayOf([RedisBulk('5-1'),
      RedisArrayOf([RedisBulk('c'), RedisBulk('1')])])])]));
  TAssert.AssertEquals(1, Length(LDados));
  TAssert.AssertEquals('s:a', LDados[0].Key);
  TAssert.AssertEquals(1, Length(LDados[0].Entries));
  TAssert.AssertEquals('5-1', LDados[0].Entries[0].Id);
  TAssert.AssertEquals('1', LDados[0].Entries[0].FieldValue('c'));
end;

procedure TRedisStreamHelpersTests.FindStreamData_AchaPeloNomeENaoPelaPosicao;
var
  LDados: TRedisStreamDataArray;
begin
  // Chave sem novidade nao aparece na resposta: pedir ['a','b'] e receber so'
  // 'b' e' o caso NORMAL, e indexar por posicao leria a chave errada.
  SetLength(LDados, 1);
  LDados[0].Key := 'b';
  TAssert.AssertEquals(0, RedisFindStreamData(LDados, 'b'));
  TAssert.AssertEquals(-1, RedisFindStreamData(LDados, 'a'));
end;

{ TRedisStreamsCommandsTests }

procedure TRedisStreamsCommandsTests.XAdd_UsaAsteriscoEDevolveOId;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LId: string;
begin
  LClient := NovoCliente(LFake, [Bulk('1700000000000-0')]);
  try
    LId := LClient.Streams.XAdd('s:log', ['nivel', 'info', 'msg', 'subiu']);
    TAssert.AssertEquals(
      Wire(['XADD', 's:log', '*', 'nivel', 'info', 'msg', 'subiu']),
      LFake.WrittenText);
    TAssert.AssertEquals('1700000000000-0', LId);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAdd_SemCampo_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      // O Redis nao guarda entrada sem campo; recusar aqui poupa uma ida ao
      // servidor para receber um erro de sintaxe.
      LClient.Streams.XAdd('s:log', []);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('XADD sem campo', LLevantou);
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAdd_QuantidadeImpar_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Streams.XAdd('s:log', ['campo']);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('campo sem valor', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAddId_MandaOIdEscolhido;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('5-1')]);
  try
    LClient.Streams.XAddId('s:log', '5-1', ['a', '1']);
    TAssert.AssertEquals(Wire(['XADD', 's:log', '5-1', 'a', '1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAddMaxLen_Aproximado_MandaOTilAntesDoNumero;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('5-1')]);
  try
    LClient.Streams.XAddMaxLen('s:log', 1000, True, ['a', '1']);
    // A ordem nao e' livre: MAXLEN, o operador, o numero e SO' ENTAO o id.
    TAssert.AssertEquals(
      Wire(['XADD', 's:log', 'MAXLEN', '~', '1000', '*', 'a', '1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAddMaxLen_Exato_MandaOIgual;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('5-1')]);
  try
    LClient.Streams.XAddMaxLen('s:log', 10, False, ['a', '1']);
    TAssert.AssertEquals(
      Wire(['XADD', 's:log', 'MAXLEN', '=', '10', '*', 'a', '1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAddMinId_MandaMinIdAntesDoAsterisco;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('9-0')]);
  try
    LClient.Streams.XAddMinId('s:log', '5-0', True, ['a', '1']);
    TAssert.AssertEquals(
      Wire(['XADD', 's:log', 'MINID', '~', '5-0', '*', 'a', '1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XTrimMaxLen_AproximadoPorPadrao;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Inteiro(3)]);
  try
    // O padrao e' o '~' de proposito: trim exato num stream grande custa caro
    // num comando so'.
    TAssert.AssertEquals(3, LClient.Streams.XTrimMaxLen('s:log', 100));
    TAssert.AssertEquals(Wire(['XTRIM', 's:log', 'MAXLEN', '~', '100']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XTrimMinId_Exato;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Inteiro(2)]);
  try
    LClient.Streams.XTrimMinId('s:log', '9-0', False);
    TAssert.AssertEquals(Wire(['XTRIM', 's:log', 'MINID', '=', '9-0']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XLen_DevolveOTamanho;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Inteiro(42)]);
  try
    TAssert.AssertEquals(42, LClient.Streams.XLen('s:log'));
    TAssert.AssertEquals(Wire(['XLEN', 's:log']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XDel_ContaOsQueExistiam;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Inteiro(2)]);
  try
    TAssert.AssertEquals(2, LClient.Streams.XDel('s:log', ['5-1', '5-2', '9-9']));
    TAssert.AssertEquals(Wire(['XDEL', 's:log', '5-1', '5-2', '9-9']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XDel_SemId_NaoVaiAoServidor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, []);
  try
    TAssert.AssertEquals(0, LClient.Streams.XDel('s:log', []));
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRange_SemCount_NaoMandaCount;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*0' + CRLF]);
  try
    LClient.Streams.XRange('s:log', REDIS_STREAM_MIN_ID, REDIS_STREAM_MAX_ID);
    TAssert.AssertEquals(Wire(['XRANGE', 's:log', '-', '+']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRange_ComCount_MandaOModificador;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*0' + CRLF]);
  try
    LClient.Streams.XRange('s:log', '(5-1', '+', 10);
    TAssert.AssertEquals(Wire(['XRANGE', 's:log', '(5-1', '+', 'COUNT', '10']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRange_LeIdECamposPorNome;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LEntradas: TRedisStreamEntryArray;
begin
  LClient := NovoCliente(LFake, [Agregado([
    Entrada('5-1', ['nivel', 'info', 'msg', 'subiu']),
    Entrada('5-2', ['nivel', 'erro', 'msg', 'caiu'])])]);
  try
    LEntradas := LClient.Streams.XRange('s:log', '-', '+');
    TAssert.AssertEquals(2, Length(LEntradas));
    TAssert.AssertEquals('5-1', LEntradas[0].Id);
    TAssert.AssertEquals(2, LEntradas[0].FieldCount);
    TAssert.AssertEquals('info', LEntradas[0].FieldValue('nivel'));
    TAssert.AssertEquals('caiu', LEntradas[1].FieldValue('msg'));
    TAssert.AssertFalse('entrada viva', LEntradas[0].IsDeleted);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRange_CampoAusente_TryFieldValueDevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LEntradas: TRedisStreamEntryArray;
  LValor: string;
begin
  LClient := NovoCliente(LFake, [Agregado([Entrada('5-1', ['a', ''])])]);
  try
    LEntradas := LClient.Streams.XRange('s:log', '-', '+');
    // Campo que existe e vale '' NAO e' o mesmo que campo ausente — a mesma
    // distincao que o GET faz entre nulo e vazio.
    TAssert.AssertTrue('campo presente e vazio',
      LEntradas[0].TryFieldValue('a', LValor));
    TAssert.AssertEquals('', LValor);
    TAssert.AssertFalse('campo ausente',
      LEntradas[0].TryFieldValue('b', LValor));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRange_EntradaApagada_VemSemCampos;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LEntradas: TRedisStreamEntryArray;
begin
  LClient := NovoCliente(LFake, [Agregado([EntradaApagada('5-1')])]);
  try
    LEntradas := LClient.Streams.XRange('s:log', '-', '+');
    // Entrada apagada do stream que ficou na PEL: o id existe, os campos nao.
    // Levantar aqui quebraria a varredura da PEL, que e' onde isso acontece.
    TAssert.AssertEquals(1, Length(LEntradas));
    TAssert.AssertEquals('5-1', LEntradas[0].Id);
    TAssert.AssertTrue('sem campos', LEntradas[0].IsDeleted);
    TAssert.AssertEquals(0, LEntradas[0].FieldCount);
    TAssert.AssertEquals('', LEntradas[0].FieldValue('a'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRevRange_InverteOsExtremos;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*0' + CRLF]);
  try
    // No XREVRANGE o comando espera (fim, inicio) — trocar isso devolve lista
    // vazia em silencio, que e' o pior tipo de bug.
    LClient.Streams.XRevRange('s:log', '+', '-', 1);
    TAssert.AssertEquals(Wire(['XREVRANGE', 's:log', '+', '-', 'COUNT', '1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRead_MontaOBlocoStreams;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LClient.Streams.XRead(['s:a', 's:b'], ['0-0', '5-1']);
    // As chaves vao TODAS antes de TODOS os ids; intercalar e' o engano comum.
    TAssert.AssertEquals(
      Wire(['XREAD', 'STREAMS', 's:a', 's:b', '0-0', '5-1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRead_ComCount_CountAntesDeStreams;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LClient.Streams.XRead(['s:a'], ['0-0'], 5);
    // STREAMS tem de ser o ULTIMO modificador: tudo o que vier depois dele e'
    // lido como chave ou id.
    TAssert.AssertEquals(
      Wire(['XREAD', 'COUNT', '5', 'STREAMS', 's:a', '0-0']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRead_ChavesEIdsDesbalanceados_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Streams.XRead(['s:a', 's:b'], ['0-0']);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    // O servidor responderia "Unbalanced XREAD list of streams", que nao diz
    // onde esta' o engano. Aqui a mensagem cita as duas contagens.
    TAssert.AssertTrue('duas chaves, um id', LLevantou);
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRead_SemChave_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Streams.XRead([], []);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('XREAD sem chave', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRead_NadaNovo_NuloViraListaVazia;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    // Sem novidade o servidor responde nulo. Para quem le', isso e' "nada
    // chegou", nao erro.
    TAssert.AssertEquals(0,
      Length(LClient.Streams.XRead(['s:a'], ['$'])));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRead_Resp2_UmaEntradaPorChave;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LDados: TRedisStreamDataArray;
begin
  LClient := NovoCliente(LFake,
    [StreamResp2('s:a', Agregado([Entrada('5-1', ['c', '1'])]))]);
  try
    LDados := LClient.Streams.XRead(['s:a'], ['0-0']);
    TAssert.AssertEquals(1, Length(LDados));
    TAssert.AssertEquals('s:a', LDados[0].Key);
    TAssert.AssertEquals('5-1', LDados[0].Entries[0].Id);
    TAssert.AssertEquals('1', LDados[0].Entries[0].FieldValue('c'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XRead_Resp3_MapaAchatado_MesmoResultado;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LDados: TRedisStreamDataArray;
begin
  // O mesmo XREAD respondido como MAPA do RESP3. O resultado tem de ser
  // identico ao do teste anterior: e' o que dispensa a aplicacao de ramificar
  // por protocolo.
  LClient := NovoCliente(LFake,
    [StreamResp3('s:a', Agregado([Entrada('5-1', ['c', '1'])]))]);
  try
    LDados := LClient.Streams.XRead(['s:a'], ['0-0']);
    TAssert.AssertEquals(1, Length(LDados));
    TAssert.AssertEquals('s:a', LDados[0].Key);
    TAssert.AssertEquals('5-1', LDados[0].Entries[0].Id);
    TAssert.AssertEquals('1', LDados[0].Entries[0].FieldValue('c'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XReadBlocking_MandaBlockEmMilissegundos;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LClient.Streams.XReadBlocking(['s:a'], ['$'], 2000, 10);
    // BLOCK e' em MILISSEGUNDOS — ao contrario do timeout do BLPOP, que e' em
    // segundos. Passar 2 aqui esperaria 2 ms.
    TAssert.AssertEquals(
      Wire(['XREAD', 'COUNT', '10', 'BLOCK', '2000', 'STREAMS', 's:a', '$']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XReadBlocking_RestauraOTimeoutDaConexao;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LAntes: Integer;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LAntes := LClient.Connection.Params.ReceiveTimeoutMs;
    LClient.Streams.XReadBlocking(['s:a'], ['$'], 30000);
    // O prazo esticado vale so' durante o comando: deixa-lo esticado faria o
    // proximo comando esperar 32 s por um servidor que ja' morreu.
    TAssert.AssertEquals(LAntes, LClient.Connection.Params.ReceiveTimeoutMs);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XGroupCreate_ComMkStream;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  try
    LClient.Streams.XGroupCreate('s:log', 'g1', '0', True);
    TAssert.AssertEquals(
      Wire(['XGROUP', 'CREATE', 's:log', 'g1', '0', 'MKSTREAM']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XGroupTryCreate_BusyGroup_DevolveFalseSemLevantar;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake,
    ['-BUSYGROUP Consumer Group name already exists' + CRLF]);
  try
    // Grupo que ja' existe e' o estado desejado, nao falha: sem isto, todo
    // worker que sobe depois do primeiro precisaria de try/except.
    TAssert.AssertFalse('ja existia',
      LClient.Streams.XGroupTryCreate('s:log', 'g1', '$'));
    TAssert.AssertEquals(Wire(['XGROUP', 'CREATE', 's:log', 'g1', '$']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XGroupTryCreate_OutroErro_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake,
    ['-WRONGTYPE Operation against a key holding the wrong kind of value' + CRLF]);
  try
    try
      // So' BUSYGROUP e' recado; o resto continua sendo falha, senao um
      // WRONGTYPE viraria "o grupo ja' existe".
      LClient.Streams.XGroupTryCreate('s:log', 'g1', '$');
    except
      on E: ERedisReplyError do
        LLevantou := True;
    end;
    TAssert.AssertTrue('WRONGTYPE nao e recado', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XGroupDestroy_ZeroEhFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Inteiro(0)]);
  try
    TAssert.AssertFalse('grupo nao existia',
      LClient.Streams.XGroupDestroy('s:log', 'g1'));
    TAssert.AssertEquals(Wire(['XGROUP', 'DESTROY', 's:log', 'g1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XGroupDelConsumer_DevolveAsPendenciasPerdidas;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Inteiro(3)]);
  try
    // O retorno nao e' decorativo: sao tres entradas que saem da PEL sem
    // terem sido processadas.
    TAssert.AssertEquals(3,
      LClient.Streams.XGroupDelConsumer('s:log', 'g1', 'w1'));
    TAssert.AssertEquals(
      Wire(['XGROUP', 'DELCONSUMER', 's:log', 'g1', 'w1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XReadGroup_MontaGroupCountENoack;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LClient.Streams.XReadGroup('g1', 'w1', ['s:log'], ['>'], 10, True);
    TAssert.AssertEquals(
      Wire(['XREADGROUP', 'GROUP', 'g1', 'w1', 'COUNT', '10', 'NOACK',
        'STREAMS', 's:log', '>']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XReadGroup_ModoNovo_MandaOMaiorQue;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LDados: TRedisStreamDataArray;
begin
  LClient := NovoCliente(LFake,
    [StreamResp2('s:log', Agregado([Entrada('5-1', ['t', 'a'])]))]);
  try
    LDados := LClient.Streams.XReadGroup('g1', 'w1', ['s:log'],
      [REDIS_STREAM_NEW]);
    TAssert.AssertEquals(
      Wire(['XREADGROUP', 'GROUP', 'g1', 'w1', 'STREAMS', 's:log', '>']),
      LFake.WrittenText);
    TAssert.AssertEquals(1, Length(LDados[0].Entries));
    TAssert.AssertEquals('a', LDados[0].Entries[0].FieldValue('t'));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XReadGroupBlocking_BlockEntreCountENoack;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*-1' + CRLF]);
  try
    LClient.Streams.XReadGroupBlocking('g1', 'w1', ['s:log'], ['>'], 5000,
      1, True);
    TAssert.AssertEquals(
      Wire(['XREADGROUP', 'GROUP', 'g1', 'w1', 'COUNT', '1', 'BLOCK', '5000',
        'NOACK', 'STREAMS', 's:log', '>']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAck_ContaOsConfirmados;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Inteiro(2)]);
  try
    TAssert.AssertEquals(2,
      LClient.Streams.XAck('s:log', 'g1', ['5-1', '5-2']));
    TAssert.AssertEquals(Wire(['XACK', 's:log', 'g1', '5-1', '5-2']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAck_SemId_NaoVaiAoServidor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, []);
  try
    TAssert.AssertEquals(0, LClient.Streams.XAck('s:log', 'g1', []));
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XPendingSummary_LeTotalFaixaEConsumidores;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LResumo: TRedisPendingSummary;
begin
  LClient := NovoCliente(LFake, [Agregado([
    Inteiro(3), Bulk('5-1'), Bulk('9-2'),
    Agregado([
      '*2' + CRLF + Bulk('w1') + Bulk('2'),
      '*2' + CRLF + Bulk('w2') + Bulk('1')])])]);
  try
    LResumo := LClient.Streams.XPendingSummary('s:log', 'g1');
    TAssert.AssertEquals(Wire(['XPENDING', 's:log', 'g1']), LFake.WrittenText);
    TAssert.AssertEquals(3, LResumo.Count);
    TAssert.AssertEquals('5-1', LResumo.MinId);
    TAssert.AssertEquals('9-2', LResumo.MaxId);
    TAssert.AssertEquals(2, Length(LResumo.Consumers));
    TAssert.AssertEquals('w1', LResumo.Consumers[0].Name);
    // A contagem por consumidor vem como bulk string, e nao como inteiro.
    TAssert.AssertEquals(2, LResumo.Consumers[0].Count);
    TAssert.AssertEquals(1, LResumo.Consumers[1].Count);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XPendingSummary_SemPendencia_VemZeradoESemIds;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LResumo: TRedisPendingSummary;
begin
  // Grupo em dia: o servidor responde zero e NULO nos tres campos restantes.
  LClient := NovoCliente(LFake, [Agregado([
    Inteiro(0), '$-1' + CRLF, '$-1' + CRLF, '*-1' + CRLF])]);
  try
    LResumo := LClient.Streams.XPendingSummary('s:log', 'g1');
    TAssert.AssertEquals(0, LResumo.Count);
    TAssert.AssertEquals('', LResumo.MinId);
    TAssert.AssertEquals('', LResumo.MaxId);
    TAssert.AssertEquals(0, Length(LResumo.Consumers));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XPendingRange_MandaFaixaEContagem;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*0' + CRLF]);
  try
    LClient.Streams.XPendingRange('s:log', 'g1', '-', '+', 100);
    TAssert.AssertEquals(Wire(['XPENDING', 's:log', 'g1', '-', '+', '100']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XPendingRange_ComConsumidor_VaiNoFim;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*0' + CRLF]);
  try
    LClient.Streams.XPendingRange('s:log', 'g1', '-', '+', 10, 'w1');
    TAssert.AssertEquals(
      Wire(['XPENDING', 's:log', 'g1', '-', '+', '10', 'w1']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XPendingRange_LeOciosidadeEEntregas;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LPendencias: TRedisPendingEntryArray;
begin
  LClient := NovoCliente(LFake, [Agregado([
    '*4' + CRLF + Bulk('5-1') + Bulk('w1') + Inteiro(60000) + Inteiro(4)])]);
  try
    LPendencias := LClient.Streams.XPendingRange('s:log', 'g1', '-', '+', 10);
    TAssert.AssertEquals(1, Length(LPendencias));
    TAssert.AssertEquals('5-1', LPendencias[0].Id);
    TAssert.AssertEquals('w1', LPendencias[0].Consumer);
    TAssert.AssertEquals(60000, LPendencias[0].IdleMs);
    // Contador alto denuncia mensagem venenosa: derruba todo worker que a pega.
    TAssert.AssertEquals(4, LPendencias[0].DeliveryCount);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XPendingIdle_MandaIdleAntesDaFaixa;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, ['*0' + CRLF]);
  try
    LClient.Streams.XPendingIdle('s:log', 'g1', 60000, '-', '+', 10);
    // IDLE vem antes da faixa; depois dela e' erro de sintaxe no servidor.
    TAssert.AssertEquals(
      Wire(['XPENDING', 's:log', 'g1', 'IDLE', '60000', '-', '+', '10']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XClaim_MandaMinIdleEOsIds;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LEntradas: TRedisStreamEntryArray;
begin
  LClient := NovoCliente(LFake, [Agregado([Entrada('5-1', ['t', 'a'])])]);
  try
    LEntradas := LClient.Streams.XClaim('s:log', 'g1', 'w2', 60000,
      ['5-1', '5-2']);
    TAssert.AssertEquals(
      Wire(['XCLAIM', 's:log', 'g1', 'w2', '60000', '5-1', '5-2']),
      LFake.WrittenText);
    // Id que nao passou do minimo de ociosidade simplesmente nao volta.
    TAssert.AssertEquals(1, Length(LEntradas));
    TAssert.AssertEquals('5-1', LEntradas[0].Id);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XClaim_SemId_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, []);
  try
    try
      LClient.Streams.XClaim('s:log', 'g1', 'w2', 60000, []);
    except
      on E: ERedisException do
        LLevantou := True;
    end;
    TAssert.AssertTrue('XCLAIM sem id', LLevantou);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XClaimJustId_MandaJustIdNoFim;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LIds: TRedisStringArray;
begin
  LClient := NovoCliente(LFake, [Arr(['5-1'])]);
  try
    LIds := LClient.Streams.XClaimJustId('s:log', 'g1', 'w2', 60000, ['5-1']);
    TAssert.AssertEquals(
      Wire(['XCLAIM', 's:log', 'g1', 'w2', '60000', '5-1', 'JUSTID']),
      LFake.WrittenText);
    TAssert.AssertEquals(1, Length(LIds));
    TAssert.AssertEquals('5-1', LIds[0]);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAutoClaim_LeCursorEntradasEApagados;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LEntradas: TRedisStreamEntryArray;
  LProximo: string;
  LApagados: TRedisStringArray;
begin
  LClient := NovoCliente(LFake, [Agregado([
    Bulk('9-0'),
    Agregado([Entrada('5-1', ['t', 'a'])]),
    Arr(['4-9'])])]);
  try
    LEntradas := LClient.Streams.XAutoClaim('s:log', 'g1', 'w2', 60000, '0-0',
      10, LProximo, LApagados);
    TAssert.AssertEquals(
      Wire(['XAUTOCLAIM', 's:log', 'g1', 'w2', '60000', '0-0', 'COUNT', '10']),
      LFake.WrittenText);
    // O cursor tem a mecanica do SCAN: repetir ate' voltar '0-0'.
    TAssert.AssertEquals('9-0', LProximo);
    TAssert.AssertEquals(1, Length(LEntradas));
    TAssert.AssertEquals('5-1', LEntradas[0].Id);
    TAssert.AssertEquals(1, Length(LApagados));
    TAssert.AssertEquals('4-9', LApagados[0]);
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XAutoClaim_RespostaDeDoisItens_AindaFunciona;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LEntradas: TRedisStreamEntryArray;
  LProximo: string;
begin
  // Redis 6.2 respondia so' [cursor, entradas]: a lista de apagados chegou no
  // 7. Aceitar os dois tamanhos e' o que mantem a lib util contra 6.2 sem
  // ramificar por versao.
  LClient := NovoCliente(LFake, [Agregado([
    Bulk('0-0'),
    Agregado([Entrada('5-1', ['t', 'a'])])])]);
  try
    LEntradas := LClient.Streams.XAutoClaim('s:log', 'g1', 'w2', 60000, '0-0',
      -1, LProximo);
    // Sem COUNT o modificador nao vai ao fio.
    TAssert.AssertEquals(
      Wire(['XAUTOCLAIM', 's:log', 'g1', 'w2', '60000', '0-0']),
      LFake.WrittenText);
    TAssert.AssertEquals('0-0', LProximo);
    TAssert.AssertEquals(1, Length(LEntradas));
  finally
    LClient.Free;
  end;
end;

procedure TRedisStreamsCommandsTests.XInfoStream_MapaAchatado_AcessoPorChave;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LInfo: IRedisReply;
begin
  LClient := NovoCliente(LFake, [Agregado([
    Bulk('length'), Inteiro(7),
    Bulk('last-generated-id'), Bulk('9-0')])]);
  try
    LInfo := LClient.Streams.XInfoStream('s:log');
    TAssert.AssertEquals(Wire(['XINFO', 'STREAM', 's:log']),
      LFake.WrittenText);
    TAssert.AssertEquals(Int64(7), LInfo.ValueByKey('length').AsInteger);
    TAssert.AssertEquals('9-0',
      LInfo.ValueByKey('last-generated-id').AsString);
  finally
    LClient.Free;
  end;
end;

initialization
  RegisterTest(TRedisCommandHelpersTests);
  RegisterTest(TRedisClientTests);
  RegisterTest(TRedisKeysCommandsTests);
  RegisterTest(TRedisStringsCommandsTests);
  RegisterTest(TRedisHashesCommandsTests);
  RegisterTest(TRedisListsCommandsTests);
  RegisterTest(TRedisSetsCommandsTests);
  RegisterTest(TRedisZSetsCommandsTests);
  RegisterTest(TRedisStreamHelpersTests);
  RegisterTest(TRedisStreamsCommandsTests);

end.
