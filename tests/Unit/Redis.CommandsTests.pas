unit Redis.CommandsTests;

{ Testes das fachadas por familia e do TRedisClient (DUnitX). Mesma cobertura
  do tests\Unit\fpc\Redis.CommandsTests.pas (FPCUnit) — as duas suites sao
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

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  Redis.Types,
  Redis.Resp,
  Redis.Connection,
  Redis.Client,
  Redis.Commands,
  Redis.Commands.Keys,
  Redis.Commands.Strings,
  Redis.Commands.Hashes,
  Redis.Commands.Lists,
  Redis.Commands.Sets,
  Redis.Commands.ZSets,
  Redis.ConnectionTests,
  Redis.DUnitXCompat;

type
  [TestFixture]
  TRedisCommandHelpersTests = class
  public
    [Test] procedure RedisArgs_ConcatenaNaOrdem;
    [Test] procedure RedisArgs_ListaVaziaDosDoisLados;
    [Test] procedure RedisAddArg_AcrescentaNoFim;
    [Test] procedure ReplyToStrings_NuloViraListaVazia;
    [Test] procedure ReplyToStrings_ItemNuloViraStringVazia;
    [Test] procedure ReplyToStrings_EscalarLevanta;
    [Test] procedure ReplyToBooleans_ZeroEUm;
    [Test] procedure ScoreBound_ExclusivoGanhaParentese;
    [Test] procedure ScoreBound_UsaPontoDecimal;
  end;

  [TestFixture]
  TRedisClientTests = class
  public
    [Test] procedure Execute_SaiPelaConexaoAmarrada;
    [Test] procedure ExecuteRaw_ErroDoServidor_NaoLevanta;
    [Test] procedure Execute_ErroDoServidor_Levanta;
    [Test] procedure Pipeline_PelaFachada;
    [Test] procedure Ping_PelaFachada;
    [Test] procedure Acquire_ConexaoUnica_DevolveSempreAMesma;
    [Test] procedure Release_ConexaoUnica_NaoDestroiAConexao;
    [Test] procedure Familias_UsamOMesmoExecutor;
  end;

  [TestFixture]
  TRedisKeysCommandsTests = class
  public
    [Test] procedure Del_UmaChave_CodificaEDevolveBooleano;
    [Test] procedure DelMany_ContaAsApagadas;
    [Test] procedure DelMany_SemChave_NaoVaiAoServidor;
    [Test] procedure Exists_ZeroEhFalse;
    [Test] procedure ExistsMany_ContaRepeticoes;
    [Test] procedure Expire_CodificaOsSegundos;
    [Test] procedure Ttl_ChaveAusente_DevolveMenosDois;
    [Test] procedure KeyType_DevolveOTipo;
    [Test] procedure Rename_NaoDevolveNada;
    [Test] procedure RenameNx_ZeroEhFalse;
    [Test] procedure CopyKey_ComReplace_MandaOModificador;
    [Test] procedure CopyKey_SemReplace_NaoMandaOModificador;
    [Test] procedure Keys_ListaAsChaves;
    [Test] procedure Scan_SemFiltro_MandaSoOCursor;
    [Test] procedure Scan_ComMatchCountETipo_NaOrdem;
    [Test] procedure Scan_AtualizaOCursorEDevolveOLote;
    [Test] procedure Scan_RespostaForaDoFormato_Levanta;
  end;

  [TestFixture]
  TRedisStringsCommandsTests = class
  public
    [Test] procedure SetValue_CodificaSetSimples;
    [Test] procedure SetWithOptions_NxGetEExpiracao_NaOrdemDaEspecificacao;
    [Test] procedure SetWithOptions_KeepTtl_NaoMandaNumero;
    [Test] procedure SetWithOptions_NxBarrado_DevolveNulo;
    [Test] procedure SetWithOptions_XxEExAt;
    [Test] procedure Get_ChaveAusente_EhNuloENaoVazio;
    [Test] procedure TryGet_ChaveAusente_DevolveFalse;
    [Test] procedure TryGet_ValorVazio_DevolveTrue;
    [Test] procedure GetString_ChaveAusente_DevolveVazio;
    [Test] procedure SetNx_ZeroEhFalse;
    [Test] procedure SetEx_CodificaNaOrdemChavePrazoValor;
    [Test] procedure Incr_DevolveOInteiro;
    [Test] procedure IncrByFloat_MandaPontoDecimal;
    [Test] procedure IncrByFloat_LeODoubleDaResposta;
    [Test] procedure MSet_QuantidadeImpar_Levanta;
    [Test] procedure MSet_CodificaOsPares;
    [Test] procedure MGet_MantemONuloNoMeio;
    [Test] procedure MGet_SemChave_Levanta;
    [Test] procedure GetRange_CodificaOsIndices;
    [Test] procedure StrLen_DevolveOTamanho;
  end;

  [TestFixture]
  TRedisHashesCommandsTests = class
  public
    [Test] procedure HSet_DevolveQuantosCamposNasceram;
    [Test] procedure HSetMany_CodificaOsPares;
    [Test] procedure HSetMany_QuantidadeImpar_Levanta;
    [Test] procedure HGetAll_Resp2Array_AcessoPorCampo;
    [Test] procedure HGetAll_Resp3Mapa_MesmaFormaQueOResp2;
    [Test] procedure HTryGet_CampoAusente_DevolveFalse;
    [Test] procedure HMGet_MantemONuloNoMeio;
    [Test] procedure HDel_ZeroEhFalse;
    [Test] procedure HIncrByFloat_MandaPontoDecimal;
    [Test] procedure HKeys_ListaOsCampos;
    [Test] procedure HScan_MontaMatchECount;
  end;

  [TestFixture]
  TRedisListsCommandsTests = class
  public
    [Test] procedure LPushMany_MandaTudoNumComandoSo;
    [Test] procedure LPushMany_SemValor_Levanta;
    [Test] procedure LRange_CodificaEDevolveALista;
    [Test] procedure LPop_ListaVazia_EhNulo;
    [Test] procedure LInsert_Before_MandaAPalavraCerta;
    [Test] procedure LInsert_After_MandaAPalavraCerta;
    [Test] procedure LMove_MandaAsDuasPontas;
    [Test] procedure BLPop_PrazoVencido_DevolveFalse;
    [Test] procedure BLPop_DevolveChaveEValor;
    [Test] procedure BLPop_CodificaOTimeoutNoFim;
    [Test] procedure BLPop_SemChave_Levanta;
    [Test] procedure BLPop_RestauraOTimeoutDaConexao;
    [Test] procedure BLMove_CodificaPontasETimeout;
  end;

  [TestFixture]
  TRedisSetsCommandsTests = class
  public
    [Test] procedure SAdd_MembroNovo_DevolveTrue;
    [Test] procedure SAdd_MembroRepetido_DevolveFalse;
    [Test] procedure SAddMany_CodificaTodos;
    [Test] procedure SMembers_ListaOsMembros;
    [Test] procedure SMIsMember_MapeiaZeroEUm;
    [Test] procedure SInterStore_DestinoAntesDasChaves;
    [Test] procedure SInterCard_MandaAQuantidadeDeChaves;
    [Test] procedure SInterCard_ComLimite;
    [Test] procedure SScan_MontaMatchECount;
    [Test] procedure SInter_SemChave_Levanta;
  end;

  [TestFixture]
  TRedisZSetsCommandsTests = class
  public
    [Test] procedure ZAdd_FormataOScoreComPonto;
    [Test] procedure ZAddOpt_NxECh_NaOrdemDaEspecificacao;
    [Test] procedure ZAddOpt_SemCondicao_NaoMandaModificador;
    [Test] procedure ZAddMany_QuantidadeImpar_Levanta;
    [Test] procedure ZRangeWithScores_Resp2Achatado;
    [Test] procedure ZRangeWithScores_Resp3Pares_MesmoResultado;
    [Test] procedure ZRangeWithScores_TamanhoImpar_Levanta;
    [Test] procedure ZRangeByScore_SemLimite_NaoMandaLimit;
    [Test] procedure ZRangeByScore_ComLimite_MandaOffsetECount;
    [Test] procedure ZRevRangeByScore_InverteMaxEMin;
    [Test] procedure ZTryScore_MembroAusente_DevolveFalse;
    [Test] procedure ZTryScore_LeODoubleDoResp3;
    [Test] procedure ZTryRank_MembroAusente_DevolveFalse;
    [Test] procedure ZPopMin_ConjuntoVazio_DevolveFalse;
    [Test] procedure ZPopMin_DevolveMembroEScore;
    [Test] procedure ZIncrBy_MandaDeltaComPonto;
    [Test] procedure ZScan_MontaMatchECount;
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

initialization
  TDUnitX.RegisterTestFixture(TRedisCommandHelpersTests);
  TDUnitX.RegisterTestFixture(TRedisClientTests);
  TDUnitX.RegisterTestFixture(TRedisKeysCommandsTests);
  TDUnitX.RegisterTestFixture(TRedisStringsCommandsTests);
  TDUnitX.RegisterTestFixture(TRedisHashesCommandsTests);
  TDUnitX.RegisterTestFixture(TRedisListsCommandsTests);
  TDUnitX.RegisterTestFixture(TRedisSetsCommandsTests);
  TDUnitX.RegisterTestFixture(TRedisZSetsCommandsTests);

end.
