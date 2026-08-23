unit Redis.RespTests;

{ Testes do codec RESP (DUnitX). Mesma cobertura do tests\Unit\fpc\Redis.RespTests.pas
  (FPCUnit) — as duas suites sao mantidas linha a linha equivalentes.

  O que estes testes perseguem, em ordem de quanto doem em producao:

    1. Leitura parcial. Toda resposta importante e' lida de novo com a fonte
       entregando 1, 2, 3, 5, 7 e 13 bytes por vez. Numa LAN a resposta quase
       sempre chega inteira numa syscall, entao um parser que so' funciona
       assim passa em todos os testes ingenuos e quebra em producao sob carga.
    2. Binario com CRLF no meio. Uma bulk string e' delimitada por comprimento,
       nao por CRLF; parser que procura CRLF corta o valor ao meio em silencio.
    3. Nulo diferente de vazio. '$-1' e '$0' sao coisas distintas.
    4. Fluxo malformado vira excecao, nao leitura torta. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Math,
  Redis.Types,
  Redis.Resp,
  Redis.DUnitXCompat;

type
  [TestFixture]
  TRedisCommandEncodeTests = class
  public
    [Test] procedure Comando_SemArgumentos;
    [Test] procedure Comando_ComNomeEArgumentos;
    [Test] procedure Comando_FormaComNomeDestacado;
    [Test] procedure Comando_ArgumentoVazio;
    [Test] procedure Comando_ArgumentoInteiro;
    [Test] procedure Comando_ArgumentoComAcentos;
    [Test] procedure Comando_ArgumentoBinarioComCrLf;
    [Test] procedure Comando_ArgumentoGrande;
    [Test] procedure Comando_ListaVazia_Levanta;
  end;

  [TestFixture]
  TRedisReadResp2Tests = class
  public
    [Test] procedure SimpleString;
    [Test] procedure SimpleString_Vazia;
    [Test] procedure Erro_TemCodigoEMensagem;
    [Test] procedure Erro_SemEspaco_CodigoEhTudo;
    [Test] procedure Erro_RaiseIfError_Levanta;
    [Test] procedure NaoErro_RaiseIfError_NaoLevanta;
    [Test] procedure Inteiro;
    [Test] procedure Inteiro_Negativo;
    [Test] procedure Inteiro_Limite;
    [Test] procedure BulkString;
    [Test] procedure BulkString_Vazia_NaoEhNula;
    [Test] procedure BulkString_Nula;
    [Test] procedure BulkString_ComAcentos;
    [Test] procedure BulkString_BinarioComCrLf;
    [Test] procedure BulkString_Grande;
    [Test] procedure ArrayDeBulks;
    [Test] procedure ArrayVazio_NaoEhNulo;
    [Test] procedure ArrayNulo;
    [Test] procedure ArrayComNuloNoMeio;
    [Test] procedure ArrayAninhado;
    [Test] procedure ArrayAninhadoFundo;
    [Test] procedure ArrayComErroNoMeio;
  end;

  [TestFixture]
  TRedisReadResp3Tests = class
  public
    [Test] procedure Nulo;
    [Test] procedure Double_Simples;
    [Test] procedure Double_Infinito;
    [Test] procedure Double_Inteiro_TambemEhInteger;
    [Test] procedure Booleano_Verdadeiro;
    [Test] procedure Booleano_Falso;
    [Test] procedure BigNumber;
    [Test] procedure BlobError;
    [Test] procedure BlobError_ComCrLfNaMensagem;
    [Test] procedure Verbatim;
    [Test] procedure Verbatim_Markdown;
    [Test] procedure Mapa_ContaAchatado;
    [Test] procedure Mapa_ValueByKey;
    [Test] procedure Mapa_Aninhado;
    [Test] procedure Conjunto;
    [Test] procedure Push;
    [Test] procedure Atributo_NaoEhResposta;
    [Test] procedure Atributo_EmElementoDeArray;
    [Test] procedure ValueByKey_TambemEmArrayResp2;
    [Test] procedure ValueByKey_ChaveAusente_DevolveNil;
  end;

  [TestFixture]
  TRedisPartialReadTests = class
  private
    procedure ChecaEmTodosOsChunks(const AResp, ADescricao: string);
  public
    [Test] procedure SimpleString_ByteAByte;
    [Test] procedure BulkString_EmTodosOsChunks;
    [Test] procedure ArrayAninhado_EmTodosOsChunks;
    [Test] procedure RespostaResp3Completa_EmTodosOsChunks;
    [Test] procedure BulkGrande_EmChunkPequeno;
    [Test] procedure CrLfPartidoEntreLeituras;
  end;

  [TestFixture]
  TRedisStreamStateTests = class
  public
    [Test] procedure Buffered_ZeroAposRespostaCompleta;
    [Test] procedure Buffered_DetectaRespostaOrfa;
    [Test] procedure RespostasEmSequencia_MesmoLeitor;
    [Test] procedure FonteConsumidaPorInteiro;
  end;

  [TestFixture]
  TRedisProtocolErrorTests = class
  public
    [Test] procedure PrefixoDesconhecido;
    [Test] procedure LinhaVazia;
    [Test] procedure ComprimentoNaoNumerico;
    [Test] procedure ComprimentoNegativoInvalido;
    [Test] procedure ComprimentoAcimaDoTeto;
    [Test] procedure Streaming_NaoSuportado;
    [Test] procedure FaltouCrLfDepoisDaCarga;
    [Test] procedure FluxoCortadoNoMeio;
    [Test] procedure FluxoCortadoAntesDoPrimeiroByte;
    [Test] procedure BooleanoInvalido;
    [Test] procedure NuloComLixoDepois;
    [Test] procedure VerbatimSemPrefixoDeFormato;
    [Test] procedure BigNumberVazio;
    [Test] procedure AninhamentoAcimaDoTeto;
    [Test] procedure InteiroInvalido;
  end;

  [TestFixture]
  TRedisTypeAccessTests = class
  public
    [Test] procedure AsInteger_EmTextoNumerico;
    [Test] procedure AsInteger_EmTextoNaoNumerico_Levanta;
    [Test] procedure AsInteger_EmNulo_Levanta;
    [Test] procedure AsInteger_EmArray_Levanta;
    [Test] procedure AsInteger_EmDoubleFracionario_Levanta;
    [Test] procedure AsDouble_EmBulkString;
    [Test] procedure AsDouble_EmInteiro;
    [Test] procedure AsBoolean_NuloEhFalso;
    [Test] procedure AsBoolean_OkEhVerdadeiro;
    [Test] procedure AsBoolean_InteiroZeroEhFalso;
    [Test] procedure AsString_EmArray_Levanta;
    [Test] procedure AsBytes_EmNulo_EhVazio;
    [Test] procedure AsString_EmBulkBinario_NaoLevanta;
    [Test] procedure Items_EmEscalar_Levanta;
    [Test] procedure Items_ForaDosLimites_Levanta;
    [Test] procedure ValueByKey_EmEscalar_Levanta;
  end;

  [TestFixture]
  TRedisReplyRoundTripTests = class
  public
    [Test] procedure Resp3_ArvoreCompleta;
    [Test] procedure Resp2_RebaixaTiposDoResp3;
    [Test] procedure Resp2_MapaViraArrayAchatado;
    [Test] procedure BinarioSobrevive;
    [Test] procedure MapaImpar_Levanta;
  end;

implementation

{ Helpers compartilhados }

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

// Le UMA resposta a partir de um texto RESP. AMaxChunk = 0 entrega tudo de
// uma vez; qualquer outro valor limita cada leitura e simula rede lenta.
function Analisa(const AResp: string; AMaxChunk: Integer = 0): IRedisReply;
var
  LReader: TRedisReader;
begin
  LReader := TRedisReader.Create(TRedisBytesSource.Create(AResp, AMaxChunk));
  try
    Result := LReader.ReadReply;
  finally
    LReader.Free;
  end;
end;

function AnalisaBytes(const AResp: TBytes; AMaxChunk: Integer = 0): IRedisReply;
var
  LReader: TRedisReader;
begin
  LReader := TRedisReader.Create(TRedisBytesSource.Create(AResp, AMaxChunk));
  try
    Result := LReader.ReadReply;
  finally
    LReader.Free;
  end;
end;

// Levanta a excecao esperada? Devolve o nome da classe que veio (ou '' se nada
// foi levantado), para a mensagem de falha dizer o que aconteceu de fato.
//
// Nao usa AssertException do FPCUnit de proposito: ele exige a classe EXATA e
// nao aceita subclasse, e nao ha como usar Fail() dentro do try sem que o
// proprio Fail vire a excecao capturada. Ver CLAUDE.md.
function ClasseLevantadaPor(const AResp: string): string;
begin
  Result := '';
  try
    Analisa(AResp);
  except
    on E: Exception do
      Result := E.ClassName;
  end;
end;

function Texto(const ABytes: TBytes): string;
begin
  Result := RedisUtf8Decode(ABytes);
end;

{ TRedisCommandEncodeTests }

procedure TRedisCommandEncodeTests.Comando_SemArgumentos;
begin
  TAssert.AssertEquals('*1'#13#10'$4'#13#10'PING'#13#10,
    Texto(RedisEncodeCommand(['PING'])));
end;

procedure TRedisCommandEncodeTests.Comando_ComNomeEArgumentos;
begin
  TAssert.AssertEquals(
    '*3'#13#10'$3'#13#10'SET'#13#10'$5'#13#10'chave'#13#10'$5'#13#10'valor'#13#10,
    Texto(RedisEncodeCommand(['SET', 'chave', 'valor'])));
end;

procedure TRedisCommandEncodeTests.Comando_FormaComNomeDestacado;
begin
  // As duas formas tem que produzir bytes identicos.
  TAssert.AssertEquals(
    Texto(RedisEncodeCommand(['SET', 'chave', 'valor'])),
    Texto(RedisEncodeCommand('SET', ['chave', 'valor'])));
end;

procedure TRedisCommandEncodeTests.Comando_ArgumentoVazio;
begin
  // Valor vazio e' legitimo no Redis e nao pode virar nulo.
  TAssert.AssertEquals('*3'#13#10'$3'#13#10'SET'#13#10'$1'#13#10'k'#13#10'$0'#13#10#13#10,
    Texto(RedisEncodeCommand('SET', ['k', ''])));
end;

procedure TRedisCommandEncodeTests.Comando_ArgumentoInteiro;
begin
  TAssert.AssertEquals(
    '*3'#13#10'$6'#13#10'EXPIRE'#13#10'$1'#13#10'k'#13#10'$2'#13#10'60'#13#10,
    Texto(RedisEncodeCommand('EXPIRE', ['k', 60])));
end;

procedure TRedisCommandEncodeTests.Comando_ArgumentoComAcentos;
begin
  // 'ação' tem 4 caracteres mas 6 bytes em UTF-8; o comprimento no fio conta
  // BYTES. Errar isso corrompe o valor e desalinha o proximo comando.
  TAssert.AssertEquals('*3'#13#10'$3'#13#10'SET'#13#10'$1'#13#10'k'#13#10 +
    '$6'#13#10'ação'#13#10,
    Texto(RedisEncodeCommand('SET', ['k', 'ação'])));
end;

procedure TRedisCommandEncodeTests.Comando_ArgumentoBinarioComCrLf;
var
  LEncoded: TBytes;
begin
  // O valor tem 4 bytes, dois deles CR e LF. Como o comprimento manda, isso
  // atravessa o fio intacto — e' o motivo de a lib nunca emitir comando inline.
  LEncoded := RedisEncodeCommand('SET', ['k', MakeBytes([65, 13, 10, 66])]);
  TAssert.AssertEquals(
    '2A330D0A24330D0A5345540D0A24310D0A6B0D0A24340D0A410D0A420D0A',
    Hex(LEncoded));
end;

procedure TRedisCommandEncodeTests.Comando_ArgumentoGrande;
var
  LValor: TBytes;
  LEncoded: TBytes;
  I: Integer;
begin
  LValor := nil;
  SetLength(LValor, 100000);
  for I := 0 to High(LValor) do
    LValor[I] := Byte(Ord('x'));
  LEncoded := RedisEncodeCommand('SET', ['k', LValor]);
  // '*3'CRLF = 4; '$3'CRLF'SET'CRLF = 9; '$1'CRLF'k'CRLF = 7;
  // '$100000'CRLF = 9; a carga + CRLF = 100002.
  TAssert.AssertEquals(4 + 9 + 7 + 9 + 100002, Length(LEncoded));
end;

procedure TRedisCommandEncodeTests.Comando_ListaVazia_Levanta;
var
  LOk: Boolean;
  LArgs: TRedisArgs;
begin
  LOk := False;
  LArgs := nil;
  try
    RedisEncodeCommand(LArgs);
  except
    on E: ERedisException do
      LOk := True;
  end;
  TAssert.AssertTrue('comando sem argumentos deveria levantar', LOk);
end;

{ TRedisReadResp2Tests }

procedure TRedisReadResp2Tests.SimpleString;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('+OK'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkSimpleString);
  TAssert.AssertEquals('OK', LReply.AsString);
  TAssert.AssertFalse(LReply.IsNull);
  TAssert.AssertFalse(LReply.IsError);
end;

procedure TRedisReadResp2Tests.SimpleString_Vazia;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('+'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkSimpleString);
  TAssert.AssertEquals('', LReply.AsString);
end;

procedure TRedisReadResp2Tests.Erro_TemCodigoEMensagem;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('-WRONGTYPE Operation against a key'#13#10);
  TAssert.AssertTrue('IsError', LReply.IsError);
  TAssert.AssertEquals('WRONGTYPE', LReply.ErrorCode);
  TAssert.AssertEquals('WRONGTYPE Operation against a key', LReply.ErrorMessage);
end;

procedure TRedisReadResp2Tests.Erro_SemEspaco_CodigoEhTudo;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('-NOAUTH'#13#10);
  TAssert.AssertEquals('NOAUTH', LReply.ErrorCode);
end;

procedure TRedisReadResp2Tests.Erro_RaiseIfError_Levanta;
var
  LReply: IRedisReply;
  LCodigo: string;
begin
  LReply := Analisa('-ERR unknown command'#13#10);
  LCodigo := '';
  try
    LReply.RaiseIfError;
  except
    on E: ERedisReplyError do
      LCodigo := E.Code;
  end;
  TAssert.AssertEquals('ERR', LCodigo);
end;

procedure TRedisReadResp2Tests.NaoErro_RaiseIfError_NaoLevanta;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('+OK'#13#10);
  LReply.RaiseIfError;  // nao pode levantar
  TAssert.AssertEquals('OK', LReply.AsString);
end;

procedure TRedisReadResp2Tests.Inteiro;
var
  LReply: IRedisReply;
begin
  LReply := Analisa(':1000'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkInteger);
  TAssert.AssertEquals(Int64(1000), LReply.AsInteger);
  TAssert.AssertEquals('1000', LReply.AsString);
end;

procedure TRedisReadResp2Tests.Inteiro_Negativo;
begin
  TAssert.AssertEquals(Int64(-1), Analisa(':-1'#13#10).AsInteger);
end;

procedure TRedisReadResp2Tests.Inteiro_Limite;
begin
  TAssert.AssertEquals(High(Int64), Analisa(':9223372036854775807'#13#10).AsInteger);
end;

procedure TRedisReadResp2Tests.BulkString;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('$5'#13#10'hello'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkBulkString);
  TAssert.AssertEquals('hello', LReply.AsString);
  TAssert.AssertEquals(5, Length(LReply.AsBytes));
end;

procedure TRedisReadResp2Tests.BulkString_Vazia_NaoEhNula;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('$0'#13#10#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkBulkString);
  TAssert.AssertFalse('vazia nao e nula', LReply.IsNull);
  TAssert.AssertEquals('', LReply.AsString);
end;

procedure TRedisReadResp2Tests.BulkString_Nula;
var
  LReply: IRedisReply;
begin
  // A diferenca entre isto e o teste acima e' a diferenca entre "a chave nao
  // existe" e "a chave existe e vale string vazia".
  LReply := Analisa('$-1'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkNull);
  TAssert.AssertTrue('IsNull', LReply.IsNull);
  TAssert.AssertEquals(0, Length(LReply.AsBytes));
end;

procedure TRedisReadResp2Tests.BulkString_ComAcentos;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('$6'#13#10'ação'#13#10);
  TAssert.AssertEquals('ação', LReply.AsString);
  TAssert.AssertEquals(6, Length(LReply.AsBytes));
end;

procedure TRedisReadResp2Tests.BulkString_BinarioComCrLf;
var
  LResp: TBytes;
  LReply: IRedisReply;
begin
  // '$4' CRLF 'A' CR LF 'B' CRLF — o CRLF do meio e' DADO, nao delimitador.
  // Um parser que varre procurando CRLF devolveria 'A' e perderia o resto.
  LResp := MakeBytes([Ord('$'), Ord('4'), 13, 10,
                      Ord('A'), 13, 10, Ord('B'), 13, 10]);
  LReply := AnalisaBytes(LResp);
  TAssert.AssertEquals('410D0A42', Hex(LReply.AsBytes));
end;

procedure TRedisReadResp2Tests.BulkString_Grande;
var
  LResp: TBytes;
  LCabecalho: TBytes;
  LReply: IRedisReply;
  I, LTamanho: Integer;
begin
  LResp := nil;
  LTamanho := 100000;  // bem acima do buffer de leitura de 8 KB
  LCabecalho := RedisUtf8Encode('$' + IntToStr(LTamanho) + #13#10);
  SetLength(LResp, Length(LCabecalho) + LTamanho + 2);
  Move(LCabecalho[0], LResp[0], Length(LCabecalho));
  for I := 0 to LTamanho - 1 do
    LResp[Length(LCabecalho) + I] := Byte(Ord('a') + (I mod 26));
  LResp[Length(LResp) - 2] := 13;
  LResp[Length(LResp) - 1] := 10;

  LReply := AnalisaBytes(LResp);
  TAssert.AssertEquals(LTamanho, Length(LReply.AsBytes));
  TAssert.AssertEquals(Ord('a'), Integer(LReply.AsBytes[0]));
  TAssert.AssertEquals(Ord('a') + ((LTamanho - 1) mod 26),
    Integer(LReply.AsBytes[LTamanho - 1]));
end;

procedure TRedisReadResp2Tests.ArrayDeBulks;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('*2'#13#10'$3'#13#10'foo'#13#10'$3'#13#10'bar'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkArray);
  TAssert.AssertEquals(2, LReply.Count);
  TAssert.AssertEquals('foo', LReply[0].AsString);
  TAssert.AssertEquals('bar', LReply[1].AsString);
end;

procedure TRedisReadResp2Tests.ArrayVazio_NaoEhNulo;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('*0'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkArray);
  TAssert.AssertFalse('array vazio nao e nulo', LReply.IsNull);
  TAssert.AssertEquals(0, LReply.Count);
end;

procedure TRedisReadResp2Tests.ArrayNulo;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('*-1'#13#10);
  TAssert.AssertTrue('IsNull', LReply.IsNull);
end;

procedure TRedisReadResp2Tests.ArrayComNuloNoMeio;
var
  LReply: IRedisReply;
begin
  // E' exatamente a resposta de um MGET com uma chave ausente.
  LReply := Analisa('*3'#13#10'$1'#13#10'a'#13#10'$-1'#13#10'$1'#13#10'c'#13#10);
  TAssert.AssertEquals(3, LReply.Count);
  TAssert.AssertEquals('a', LReply[0].AsString);
  TAssert.AssertTrue('do meio e nulo', LReply[1].IsNull);
  TAssert.AssertEquals('c', LReply[2].AsString);
end;

procedure TRedisReadResp2Tests.ArrayAninhado;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('*2'#13#10 +
                    '*2'#13#10':1'#13#10':2'#13#10 +
                    '+ok'#13#10);
  TAssert.AssertEquals(2, LReply.Count);
  TAssert.AssertEquals(2, LReply[0].Count);
  TAssert.AssertEquals(Int64(2), LReply[0][1].AsInteger);
  TAssert.AssertEquals('ok', LReply[1].AsString);
end;

procedure TRedisReadResp2Tests.ArrayAninhadoFundo;
var
  LResp: string;
  LReply: IRedisReply;
  I, LNiveis: Integer;
begin
  // Fundo, mas abaixo do teto: tem que passar.
  LNiveis := 20;
  LResp := '';
  for I := 1 to LNiveis do
    LResp := LResp + '*1'#13#10;
  LResp := LResp + ':7'#13#10;

  LReply := Analisa(LResp);
  for I := 1 to LNiveis do
    LReply := LReply[0];
  TAssert.AssertEquals(Int64(7), LReply.AsInteger);
end;

procedure TRedisReadResp2Tests.ArrayComErroNoMeio;
var
  LReply: IRedisReply;
begin
  // E' a forma de um EXEC em que um dos comandos falhou: o erro e' um elemento
  // do array, nao o resultado inteiro.
  LReply := Analisa('*2'#13#10'+OK'#13#10'-ERR falhou'#13#10);
  TAssert.AssertFalse('o array em si nao e erro', LReply.IsError);
  TAssert.AssertTrue('o segundo elemento e', LReply[1].IsError);
  TAssert.AssertEquals('ERR', LReply[1].ErrorCode);
end;

{ TRedisReadResp3Tests }

procedure TRedisReadResp3Tests.Nulo;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('_'#13#10);
  TAssert.AssertTrue('IsNull', LReply.IsNull);
  TAssert.AssertTrue('kind', LReply.Kind = rkNull);
end;

procedure TRedisReadResp3Tests.Double_Simples;
var
  LReply: IRedisReply;
begin
  LReply := Analisa(',3.14'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkDouble);
  TAssert.AssertEquals(3.14, LReply.AsDouble, 1E-12);
end;

procedure TRedisReadResp3Tests.Double_Infinito;
var
  LReply: IRedisReply;
begin
  LReply := Analisa(',inf'#13#10);
  TAssert.AssertTrue('infinito positivo',
    IsInfinite(LReply.AsDouble) and (LReply.AsDouble > 0));
  LReply := Analisa(',-inf'#13#10);
  TAssert.AssertTrue('infinito negativo',
    IsInfinite(LReply.AsDouble) and (LReply.AsDouble < 0));
end;

procedure TRedisReadResp3Tests.Double_Inteiro_TambemEhInteger;
var
  LReply: IRedisReply;
begin
  // Um ZSCORE de 42 chega como ',42' em RESP3 e como '$2 42' em RESP2. Aceitar
  // o double exato aqui deixa o codigo da aplicacao igual nos dois protocolos.
  LReply := Analisa(',42'#13#10);
  TAssert.AssertEquals(Int64(42), LReply.AsInteger);
end;

procedure TRedisReadResp3Tests.Booleano_Verdadeiro;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('#t'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkBoolean);
  TAssert.AssertTrue('valor', LReply.AsBoolean);
  TAssert.AssertEquals(Int64(1), LReply.AsInteger);
end;

procedure TRedisReadResp3Tests.Booleano_Falso;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('#f'#13#10);
  TAssert.AssertFalse(LReply.AsBoolean);
  TAssert.AssertEquals(Int64(0), LReply.AsInteger);
end;

procedure TRedisReadResp3Tests.BigNumber;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('(3492890328409238509324850943850943825024385'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkBigNumber);
  // Nao cabe em Int64 — fica como texto, e nao ha perda silenciosa.
  TAssert.AssertEquals('3492890328409238509324850943850943825024385',
    LReply.AsString);
end;

procedure TRedisReadResp3Tests.BlobError;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('!21'#13#10'SYNTAX invalid syntax'#13#10);
  TAssert.AssertTrue('IsError', LReply.IsError);
  TAssert.AssertEquals('SYNTAX', LReply.ErrorCode);
  TAssert.AssertEquals('SYNTAX invalid syntax', LReply.ErrorMessage);
end;

procedure TRedisReadResp3Tests.BlobError_ComCrLfNaMensagem;
var
  LResp: TBytes;
  LReply: IRedisReply;
begin
  // A vantagem do blob error sobre o '-': a mensagem pode conter CRLF.
  LResp := MakeBytes([Ord('!'), Ord('7'), 13, 10,
                      Ord('E'), Ord('R'), Ord('R'), Ord(' '),
                      Ord('a'), 13, 10, 13, 10]);
  LReply := AnalisaBytes(LResp);
  TAssert.AssertTrue('IsError', LReply.IsError);
  TAssert.AssertEquals('ERR', LReply.ErrorCode);
  TAssert.AssertEquals('45525220610D0A', Hex(LReply.AsBytes));
end;

procedure TRedisReadResp3Tests.Verbatim;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('=15'#13#10'txt:Some string'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkVerbatim);
  TAssert.AssertEquals('txt', LReply.VerbatimFormat);
  // O prefixo de formato NAO faz parte do conteudo.
  TAssert.AssertEquals('Some string', LReply.AsString);
end;

procedure TRedisReadResp3Tests.Verbatim_Markdown;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('=9'#13#10'mkd:# ola'#13#10);
  TAssert.AssertEquals('mkd', LReply.VerbatimFormat);
  TAssert.AssertEquals('# ola', LReply.AsString);
end;

procedure TRedisReadResp3Tests.Mapa_ContaAchatado;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('%2'#13#10'+first'#13#10':1'#13#10'+second'#13#10':2'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkMap);
  // O cabecalho diz 2 (pares); a arvore expoe 4 (achatado), de proposito: e' a
  // mesma forma que o HGETALL tem em RESP2.
  TAssert.AssertEquals(4, LReply.Count);
  TAssert.AssertEquals('first', LReply[0].AsString);
  TAssert.AssertEquals(Int64(1), LReply[1].AsInteger);
end;

procedure TRedisReadResp3Tests.Mapa_ValueByKey;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('%2'#13#10'+first'#13#10':1'#13#10'+second'#13#10':2'#13#10);
  TAssert.AssertEquals(Int64(2), LReply.ValueByKey('second').AsInteger);
end;

procedure TRedisReadResp3Tests.Mapa_Aninhado;
var
  LReply, LInterno: IRedisReply;
begin
  // E' a forma da resposta de um HELLO 3.
  LReply := Analisa('%2'#13#10 +
                    '+server'#13#10'$5'#13#10'redis'#13#10 +
                    '+modules'#13#10'%1'#13#10'+name'#13#10'$6'#13#10'search'#13#10);
  TAssert.AssertEquals('redis', LReply.ValueByKey('server').AsString);
  LInterno := LReply.ValueByKey('modules');
  TAssert.AssertTrue('interno e mapa', LInterno.Kind = rkMap);
  TAssert.AssertEquals('search', LInterno.ValueByKey('name').AsString);
end;

procedure TRedisReadResp3Tests.Conjunto;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('~2'#13#10'+a'#13#10'+b'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkSet);
  TAssert.AssertEquals(2, LReply.Count);
  TAssert.AssertEquals('b', LReply[1].AsString);
end;

procedure TRedisReadResp3Tests.Push;
var
  LReply: IRedisReply;
begin
  // O push e' o que permite pub/sub em RESP3 sem sequestrar a conexao.
  LReply := Analisa('>3'#13#10'$7'#13#10'message'#13#10 +
                    '$3'#13#10'foo'#13#10'$3'#13#10'bar'#13#10);
  TAssert.AssertTrue('kind', LReply.Kind = rkPush);
  TAssert.AssertEquals(3, LReply.Count);
  TAssert.AssertEquals('message', LReply[0].AsString);
end;

procedure TRedisReadResp3Tests.Atributo_NaoEhResposta;
var
  LReply: IRedisReply;
begin
  // O '|' precede a resposta de verdade; quem le tem que receber o ':42' e
  // encontrar o mapa em Attributes, nunca o mapa no lugar da resposta.
  LReply := Analisa('|1'#13#10'+key-popularity'#13#10':85'#13#10':42'#13#10);
  TAssert.AssertTrue('a resposta e o inteiro', LReply.Kind = rkInteger);
  TAssert.AssertEquals(Int64(42), LReply.AsInteger);
  TAssert.AssertTrue('tem atributos', LReply.Attributes <> nil);
  TAssert.AssertEquals(Int64(85),
    LReply.Attributes.ValueByKey('key-popularity').AsInteger);
end;

procedure TRedisReadResp3Tests.Atributo_EmElementoDeArray;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('*2'#13#10 +
                    '|1'#13#10'+ttl'#13#10':10'#13#10'$1'#13#10'a'#13#10 +
                    '$1'#13#10'b'#13#10);
  TAssert.AssertEquals(2, LReply.Count);
  TAssert.AssertEquals('a', LReply[0].AsString);
  TAssert.AssertTrue('elemento 0 tem atributo', LReply[0].Attributes <> nil);
  TAssert.AssertTrue('elemento 1 nao tem', LReply[1].Attributes = nil);
end;

procedure TRedisReadResp3Tests.ValueByKey_TambemEmArrayResp2;
var
  LReply: IRedisReply;
begin
  // Mesmo dado do Mapa_ValueByKey, so' que na forma RESP2 (array achatado).
  // O codigo da aplicacao nao precisa saber qual protocolo esta em uso.
  LReply := Analisa('*4'#13#10'$5'#13#10'first'#13#10':1'#13#10 +
                    '$6'#13#10'second'#13#10':2'#13#10);
  TAssert.AssertEquals(Int64(2), LReply.ValueByKey('second').AsInteger);
end;

procedure TRedisReadResp3Tests.ValueByKey_ChaveAusente_DevolveNil;
var
  LReply: IRedisReply;
begin
  LReply := Analisa('%1'#13#10'+a'#13#10':1'#13#10);
  TAssert.AssertTrue('chave inexistente devolve nil',
    LReply.ValueByKey('nao-existe') = nil);
end;

{ TRedisPartialReadTests }

procedure TRedisPartialReadTests.ChecaEmTodosOsChunks(const AResp,
  ADescricao: string);
const
  CHUNKS: array[0..5] of Integer = (1, 2, 3, 5, 7, 13);
var
  LEsperado, LObtido: string;
  I: Integer;
begin
  // Referencia: a mesma resposta lida de uma vez, reserializada em RESP3.
  LEsperado := RedisUtf8Decode(RedisEncodeReply(Analisa(AResp, 0), rpRESP3));
  for I := Low(CHUNKS) to High(CHUNKS) do
  begin
    LObtido := RedisUtf8Decode(
      RedisEncodeReply(Analisa(AResp, CHUNKS[I]), rpRESP3));
    TAssert.AssertEquals(ADescricao + ' com chunk ' + IntToStr(CHUNKS[I]),
      LEsperado, LObtido);
  end;
end;

procedure TRedisPartialReadTests.SimpleString_ByteAByte;
begin
  TAssert.AssertEquals('OK', Analisa('+OK'#13#10, 1).AsString);
end;

procedure TRedisPartialReadTests.BulkString_EmTodosOsChunks;
begin
  ChecaEmTodosOsChunks('$11'#13#10'hello world'#13#10, 'bulk string');
end;

procedure TRedisPartialReadTests.ArrayAninhado_EmTodosOsChunks;
begin
  ChecaEmTodosOsChunks(
    '*3'#13#10 +
    '*2'#13#10'$3'#13#10'foo'#13#10'$-1'#13#10 +
    ':-12345'#13#10 +
    '*0'#13#10,
    'array aninhado');
end;

procedure TRedisPartialReadTests.RespostaResp3Completa_EmTodosOsChunks;
begin
  ChecaEmTodosOsChunks(
    '%3'#13#10 +
    '+server'#13#10'$5'#13#10'redis'#13#10 +
    '+score'#13#10',3.5'#13#10 +
    '+flags'#13#10'~2'#13#10'#t'#13#10'#f'#13#10,
    'mapa RESP3');
end;

procedure TRedisPartialReadTests.BulkGrande_EmChunkPequeno;
var
  LResp, LCabecalho: TBytes;
  LReply: IRedisReply;
  I, LTamanho: Integer;
begin
  // 20 KB entregues de 7 em 7 bytes: forca o buffer a crescer, compactar e
  // atravessar dezenas de leituras parciais numa carga so'.
  LResp := nil;
  LTamanho := 20000;
  LCabecalho := RedisUtf8Encode('$' + IntToStr(LTamanho) + #13#10);
  SetLength(LResp, Length(LCabecalho) + LTamanho + 2);
  Move(LCabecalho[0], LResp[0], Length(LCabecalho));
  for I := 0 to LTamanho - 1 do
    LResp[Length(LCabecalho) + I] := Byte(I mod 251);
  LResp[Length(LResp) - 2] := 13;
  LResp[Length(LResp) - 1] := 10;

  LReply := AnalisaBytes(LResp, 7);
  TAssert.AssertEquals(LTamanho, Length(LReply.AsBytes));
  for I := 0 to LTamanho - 1 do
    if LReply.AsBytes[I] <> Byte(I mod 251) then
      TAssert.Fail('byte ' + IntToStr(I) + ' saiu errado');
end;

procedure TRedisPartialReadTests.CrLfPartidoEntreLeituras;
var
  LReply: IRedisReply;
begin
  // Chunk 2 sobre '$1'CRLF'a'CRLF parte o CRLF final entre duas leituras: o
  // scan de linha nao pode dar o CR por conferido antes de ver o proximo byte.
  LReply := Analisa('$1'#13#10'a'#13#10, 2);
  TAssert.AssertEquals('a', LReply.AsString);
  LReply := Analisa('+PONG'#13#10, 2);
  TAssert.AssertEquals('PONG', LReply.AsString);
  LReply := Analisa('+PING'#13#10, 3);
  TAssert.AssertEquals('PING', LReply.AsString);
end;

{ TRedisStreamStateTests }

procedure TRedisStreamStateTests.Buffered_ZeroAposRespostaCompleta;
var
  LReader: TRedisReader;
begin
  LReader := TRedisReader.Create(TRedisBytesSource.Create('+OK'#13#10));
  try
    LReader.ReadReply;
    TAssert.AssertEquals(0, LReader.Buffered);
  finally
    LReader.Free;
  end;
end;

procedure TRedisStreamStateTests.Buffered_DetectaRespostaOrfa;
var
  LReader: TRedisReader;
begin
  // E' a mecanica de deteccao de conexao suja do M3: se depois de ler a
  // resposta de UM comando ainda ha bytes no buffer, sobrou resposta do
  // comando anterior e a conexao nao pode voltar para o pool.
  LReader := TRedisReader.Create(TRedisBytesSource.Create('+OK'#13#10'+PONG'#13#10));
  try
    LReader.ReadReply;
    TAssert.AssertEquals(7, LReader.Buffered);
  finally
    LReader.Free;
  end;
end;

procedure TRedisStreamStateTests.RespostasEmSequencia_MesmoLeitor;
var
  LReader: TRedisReader;
begin
  // Um pipeline: tres comandos, tres respostas, um unico leitor.
  LReader := TRedisReader.Create(TRedisBytesSource.Create(
    '+OK'#13#10':7'#13#10'$3'#13#10'abc'#13#10, 3));
  try
    TAssert.AssertEquals('OK', LReader.ReadReply.AsString);
    TAssert.AssertEquals(Int64(7), LReader.ReadReply.AsInteger);
    TAssert.AssertEquals('abc', LReader.ReadReply.AsString);
    TAssert.AssertEquals(0, LReader.Buffered);
  finally
    LReader.Free;
  end;
end;

procedure TRedisStreamStateTests.FonteConsumidaPorInteiro;
var
  LSource: TRedisBytesSource;
  LReader: TRedisReader;
begin
  LSource := TRedisBytesSource.Create('$3'#13#10'abc'#13#10, 1);
  LReader := TRedisReader.Create(LSource);
  try
    LReader.ReadReply;
    // Com chunk 1 o leitor pede byte a byte e nao pode passar do fim.
    TAssert.AssertEquals(9, LSource.Position);
  finally
    LReader.Free;
  end;
end;

{ TRedisProtocolErrorTests }

procedure TRedisProtocolErrorTests.PrefixoDesconhecido;
begin
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('@x'#13#10));
end;

procedure TRedisProtocolErrorTests.LinhaVazia;
begin
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor(#13#10));
end;

procedure TRedisProtocolErrorTests.ComprimentoNaoNumerico;
begin
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('$abc'#13#10));
end;

procedure TRedisProtocolErrorTests.ComprimentoNegativoInvalido;
begin
  // '-1' e' o nulo do RESP2 e e' valido; '-5' nao existe.
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('$-5'#13#10));
end;

procedure TRedisProtocolErrorTests.ComprimentoAcimaDoTeto;
begin
  // Acima de 512 MB so' pode ser fluxo dessincronizado; alocar antes de
  // desconfiar seria um jeito barato de derrubar o processo.
  TAssert.AssertEquals('ERedisProtocolError',
    ClasseLevantadaPor('$999999999'#13#10));
end;

procedure TRedisProtocolErrorTests.Streaming_NaoSuportado;
begin
  // O servidor Redis nao emite agregado em streaming; falhar claro e' melhor
  // do que travar esperando um terminador que nunca vem.
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('$?'#13#10));
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('*?'#13#10));
end;

procedure TRedisProtocolErrorTests.FaltouCrLfDepoisDaCarga;
begin
  TAssert.AssertEquals('ERedisProtocolError',
    ClasseLevantadaPor('$2'#13#10'abXY'));
end;

procedure TRedisProtocolErrorTests.FluxoCortadoNoMeio;
begin
  // Comprimento anunciado maior do que o que chegou: a conexao caiu no meio.
  TAssert.AssertEquals('ERedisConnectionLost',
    ClasseLevantadaPor('$10'#13#10'abc'));
end;

procedure TRedisProtocolErrorTests.FluxoCortadoAntesDoPrimeiroByte;
begin
  TAssert.AssertEquals('ERedisConnectionLost', ClasseLevantadaPor(''));
end;

procedure TRedisProtocolErrorTests.BooleanoInvalido;
begin
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('#x'#13#10));
end;

procedure TRedisProtocolErrorTests.NuloComLixoDepois;
begin
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('_x'#13#10));
end;

procedure TRedisProtocolErrorTests.VerbatimSemPrefixoDeFormato;
begin
  TAssert.AssertEquals('ERedisProtocolError',
    ClasseLevantadaPor('=3'#13#10'abc'#13#10));
end;

procedure TRedisProtocolErrorTests.BigNumberVazio;
begin
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor('('#13#10));
end;

procedure TRedisProtocolErrorTests.AninhamentoAcimaDoTeto;
var
  LResp: string;
  I: Integer;
begin
  // A leitura e' recursiva; sem teto um '*1' repetido estouraria a pilha.
  LResp := '';
  for I := 1 to REDIS_MAX_DEPTH + 5 do
    LResp := LResp + '*1'#13#10;
  LResp := LResp + ':1'#13#10;
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor(LResp));
end;

procedure TRedisProtocolErrorTests.InteiroInvalido;
begin
  TAssert.AssertEquals('ERedisProtocolError', ClasseLevantadaPor(':abc'#13#10));
end;

{ TRedisTypeAccessTests }

procedure TRedisTypeAccessTests.AsInteger_EmTextoNumerico;
begin
  // Um script Lua devolve '42' como bulk string; ler como inteiro e' comum.
  TAssert.AssertEquals(Int64(42), Analisa('$2'#13#10'42'#13#10).AsInteger);
end;

procedure TRedisTypeAccessTests.AsInteger_EmTextoNaoNumerico_Levanta;
var
  LOk: Boolean;
begin
  LOk := False;
  try
    Analisa('$3'#13#10'abc'#13#10).AsInteger;
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

procedure TRedisTypeAccessTests.AsInteger_EmNulo_Levanta;
var
  LOk: Boolean;
begin
  // Devolver 0 confundiria "chave ausente" com "chave que vale zero".
  LOk := False;
  try
    Analisa('$-1'#13#10).AsInteger;
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

procedure TRedisTypeAccessTests.AsInteger_EmArray_Levanta;
var
  LOk: Boolean;
begin
  LOk := False;
  try
    Analisa('*1'#13#10':1'#13#10).AsInteger;
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

procedure TRedisTypeAccessTests.AsInteger_EmDoubleFracionario_Levanta;
var
  LOk: Boolean;
begin
  // Truncar seria perda silenciosa de dados.
  LOk := False;
  try
    Analisa(',3.5'#13#10).AsInteger;
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

procedure TRedisTypeAccessTests.AsDouble_EmBulkString;
begin
  // E' assim que o ZSCORE responde em RESP2.
  TAssert.AssertEquals(1.5, Analisa('$3'#13#10'1.5'#13#10).AsDouble, 1E-12);
end;

procedure TRedisTypeAccessTests.AsDouble_EmInteiro;
begin
  TAssert.AssertEquals(7, Analisa(':7'#13#10).AsDouble, 1E-12);
end;

procedure TRedisTypeAccessTests.AsBoolean_NuloEhFalso;
begin
  // E' como o RESP2 diz "nao" a um SET NX que nao pegou.
  TAssert.AssertFalse(Analisa('$-1'#13#10).AsBoolean);
end;

procedure TRedisTypeAccessTests.AsBoolean_OkEhVerdadeiro;
begin
  TAssert.AssertTrue(Analisa('+OK'#13#10).AsBoolean);
end;

procedure TRedisTypeAccessTests.AsBoolean_InteiroZeroEhFalso;
begin
  TAssert.AssertFalse(Analisa(':0'#13#10).AsBoolean);
  TAssert.AssertTrue(Analisa(':1'#13#10).AsBoolean);
end;

procedure TRedisTypeAccessTests.AsString_EmArray_Levanta;
var
  LOk: Boolean;
begin
  LOk := False;
  try
    Analisa('*0'#13#10).AsString;
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

procedure TRedisTypeAccessTests.AsBytes_EmNulo_EhVazio;
begin
  TAssert.AssertEquals(0, Length(Analisa('_'#13#10).AsBytes));
end;

procedure TRedisTypeAccessTests.AsString_EmBulkBinario_NaoLevanta;
var
  LBin: TBytes;
  LReply: IRedisReply;
  LLevantou: Boolean;
begin
  // Valor binario e' o caso normal no Redis, e AsString nele e' um engano
  // brando: o certo e' AsBytes. Engano brando nao pode virar excecao da RTL —
  // e virava, so' no Delphi, porque o TEncoding.UTF8 dele recusa byte
  // invalido. Ver Redis.TypesTests.Decode_BytesInvalidos_NaoLevanta.
  LBin := MakeBytes([0, 13, 10, 255, 1, 65]);
  LReply := RedisBulk(LBin);
  LLevantou := False;
  try
    LReply.AsString;
  except
    on E: Exception do
      LLevantou := True;
  end;
  TAssert.AssertFalse('AsString em bulk binario nao pode levantar', LLevantou);
  // E o caminho binario continua exato, que e' o que de fato importa.
  TAssert.AssertEquals(6, Length(LReply.AsBytes));
  TAssert.AssertEquals(255, LReply.AsBytes[3]);
end;

procedure TRedisTypeAccessTests.Items_EmEscalar_Levanta;
var
  LOk: Boolean;
  LReply: IRedisReply;
begin
  LOk := False;
  LReply := Analisa('+OK'#13#10);
  try
    LReply[0];
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

procedure TRedisTypeAccessTests.Items_ForaDosLimites_Levanta;
var
  LOk: Boolean;
  LReply: IRedisReply;
begin
  LOk := False;
  LReply := Analisa('*1'#13#10':1'#13#10);
  try
    LReply[5];
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

procedure TRedisTypeAccessTests.ValueByKey_EmEscalar_Levanta;
var
  LOk: Boolean;
begin
  LOk := False;
  try
    Analisa('+OK'#13#10).ValueByKey('a');
  except
    on E: ERedisTypeError do
      LOk := True;
  end;
  TAssert.AssertTrue('esperava ERedisTypeError', LOk);
end;

{ TRedisReplyRoundTripTests }

function ArvoreExemplo: IRedisReply;
begin
  Result := RedisArrayOf([
    RedisSimpleString('OK'),
    RedisInteger(-42),
    RedisBulk('hello'),
    RedisNull,
    RedisMapOf([RedisBulk('chave'), RedisDouble(2.5)]),
    RedisSetOf([RedisBulk('a'), RedisBulk('b')]),
    RedisBoolean(True),
    RedisVerbatim('txt', 'linha'),
    RedisBigNumber('12345678901234567890123456789'),
    RedisArrayOf([])
  ]);
end;

procedure TRedisReplyRoundTripTests.Resp3_ArvoreCompleta;
var
  LOriginal, LLido: IRedisReply;
  LBytes: TBytes;
begin
  LOriginal := ArvoreExemplo;
  LBytes := RedisEncodeReply(LOriginal, rpRESP3);
  LLido := AnalisaBytes(LBytes);
  // Reserializar as duas e comparar cobre a arvore inteira de uma vez.
  TAssert.AssertEquals(Hex(LBytes), Hex(RedisEncodeReply(LLido, rpRESP3)));
  TAssert.AssertEquals(10, LLido.Count);
  // Valores conferidos um a um, e nao so' pela reserializacao: comparar a
  // arvore com ela mesma nao pega construtor que esquece de preencher campo.
  TAssert.AssertEquals('OK', LLido[0].AsString);
  TAssert.AssertEquals(Int64(-42), LLido[1].AsInteger);
  TAssert.AssertEquals('hello', LLido[2].AsString);
  TAssert.AssertTrue('nulo preservado', LLido[3].IsNull);
  TAssert.AssertEquals(2.5, LLido[4].ValueByKey('chave').AsDouble, 1E-12);
  TAssert.AssertEquals(2, LLido[5].Count);
  TAssert.AssertTrue('booleano', LLido[6].AsBoolean);
  TAssert.AssertEquals('txt', LLido[7].VerbatimFormat);
  TAssert.AssertEquals('12345678901234567890123456789', LLido[8].AsString);
  TAssert.AssertEquals(0, LLido[9].Count);
end;

procedure TRedisReplyRoundTripTests.Resp2_RebaixaTiposDoResp3;
var
  LLido: IRedisReply;
begin
  // Em RESP2 os tipos do RESP3 chegam rebaixados: e' o que um servidor RESP2
  // manda para o mesmo dado. O teste prova que a lib entende as duas formas.
  LLido := AnalisaBytes(RedisEncodeReply(ArvoreExemplo, rpRESP2));
  TAssert.AssertEquals(10, LLido.Count);
  TAssert.AssertTrue('nulo virou $-1 e continua nulo', LLido[3].IsNull);
  TAssert.AssertTrue('double virou bulk', LLido[4][1].Kind = rkBulkString);
  TAssert.AssertEquals(2.5, LLido[4][1].AsDouble, 1E-12);
  TAssert.AssertTrue('set virou array', LLido[5].Kind = rkArray);
  TAssert.AssertTrue('booleano virou inteiro', LLido[6].Kind = rkInteger);
  TAssert.AssertTrue('booleano continua verdadeiro', LLido[6].AsBoolean);
  TAssert.AssertEquals('linha', LLido[7].AsString);
end;

procedure TRedisReplyRoundTripTests.Resp2_MapaViraArrayAchatado;
var
  LLido: IRedisReply;
begin
  LLido := AnalisaBytes(RedisEncodeReply(
    RedisMapOf([RedisBulk('a'), RedisInteger(1),
                RedisBulk('b'), RedisInteger(2)]), rpRESP2));
  TAssert.AssertTrue('virou array', LLido.Kind = rkArray);
  TAssert.AssertEquals(4, LLido.Count);
  // E ValueByKey funciona igual nas duas formas — e' o ponto do achatamento.
  TAssert.AssertEquals(Int64(2), LLido.ValueByKey('b').AsInteger);
end;

procedure TRedisReplyRoundTripTests.BinarioSobrevive;
var
  LOriginal, LLido: IRedisReply;
begin
  LOriginal := RedisArrayOf([RedisBulk(MakeBytes([0, 13, 10, 255, 128]))]);
  LLido := AnalisaBytes(RedisEncodeReply(LOriginal, rpRESP3));
  TAssert.AssertEquals('000D0AFF80', Hex(LLido[0].AsBytes));
end;

procedure TRedisReplyRoundTripTests.MapaImpar_Levanta;
var
  LOk: Boolean;
begin
  LOk := False;
  try
    RedisMapOf([RedisBulk('a')]);
  except
    on E: ERedisProtocolError do
      LOk := True;
  end;
  TAssert.AssertTrue('mapa impar deveria levantar', LOk);
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisCommandEncodeTests);
  TDUnitX.RegisterTestFixture(TRedisReadResp2Tests);
  TDUnitX.RegisterTestFixture(TRedisReadResp3Tests);
  TDUnitX.RegisterTestFixture(TRedisPartialReadTests);
  TDUnitX.RegisterTestFixture(TRedisStreamStateTests);
  TDUnitX.RegisterTestFixture(TRedisProtocolErrorTests);
  TDUnitX.RegisterTestFixture(TRedisTypeAccessTests);
  TDUnitX.RegisterTestFixture(TRedisReplyRoundTripTests);

end.
