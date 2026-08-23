unit Redis.TypesTests;

{ Testes de Redis.Types (FPCUnit). Mesma cobertura do tests\Unit\Redis.TypesTests.pas
  (DUnitX/Delphi) — as duas suites sao mantidas linha a linha equivalentes, entao
  toda mudanca aqui vai para la' na mesma sessao. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Math, Redis.Types;

type
  TRedisUtf8Tests = class(TTestCase)
  published
    procedure Encode_Ascii;
    procedure Encode_Acentos_ProduzBytesUtf8;
    procedure Encode_Vazio;
    procedure Decode_Vazio;
    procedure RoundTrip_ComAcentos;
    procedure RoundTrip_TextoLongo;
    procedure Decode_PreservaBytesArbitrarios;
    procedure Decode_BytesInvalidos_NaoLevanta;
  end;

  TRedisNumberTests = class(TTestCase)
  published
    procedure FormatDouble_UsaPontoDecimal;
    procedure FormatDouble_Inteiro_SemCasas;
    procedure FormatDouble_Infinito;
    procedure FormatDouble_InfinitoNegativo;
    procedure FormatDouble_Nan;
    procedure FormatDouble_RoundTrip;
    procedure ParseDouble_Simples;
    procedure ParseDouble_Infinitos;
    procedure ParseDouble_Nan;
    procedure ParseDouble_ComEspacos;
    procedure ParseDouble_TextoInvalido_Falha;
    procedure ParseInt64_Positivo;
    procedure ParseInt64_Negativo;
    procedure ParseInt64_Limites;
    procedure ParseInt64_Vazio_Falha;
    procedure ParseInt64_SoSinal_Falha;
    procedure ParseInt64_ComEspaco_Falha;
    procedure ParseInt64_NaoNumerico_Falha;
  end;

  TRedisArgTests = class(TTestCase)
  published
    procedure Arg_String;
    procedure Arg_StringComAcentos_VaiEmUtf8;
    procedure Arg_Inteiro;
    procedure Arg_Int64Grande;
    procedure Arg_Double_UsaPontoDecimal;
    procedure Arg_Boolean_ViraUmOuZero;
    procedure Arg_Bytes_PreservaBinario;
    procedure Arg_TiposMisturados_NoMesmoArray;
  end;

  TRedisParamsTests = class(TTestCase)
  published
    procedure Default_ApontaParaLocalhost;
    procedure Default_FalaResp2SemTls;
    procedure Default_TemTimeouts;
  end;

  TRedisReplyKindNameTests = class(TTestCase)
  published
    procedure TodosOsKinds_TemNome;
  end;

implementation

{ Helpers }

function Bs(const ABytes: TBytes): string;
// Descreve bytes em hexa, para a mensagem de falha ficar legivel.
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(ABytes) do
    Result := Result + IntToHex(ABytes[I], 2);
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

{ TRedisUtf8Tests }

procedure TRedisUtf8Tests.Encode_Ascii;
begin
  TAssert.AssertEquals('50494E47', Bs(RedisUtf8Encode('PING')));
end;

procedure TRedisUtf8Tests.Encode_Acentos_ProduzBytesUtf8;
begin
  // 'a' com til = U+00E3 = C3 A3 em UTF-8. Se sair 'E3' (1 byte) a string foi
  // tratada como Latin-1 e o valor chegaria corrompido no servidor.
  TAssert.AssertEquals('C3A3', Bs(RedisUtf8Encode('ã')));
end;

procedure TRedisUtf8Tests.Encode_Vazio;
begin
  TAssert.AssertEquals(0, Length(RedisUtf8Encode('')));
end;

procedure TRedisUtf8Tests.Decode_Vazio;
begin
  TAssert.AssertEquals('', RedisUtf8Decode(nil));
end;

procedure TRedisUtf8Tests.RoundTrip_ComAcentos;
const
  TEXTO = 'ação de graças — çÇáéíóúÀÊÕ';
begin
  TAssert.AssertEquals(TEXTO, RedisUtf8Decode(RedisUtf8Encode(TEXTO)));
end;

procedure TRedisUtf8Tests.RoundTrip_TextoLongo;
var
  LTexto: string;
  I: Integer;
begin
  // Dobra em vez de concatenar o literal 2000 vezes: assim o unico ponto de
  // contato com o literal acentuado e' a atribuicao inicial, e o laco soma
  // string com string do mesmo tipo (sem passar por UnicodeString no meio).
  LTexto := 'áb';
  for I := 1 to 11 do
    LTexto := LTexto + LTexto;  // 2^11 repeticoes = 4096 caracteres
  TAssert.AssertEquals(LTexto, RedisUtf8Decode(RedisUtf8Encode(LTexto)));
end;

procedure TRedisUtf8Tests.Decode_PreservaBytesArbitrarios;
var
  LBytes: TBytes;
begin
  // Sequencia UTF-8 valida montada a mao (nao vem de literal): garante que o
  // decode nao mexe nos bytes alem de marcar o codepage.
  LBytes := MakeBytes([$C3, $A9, $20, $41]);  // 'e' agudo, espaco, 'A'
  TAssert.AssertEquals('C3A92041', Bs(RedisUtf8Encode(RedisUtf8Decode(LBytes))));
end;

procedure TRedisUtf8Tests.Decode_BytesInvalidos_NaoLevanta;
var
  LBytes: TBytes;
  LTexto: string;
  LLevantou: Boolean;
begin
  // $FF nao aparece em UTF-8 valido em lugar nenhum. Decodificar isso e' o que
  // acontece quando alguem chama AsString num valor BINARIO — coisa
  // corriqueira num Redis, que guarda bytes.
  //
  // Este teste existe por uma divergencia real entre os compiladores: o
  // TEncoding.UTF8 do Delphi nasce com MB_ERR_INVALID_CHARS e o GetString
  // LEVANTA EEncodingError quando a conversao nao produz caractere nenhum,
  // enquanto no FPC a mesma rotina so' remarca o codepage e nunca falha. Eram
  // dois comportamentos para a mesma chamada, e o do Delphi ainda vazava
  // excecao da RTL para fora da lib. Ver a nota na inicializacao de
  // Redis.Types.
  LBytes := MakeBytes([0, 13, 10, 255, 1, 65]);
  LLevantou := False;
  LTexto := 'sentinela';
  try
    LTexto := RedisUtf8Decode(LBytes);
  except
    on E: Exception do
      LLevantou := True;
  end;
  TAssert.AssertFalse('decodificar bytes binarios nao pode levantar', LLevantou);
  // O que sai do decode difere entre os compiladores de proposito (o Delphi
  // troca o byte invalido por U+FFFD; o FPC carrega os bytes). O contrato e'
  // so' este: nao levanta, e quem quer os bytes usa AsBytes.
  TAssert.AssertTrue('e devolve alguma coisa', LTexto <> 'sentinela');
end;

{ TRedisNumberTests }

procedure TRedisNumberTests.FormatDouble_UsaPontoDecimal;
begin
  // Numa maquina em pt-BR o FloatToStr solta '1,5' e o Redis recusaria o ZADD
  // com "value is not a valid float". Este teste e' o guarda desse bug.
  TAssert.AssertEquals('1.5', RedisFormatDouble(1.5));
end;

procedure TRedisNumberTests.FormatDouble_Inteiro_SemCasas;
begin
  TAssert.AssertEquals('42', RedisFormatDouble(42));
end;

procedure TRedisNumberTests.FormatDouble_Infinito;
begin
  TAssert.AssertEquals('inf', RedisFormatDouble(Infinity));
end;

procedure TRedisNumberTests.FormatDouble_InfinitoNegativo;
begin
  TAssert.AssertEquals('-inf', RedisFormatDouble(NegInfinity));
end;

procedure TRedisNumberTests.FormatDouble_Nan;
begin
  TAssert.AssertEquals('nan', RedisFormatDouble(NaN));
end;

procedure TRedisNumberTests.FormatDouble_RoundTrip;
var
  LValor: Double;
begin
  TAssert.AssertTrue('deveria reparsear',
    RedisTryParseDouble(RedisFormatDouble(3.14159265358979), LValor));
  TAssert.AssertEquals(3.14159265358979, LValor, 1E-15);
end;

procedure TRedisNumberTests.ParseDouble_Simples;
var
  LValor: Double;
begin
  TAssert.AssertTrue(RedisTryParseDouble('1.5', LValor));
  TAssert.AssertEquals(1.5, LValor, 1E-12);
end;

procedure TRedisNumberTests.ParseDouble_Infinitos;
var
  LValor: Double;
begin
  TAssert.AssertTrue(RedisTryParseDouble('inf', LValor));
  TAssert.AssertTrue('inf positivo', IsInfinite(LValor) and (LValor > 0));
  TAssert.AssertTrue(RedisTryParseDouble('-inf', LValor));
  TAssert.AssertTrue('inf negativo', IsInfinite(LValor) and (LValor < 0));
end;

procedure TRedisNumberTests.ParseDouble_Nan;
var
  LValor: Double;
begin
  TAssert.AssertTrue(RedisTryParseDouble('nan', LValor));
  TAssert.AssertTrue('deveria ser NaN', IsNan(LValor));
end;

procedure TRedisNumberTests.ParseDouble_ComEspacos;
var
  LValor: Double;
begin
  TAssert.AssertTrue(RedisTryParseDouble('  2.25  ', LValor));
  TAssert.AssertEquals(2.25, LValor, 1E-12);
end;

procedure TRedisNumberTests.ParseDouble_TextoInvalido_Falha;
var
  LValor: Double;
begin
  TAssert.AssertFalse(RedisTryParseDouble('abc', LValor));
  TAssert.AssertFalse(RedisTryParseDouble('', LValor));
end;

procedure TRedisNumberTests.ParseInt64_Positivo;
var
  LValor: Int64;
begin
  TAssert.AssertTrue(RedisTryParseInt64('1000', LValor));
  TAssert.AssertEquals(Int64(1000), LValor);
end;

procedure TRedisNumberTests.ParseInt64_Negativo;
var
  LValor: Int64;
begin
  TAssert.AssertTrue(RedisTryParseInt64('-1', LValor));
  TAssert.AssertEquals(Int64(-1), LValor);
end;

procedure TRedisNumberTests.ParseInt64_Limites;
var
  LValor: Int64;
begin
  TAssert.AssertTrue(RedisTryParseInt64('9223372036854775807', LValor));
  TAssert.AssertEquals(High(Int64), LValor);
  TAssert.AssertTrue(RedisTryParseInt64('-9223372036854775808', LValor));
  TAssert.AssertEquals(Low(Int64), LValor);
end;

procedure TRedisNumberTests.ParseInt64_Vazio_Falha;
var
  LValor: Int64;
begin
  TAssert.AssertFalse(RedisTryParseInt64('', LValor));
end;

procedure TRedisNumberTests.ParseInt64_SoSinal_Falha;
var
  LValor: Int64;
begin
  TAssert.AssertFalse(RedisTryParseInt64('-', LValor));
  TAssert.AssertFalse(RedisTryParseInt64('+', LValor));
end;

procedure TRedisNumberTests.ParseInt64_ComEspaco_Falha;
var
  LValor: Int64;
begin
  // O TryStrToInt64 sozinho aceitaria ' 12 '; num comprimento de bulk string
  // isso e' fluxo malformado, nao numero com espaco.
  TAssert.AssertFalse(RedisTryParseInt64(' 12', LValor));
  TAssert.AssertFalse(RedisTryParseInt64('12 ', LValor));
end;

procedure TRedisNumberTests.ParseInt64_NaoNumerico_Falha;
var
  LValor: Int64;
begin
  TAssert.AssertFalse(RedisTryParseInt64('12a', LValor));
  TAssert.AssertFalse(RedisTryParseInt64('1.5', LValor));
end;

{ TRedisArgTests }

procedure TRedisArgTests.Arg_String;
var
  LArg: TRedisArg;
begin
  LArg := 'SET';
  TAssert.AssertEquals('534554', Bs(LArg.Bytes));
end;

procedure TRedisArgTests.Arg_StringComAcentos_VaiEmUtf8;
var
  LArg: TRedisArg;
begin
  LArg := 'ã';
  TAssert.AssertEquals('C3A3', Bs(LArg.Bytes));
end;

procedure TRedisArgTests.Arg_Inteiro;
var
  LArg: TRedisArg;
begin
  LArg := 60;
  TAssert.AssertEquals('60', RedisUtf8Decode(LArg.Bytes));
end;

procedure TRedisArgTests.Arg_Int64Grande;
var
  LArg: TRedisArg;
begin
  LArg := High(Int64);
  TAssert.AssertEquals('9223372036854775807', RedisUtf8Decode(LArg.Bytes));
end;

procedure TRedisArgTests.Arg_Double_UsaPontoDecimal;
var
  LArg: TRedisArg;
begin
  LArg := 1.5;
  TAssert.AssertEquals('1.5', RedisUtf8Decode(LArg.Bytes));
end;

procedure TRedisArgTests.Arg_Boolean_ViraUmOuZero;
var
  LArg: TRedisArg;
begin
  LArg := True;
  TAssert.AssertEquals('1', RedisUtf8Decode(LArg.Bytes));
  LArg := False;
  TAssert.AssertEquals('0', RedisUtf8Decode(LArg.Bytes));
end;

procedure TRedisArgTests.Arg_Bytes_PreservaBinario;
var
  LArg: TRedisArg;
begin
  // Zero, CR e LF no meio: o caminho binario nao pode tocar em nada disso.
  LArg := MakeBytes([0, 13, 10, 255]);
  TAssert.AssertEquals('000D0AFF', Bs(LArg.Bytes));
end;

procedure TRedisArgTests.Arg_TiposMisturados_NoMesmoArray;
var
  LArgs: array of TRedisArg;
begin
  LArgs := nil;
  // E' o que faz Execute('SET', ['k', 'v', 'EX', 60]) compilar: os operadores
  // Implicit sao aplicados dentro do construtor de array aberto.
  SetLength(LArgs, 4);
  LArgs[0] := 'SET';
  LArgs[1] := 'chave';
  LArgs[2] := 'EX';
  LArgs[3] := 60;
  TAssert.AssertEquals('SET', RedisUtf8Decode(LArgs[0].Bytes));
  TAssert.AssertEquals('60', RedisUtf8Decode(LArgs[3].Bytes));
end;

{ TRedisParamsTests }

procedure TRedisParamsTests.Default_ApontaParaLocalhost;
var
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  TAssert.AssertEquals('localhost', LParams.Host);
  TAssert.AssertEquals(6379, Integer(LParams.Port));
  TAssert.AssertEquals(0, LParams.Database);
end;

procedure TRedisParamsTests.Default_FalaResp2SemTls;
var
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  TAssert.AssertTrue('padrao deve ser RESP2', LParams.Protocol = rpRESP2);
  TAssert.AssertFalse('TLS e opt-in', LParams.UseTls);
  TAssert.AssertTrue('verificar o peer e o padrao seguro', LParams.TlsVerifyPeer);
end;

procedure TRedisParamsTests.Default_TemTimeouts;
var
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  // Zero aqui significaria comando capaz de pendurar a thread para sempre.
  TAssert.AssertTrue('connect', LParams.ConnectTimeoutMs > 0);
  TAssert.AssertTrue('receive', LParams.ReceiveTimeoutMs > 0);
  TAssert.AssertTrue('send', LParams.SendTimeoutMs > 0);
end;

{ TRedisReplyKindNameTests }

procedure TRedisReplyKindNameTests.TodosOsKinds_TemNome;
var
  LKind: TRedisReplyKind;
begin
  for LKind := Low(TRedisReplyKind) to High(TRedisReplyKind) do
    TAssert.AssertTrue('kind sem nome: ' + IntToStr(Ord(LKind)),
      RedisReplyKindName(LKind) <> 'desconhecido');
end;

initialization
  RegisterTest(TRedisUtf8Tests);
  RegisterTest(TRedisNumberTests);
  RegisterTest(TRedisArgTests);
  RegisterTest(TRedisParamsTests);
  RegisterTest(TRedisReplyKindNameTests);

end.
