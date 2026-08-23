unit Redis.ConnectionTests;

{ Testes de Redis.Connection (DUnitX). Mesma cobertura do tests\Unit\fpc\Redis.ConnectionTests.pas
  (FPCUnit) — as duas suites sao mantidas linha a linha equivalentes, entao
  toda mudanca aqui vai para la' na mesma sessao.

  Nao ha' servidor Redis aqui. A conexao e' criada com CreateOnStream sobre um
  TRedisFakeServerStream, que devolve respostas roteirizadas e guarda o que o
  cliente escreveu. Com isso da' para conferir exatamente o que vai para o fio
  (handshake, unified request protocol, lote de pipeline) e, principalmente,
  exercitar os caminhos de falha que sao caros de reproduzir contra um servidor
  de verdade: fim de fluxo no meio da resposta, envio partido em pedacos e
  resposta orfa sobrando no buffer — o bug classico de cliente Redis, que o
  pool do M3 vai usar para decidir destruir a conexao em vez de devolve-la. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  Redis.Types,
  Redis.Resp,
  Redis.Connection,
  Redis.Transport,
  Redis.DUnitXCompat;

type
  { Servidor Redis de mentira: entrega respostas roteirizadas e grava o que o
    cliente mandou.

    As respostas vao numa LISTA, uma entrada por "pacote" — e uma leitura nunca
    atravessa a fronteira entre duas entradas. Isso reproduz o que um servidor
    de verdade faz: a resposta do proximo comando so' aparece no socket depois
    que o comando for enviado. Se o fake devolvesse tudo de uma vez, o leitor
    encheria o buffer com respostas futuras e a conexao se declararia suja sem
    ter culpa. Duas respostas na MESMA entrada e' justamente como se simula a
    resposta orfa que ja' estava no buffer.

    AMaxChunk limita quantos bytes cada leitura devolve (0 = tudo o que couber
    na entrada corrente) e AMaxWriteChunk faz o mesmo do lado da escrita. Os
    dois existem para reproduzir o que uma rede faz e um teste ingenuo nunca
    ve: recv parcial e send parcial. }
  TRedisFakeServerStream = class(TStream)
  private
    FSegments: array of TBytes;
    FSeg: Integer;
    FSegPos: Integer;
    FWritten: TBytes;
    FWrittenLen: Integer;
    FMaxChunk: Integer;
    FMaxWriteChunk: Integer;
  public
    constructor Create(const AResponses: array of string; AMaxChunk: Integer = 0;
      AMaxWriteChunk: Integer = 0);
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
    /// O que o cliente escreveu, como texto.
    function WrittenText: string;
    /// O que o cliente escreveu, cru.
    function WrittenBytes: TBytes;
  end;

  [TestFixture]
  TRedisPipelineTests = class
  public
    [Test] procedure Vazio_NaoTemComando;
    [Test] procedure Queue_ContaOsComandos;
    [Test] procedure Queue_CodificaUnifiedRequest;
    [Test] procedure Queue_SemArgumentos;
    [Test] procedure QueueArgs_MesmoResultado;
    [Test] procedure Clear_ZeraOLote;
    [Test] procedure LoteGrande_MantemAOrdem;
  end;

  [TestFixture]
  TRedisHandshakeTests = class
  public
    [Test] procedure Resp2_SemSenha_NaoEmiteNada;
    [Test] procedure Resp2_ComSenha_EmiteAuth;
    [Test] procedure Resp2_ComUsuario_EmiteAuthComUsuario;
    [Test] procedure ClientName_EmiteClientSetname;
    [Test] procedure Database_EmiteSelect;
    [Test] procedure DatabaseZero_NaoEmiteSelect;
    [Test] procedure Resp3_EmiteHello3;
    [Test] procedure Resp3_ComSenha_EmiteAuthComUsuarioDefault;
    [Test] procedure Resp3_LeVersaoEIdDoServidor;
    [Test] procedure Resp3_ServidorSemHello_ExplicaOMotivo;
    [Test] procedure SenhaErrada_LevantaEDeixaFechada;
    [Test] procedure OrdemDosComandos_AuthDepoisNomeDepoisSelect;
    [Test] procedure ConexaoNaoAberta_ExecuteLevanta;
  end;

  [TestFixture]
  TRedisExecuteTests = class
  public
    [Test] procedure Execute_EscreveUnifiedRequest;
    [Test] procedure Execute_DevolveARespostaLida;
    [Test] procedure Execute_ErroDoServidor_Levanta;
    [Test] procedure Execute_ErroDoServidor_NaoInvalidaAConexao;
    [Test] procedure ExecuteRaw_ErroDoServidor_NaoLevanta;
    [Test] procedure ExecuteArgs_ComandoDeDuasPalavras;
    [Test] procedure Execute_ArgumentoBinario_VaiIntacto;
    [Test] procedure Execute_RespostaBinaria_VoltaIntacta;
    [Test] procedure Execute_LeituraFatiada_MontaARespostaInteira;
    [Test] procedure Execute_EnvioFatiado_MandaOComandoInteiro;
    [Test] procedure Execute_FimDeFluxo_LevantaEInvalida;
    [Test] procedure Execute_DepoisDeInvalidada_RecusaSemIr;
    [Test] procedure Execute_RespostaSobrando_MarcaConexaoSuja;
    [Test] procedure Ping_PongDevolveTrue;
    [Test] procedure Ping_ErroDevolveFalse;
    [Test] procedure Select_AtualizaOBanco;
    [Test] procedure Select_ComErro_NaoAtualizaOBanco;
    [Test] procedure Close_FechaEPermiteDestruir;
    [Test] procedure Adotada_NaoReabreDepoisDoClose;
  end;

  [TestFixture]
  TRedisConnectionPipelineTests = class
  public
    [Test] procedure Pipeline_EnviaOLoteNumaEscritaSo;
    [Test] procedure Pipeline_UmaRespostaPorComando;
    [Test] procedure Pipeline_ErroNoMeio_NaoLevanta;
    [Test] procedure Pipeline_Vazio_NaoVaiAoServidor;
    [Test] procedure Pipeline_RespostaFaltando_LevantaEInvalida;
    [Test] procedure Pipeline_NaoDeixaResto_ConexaoContinuaLimpa;
  end;

  [TestFixture]
  TRedisTlsTests = class
  public
    [Test] procedure DefaultTlsParams_TrocaAPortaELigaOTls;
    [Test] procedure DefaultTlsParams_MantemAVerificacaoLigada;
    [Test] procedure BackendTlsDesteBuild_SeIdentifica;
    [Test] procedure ConexaoAdotada_IgnoraUseTls;
  end;

implementation

const
  CRLF = #13#10;

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

// Monta o texto RESP de um comando no unified request protocol.
//
// Escrito a mao de proposito: usar RedisEncodeCommand aqui compararia a lib
// com ela mesma, e um erro no codificador passaria batido nos dois lados.
function Wire(const AArgs: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AArgs)) + CRLF;
  for I := 0 to High(AArgs) do
    Result := Result + '$' + IntToStr(Length(AArgs[I])) + CRLF + AArgs[I] + CRLF;
end;

// Resposta tipica de um HELLO 3: mapa RESP3 de 7 pares.
function RespostaHello: string;
begin
  Result :=
    '%7' + CRLF +
    '$6' + CRLF + 'server' + CRLF + '$5' + CRLF + 'redis' + CRLF +
    '$7' + CRLF + 'version' + CRLF + '$6' + CRLF + '7.2.16' + CRLF +
    '$5' + CRLF + 'proto' + CRLF + ':3' + CRLF +
    '$2' + CRLF + 'id' + CRLF + ':42' + CRLF +
    '$4' + CRLF + 'mode' + CRLF + '$10' + CRLF + 'standalone' + CRLF +
    '$4' + CRLF + 'role' + CRLF + '$6' + CRLF + 'master' + CRLF +
    '$7' + CRLF + 'modules' + CRLF + '*0' + CRLF;
end;

{ TRedisFakeServerStream }

constructor TRedisFakeServerStream.Create(const AResponses: array of string;
  AMaxChunk: Integer = 0; AMaxWriteChunk: Integer = 0);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FSegments, Length(AResponses));
  for I := 0 to High(AResponses) do
    FSegments[I] := RedisUtf8Encode(AResponses[I]);
  FSeg := 0;
  FSegPos := 0;
  FWrittenLen := 0;
  FMaxChunk := AMaxChunk;
  FMaxWriteChunk := AMaxWriteChunk;
end;

function TRedisFakeServerStream.Read(var Buffer; Count: Longint): Longint;
var
  LAvailable: Integer;
begin
  // Pula entradas ja' consumidas (e as vazias).
  while (FSeg <= High(FSegments)) and
        (FSegPos >= Length(FSegments[FSeg])) do
  begin
    Inc(FSeg);
    FSegPos := 0;
  end;
  if FSeg > High(FSegments) then
  begin
    // Roteiro esgotado: e' o EOF que a conexao tem de tratar como queda.
    Result := 0;
    Exit;
  end;
  LAvailable := Length(FSegments[FSeg]) - FSegPos;
  Result := LAvailable;
  if Result > Count then
    Result := Count;
  if (FMaxChunk > 0) and (Result > FMaxChunk) then
    Result := FMaxChunk;
  Move(FSegments[FSeg][FSegPos], Buffer, Result);
  Inc(FSegPos, Result);
end;

function TRedisFakeServerStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result := Count;
  if (FMaxWriteChunk > 0) and (Result > FMaxWriteChunk) then
    Result := FMaxWriteChunk;
  if Result <= 0 then
  begin
    Result := 0;
    Exit;
  end;
  if FWrittenLen + Result > Length(FWritten) then
    SetLength(FWritten, (FWrittenLen + Result) * 2);
  Move(Buffer, FWritten[FWrittenLen], Result);
  Inc(FWrittenLen, Result);
end;

function TRedisFakeServerStream.Seek(const Offset: Int64;
  Origin: TSeekOrigin): Int64;
begin
  Result := 0;
  raise ERedisException.Create('o servidor falso nao suporta Seek');
end;

function TRedisFakeServerStream.WrittenBytes: TBytes;
begin
  Result := Copy(FWritten, 0, FWrittenLen);
end;

function TRedisFakeServerStream.WrittenText: string;
begin
  Result := RedisUtf8Decode(WrittenBytes);
end;

// Cria uma conexao ligada a um servidor falso.
//
// O stream fica visivel para o teste (WrittenText), mas a POSSE e' da conexao:
// liberar a conexao libera o stream, e um Open que falha — ou um erro de I/O —
// ja' o libera na hora. Por isso nenhum teste toca no fake depois de um desses.
function NovaConexao(out AFake: TRedisFakeServerStream;
  const AResponses: array of string; const AParams: TRedisParams;
  AMaxChunk: Integer = 0; AMaxWriteChunk: Integer = 0): TRedisConnection;
begin
  AFake := TRedisFakeServerStream.Create(AResponses, AMaxChunk, AMaxWriteChunk);
  Result := TRedisConnection.CreateOnStream(AFake, AParams);
end;

{ TRedisPipelineTests }

procedure TRedisPipelineTests.Vazio_NaoTemComando;
var
  LPipe: TRedisPipeline;
begin
  LPipe := TRedisPipeline.Create;
  try
    TAssert.AssertEquals(0, LPipe.Count);
    TAssert.AssertEquals(0, Length(LPipe.ToBytes));
  finally
    LPipe.Free;
  end;
end;

procedure TRedisPipelineTests.Queue_ContaOsComandos;
var
  LPipe: TRedisPipeline;
begin
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('PING');
    LPipe.Queue('SET', ['k', 'v']);
    LPipe.Queue('GET', ['k']);
    TAssert.AssertEquals(3, LPipe.Count);
  finally
    LPipe.Free;
  end;
end;

procedure TRedisPipelineTests.Queue_CodificaUnifiedRequest;
var
  LPipe: TRedisPipeline;
begin
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('SET', ['k', 'v']);
    LPipe.Queue('GET', ['k']);
    TAssert.AssertEquals(Wire(['SET', 'k', 'v']) + Wire(['GET', 'k']),
      RedisUtf8Decode(LPipe.ToBytes));
  finally
    LPipe.Free;
  end;
end;

procedure TRedisPipelineTests.Queue_SemArgumentos;
var
  LPipe: TRedisPipeline;
begin
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('PING');
    TAssert.AssertEquals(Wire(['PING']), RedisUtf8Decode(LPipe.ToBytes));
  finally
    LPipe.Free;
  end;
end;

procedure TRedisPipelineTests.QueueArgs_MesmoResultado;
var
  LComNome, LSoArray: TRedisPipeline;
begin
  LComNome := TRedisPipeline.Create;
  LSoArray := TRedisPipeline.Create;
  try
    LComNome.Queue('SET', ['k', 'v']);
    LSoArray.QueueArgs(['SET', 'k', 'v']);
    TAssert.AssertEquals(RedisUtf8Decode(LComNome.ToBytes),
      RedisUtf8Decode(LSoArray.ToBytes));
  finally
    LComNome.Free;
    LSoArray.Free;
  end;
end;

procedure TRedisPipelineTests.Clear_ZeraOLote;
var
  LPipe: TRedisPipeline;
begin
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('PING');
    LPipe.Queue('PING');
    LPipe.Clear;
    TAssert.AssertEquals(0, LPipe.Count);
    TAssert.AssertEquals(0, Length(LPipe.ToBytes));
    // Depois do Clear o buffer e' reusado: o lote novo nao pode trazer resto
    // do anterior.
    LPipe.Queue('GET', ['k']);
    TAssert.AssertEquals(Wire(['GET', 'k']), RedisUtf8Decode(LPipe.ToBytes));
  finally
    LPipe.Free;
  end;
end;

procedure TRedisPipelineTests.LoteGrande_MantemAOrdem;
var
  LPipe: TRedisPipeline;
  LEsperado: string;
  I: Integer;
begin
  // Mil comandos passam varias vezes pelo crescimento do buffer; se o Append
  // se perdesse numa realocacao, o lote sairia truncado ou fora de ordem.
  LPipe := TRedisPipeline.Create;
  try
    LEsperado := '';
    for I := 1 to 1000 do
    begin
      LPipe.Queue('SET', ['k' + IntToStr(I), IntToStr(I)]);
      LEsperado := LEsperado + Wire(['SET', 'k' + IntToStr(I), IntToStr(I)]);
    end;
    TAssert.AssertEquals(1000, LPipe.Count);
    TAssert.AssertEquals(LEsperado, RedisUtf8Decode(LPipe.ToBytes));
  finally
    LPipe.Free;
  end;
end;

{ TRedisHandshakeTests }

procedure TRedisHandshakeTests.Resp2_SemSenha_NaoEmiteNada;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  // RESP2 sem senha, sem nome e no banco 0: nada a negociar. De proposito NAO
  // se emite HELLO 2 — servidor anterior ao 6.0 nao conhece o comando, e a
  // lib nao vai exigir Redis 6 de quem nao pediu RESP3.
  LConn := NovaConexao(LFake, [], RedisDefaultParams);
  try
    LConn.Open;
    TAssert.AssertTrue('a conexao devia estar aberta', LConn.IsOpen);
    TAssert.AssertEquals('handshake mudo', '', LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.Resp2_ComSenha_EmiteAuth;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  LParams.Password := 'segredo';
  LConn := NovaConexao(LFake, ['+OK' + CRLF], LParams);
  try
    LConn.Open;
    // Sem usuario vai o AUTH de um argumento so' — a forma que o requirepass
    // classico entende, e a unica que servidor antigo aceita.
    TAssert.AssertEquals(Wire(['AUTH', 'segredo']), LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.Resp2_ComUsuario_EmiteAuthComUsuario;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  LParams.Username := 'alice';
  LParams.Password := 'segredo';
  LConn := NovaConexao(LFake, ['+OK' + CRLF], LParams);
  try
    LConn.Open;
    TAssert.AssertEquals(Wire(['AUTH', 'alice', 'segredo']), LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.ClientName_EmiteClientSetname;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  LParams.ClientName := 'minha-app';
  LConn := NovaConexao(LFake, ['+OK' + CRLF], LParams);
  try
    LConn.Open;
    TAssert.AssertEquals(Wire(['CLIENT', 'SETNAME', 'minha-app']),
      LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.Database_EmiteSelect;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  LParams.Database := 3;
  LConn := NovaConexao(LFake, ['+OK' + CRLF], LParams);
  try
    LConn.Open;
    TAssert.AssertEquals(Wire(['SELECT', '3']), LFake.WrittenText);
    TAssert.AssertEquals(3, LConn.Database);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.DatabaseZero_NaoEmiteSelect;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  // O banco 0 ja' e' o corrente: emitir SELECT 0 seria um round-trip a toa em
  // TODA conexao que o pool abrir.
  LConn := NovaConexao(LFake, [], RedisDefaultParams);
  try
    LConn.Open;
    TAssert.AssertEquals('', LFake.WrittenText);
    TAssert.AssertEquals(0, LConn.Database);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.Resp3_EmiteHello3;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  LParams.Protocol := rpRESP3;
  LConn := NovaConexao(LFake, [RespostaHello], LParams);
  try
    LConn.Open;
    TAssert.AssertEquals(Wire(['HELLO', '3']), LFake.WrittenText);
    TAssert.AssertTrue('devia ter negociado RESP3',
      LConn.NegotiatedProtocol = rpRESP3);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.Resp3_ComSenha_EmiteAuthComUsuarioDefault;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  // HELLO ... AUTH exige usuario E senha. Sem usuario configurado vai
  // 'default' — o usuario que o requirepass classico configura.
  LParams := RedisDefaultParams;
  LParams.Protocol := rpRESP3;
  LParams.Password := 'segredo';
  LConn := NovaConexao(LFake, [RespostaHello], LParams);
  try
    LConn.Open;
    TAssert.AssertEquals(Wire(['HELLO', '3', 'AUTH', 'default', 'segredo']),
      LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.Resp3_LeVersaoEIdDoServidor;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  LParams := RedisDefaultParams;
  LParams.Protocol := rpRESP3;
  LConn := NovaConexao(LFake, [RespostaHello], LParams);
  try
    LConn.Open;
    TAssert.AssertEquals('7.2.16', LConn.ServerVersion);
    TAssert.AssertEquals(Int64(42), LConn.ServerId);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.Resp3_ServidorSemHello_ExplicaOMotivo;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LMensagem: string;
begin
  // Redis anterior ao 6.0 responde "unknown command 'HELLO'". Repassar isso
  // cru mandaria o usuario cacar um comando que ELE nao escreveu; a lib
  // traduz para o que de fato aconteceu.
  LParams := RedisDefaultParams;
  LParams.Protocol := rpRESP3;
  LConn := NovaConexao(LFake, ['-ERR unknown command ''HELLO''' + CRLF],
    LParams);
  try
    LMensagem := '';
    try
      LConn.Open;
    except
      on E: Exception do
        LMensagem := E.Message;
    end;
    TAssert.AssertTrue('devia citar RESP3: ' + LMensagem,
      Pos('RESP3', LMensagem) > 0);
    TAssert.AssertFalse('nao pode ficar aberta', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.SenhaErrada_LevantaEDeixaFechada;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LClasse: string;
begin
  LParams := RedisDefaultParams;
  LParams.Password := 'errada';
  LConn := NovaConexao(LFake,
    ['-WRONGPASS invalid username-password pair' + CRLF], LParams);
  try
    LClasse := '';
    try
      LConn.Open;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisReplyError', LClasse);
    // Handshake que falha nao pode deixar socket aberto para tras.
    TAssert.AssertFalse('nao pode ficar aberta', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.OrdemDosComandos_AuthDepoisNomeDepoisSelect;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  // A ordem importa: sem AUTH antes, o servidor recusaria CLIENT SETNAME e
  // SELECT com NOAUTH.
  LParams := RedisDefaultParams;
  LParams.Password := 'segredo';
  LParams.ClientName := 'app';
  LParams.Database := 2;
  LConn := NovaConexao(LFake,
    ['+OK' + CRLF, '+OK' + CRLF, '+OK' + CRLF], LParams);
  try
    LConn.Open;
    TAssert.AssertEquals(
      Wire(['AUTH', 'segredo']) +
      Wire(['CLIENT', 'SETNAME', 'app']) +
      Wire(['SELECT', '2']),
      LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisHandshakeTests.ConexaoNaoAberta_ExecuteLevanta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LClasse: string;
begin
  LConn := NovaConexao(LFake, ['+PONG' + CRLF], RedisDefaultParams);
  try
    LClasse := '';
    try
      LConn.Execute('PING');
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisException', LClasse);
    TAssert.AssertEquals('nada podia ir ao servidor', '', LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

{ TRedisExecuteTests }

procedure TRedisExecuteTests.Execute_EscreveUnifiedRequest;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  LConn := NovaConexao(LFake, ['+OK' + CRLF], RedisDefaultParams);
  try
    LConn.Open;
    LConn.Execute('SET', ['chave', 'valor']);
    TAssert.AssertEquals(Wire(['SET', 'chave', 'valor']), LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_DevolveARespostaLida;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LReply: IRedisReply;
begin
  LConn := NovaConexao(LFake, ['$5' + CRLF + 'valor' + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    LReply := LConn.Execute('GET', ['chave']);
    TAssert.AssertTrue('devia ser bulk string', LReply.Kind = rkBulkString);
    TAssert.AssertEquals('valor', LReply.AsString);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_ErroDoServidor_Levanta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LCodigo, LClasse: string;
begin
  LConn := NovaConexao(LFake,
    ['-WRONGTYPE Operation against a key holding the wrong kind of value' + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    LClasse := '';
    LCodigo := '';
    try
      LConn.Execute('LPUSH', ['chave', 'x']);
    except
      on E: ERedisReplyError do
      begin
        LClasse := E.ClassName;
        LCodigo := E.Code;
      end;
    end;
    TAssert.AssertEquals('ERedisReplyError', LClasse);
    TAssert.AssertEquals('WRONGTYPE', LCodigo);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_ErroDoServidor_NaoInvalidaAConexao;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  // Um '-WRONGTYPE' e' resposta valida: quem errou foi o comando, nao a
  // conexao. Invalidar aqui torraria uma conexao do pool a cada erro de
  // aplicacao.
  LConn := NovaConexao(LFake, ['-ERR nao rolou' + CRLF, '+PONG' + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    try
      LConn.Execute('GET', ['chave']);
    except
      on E: ERedisReplyError do
        ;
    end;
    TAssert.AssertFalse('nao podia invalidar', LConn.IsBroken);
    TAssert.AssertTrue('devia continuar utilizavel', LConn.IsUsable);
    TAssert.AssertTrue('e devia aceitar o proximo comando', LConn.Ping);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.ExecuteRaw_ErroDoServidor_NaoLevanta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LReply: IRedisReply;
begin
  LConn := NovaConexao(LFake, ['-NOSCRIPT No matching script' + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    LReply := LConn.ExecuteRaw('EVALSHA', ['abc', 0]);
    TAssert.AssertTrue('devia vir como erro', LReply.IsError);
    TAssert.AssertEquals('NOSCRIPT', LReply.ErrorCode);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.ExecuteArgs_ComandoDeDuasPalavras;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  LConn := NovaConexao(LFake, ['+OK' + CRLF], RedisDefaultParams);
  try
    LConn.Open;
    LConn.ExecuteArgs(['CONFIG', 'SET', 'maxmemory', '0']);
    TAssert.AssertEquals(Wire(['CONFIG', 'SET', 'maxmemory', '0']),
      LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_ArgumentoBinario_VaiIntacto;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LValor: TBytes;
begin
  // CRLF e zero no meio do valor: e' o caso que quebraria um comando inline, e
  // a razao de a lib so' emitir o unified request protocol.
  LValor := MakeBytes([65, 13, 10, 66, 0]);
  LConn := NovaConexao(LFake, ['+OK' + CRLF], RedisDefaultParams);
  try
    LConn.Open;
    LConn.Execute('SET', ['k', LValor]);
    TAssert.AssertEquals(
      '*3' + CRLF + '$3' + CRLF + 'SET' + CRLF + '$1' + CRLF + 'k' + CRLF +
      '$5' + CRLF + #65#13#10#66#0 + CRLF,
      LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_RespostaBinaria_VoltaIntacta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LReply: IRedisReply;
begin
  LConn := NovaConexao(LFake, ['$5' + CRLF + #65#13#10#66#0 + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    LReply := LConn.Execute('GET', ['k']);
    TAssert.AssertEquals('410D0A4200', Hex(LReply.AsBytes));
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_LeituraFatiada_MontaARespostaInteira;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LReply: IRedisReply;
begin
  // Um byte por leitura: a resposta chega picada como numa rede lenta, e o
  // leitor tem de bloquear ate' completar em vez de devolver meia arvore.
  LConn := NovaConexao(LFake,
    ['*2' + CRLF + '$1' + CRLF + 'a' + CRLF + '$1' + CRLF + 'b' + CRLF],
    RedisDefaultParams, 1);
  try
    LConn.Open;
    LReply := LConn.Execute('LRANGE', ['k', 0, -1]);
    TAssert.AssertEquals(2, LReply.Count);
    TAssert.AssertEquals('a', LReply[0].AsString);
    TAssert.AssertEquals('b', LReply[1].AsString);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_EnvioFatiado_MandaOComandoInteiro;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  // send parcial e' normal em socket: quem escreve tem de repetir ate' acabar.
  // Sem o laco, o comando sairia truncado e o servidor ficaria esperando o
  // resto — prendendo a conexao E a thread que chamou.
  LConn := NovaConexao(LFake, ['+OK' + CRLF], RedisDefaultParams, 0, 3);
  try
    LConn.Open;
    LConn.Execute('SET', ['chave', 'valor']);
    TAssert.AssertEquals(Wire(['SET', 'chave', 'valor']), LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_FimDeFluxo_LevantaEInvalida;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LClasse: string;
begin
  // Servidor que fecha sem responder. O comando NAO e' reexecutado — INCR e
  // LPUSH nao sao idempotentes — e a conexao morre com ele.
  LConn := NovaConexao(LFake, [], RedisDefaultParams);
  try
    LConn.Open;
    LClasse := '';
    try
      LConn.Execute('INCR', ['contador']);
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisConnectionLost', LClasse);
    TAssert.AssertTrue('devia estar invalidada', LConn.IsBroken);
    TAssert.AssertFalse('e nao utilizavel', LConn.IsUsable);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Execute_DepoisDeInvalidada_RecusaSemIr;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LClasse: string;
begin
  LConn := NovaConexao(LFake, [], RedisDefaultParams);
  try
    LConn.Open;
    try
      LConn.Execute('PING');
    except
      on E: Exception do
        ;
    end;
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
  end;
end;

procedure TRedisExecuteTests.Execute_RespostaSobrando_MarcaConexaoSuja;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LReply: IRedisReply;
begin
  // Duas respostas no MESMO pacote para um comando so': a primeira e' a certa,
  // a segunda e' resto de um comando anterior (o caso do comando que sofreu
  // timeout e cuja resposta chegou depois). A conexao continua aberta e a
  // resposta entregue esta' correta, mas ela nao pode voltar ao pool — o
  // proximo comando leria a sobra achando que e' a resposta dele.
  LConn := NovaConexao(LFake, ['+PONG' + CRLF + '+PONG' + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    LReply := LConn.Execute('PING');
    TAssert.AssertEquals('PONG', LReply.AsString);
    TAssert.AssertTrue('devia estar suja', LConn.IsDirty);
    TAssert.AssertFalse('e fora de uso', LConn.IsUsable);
    TAssert.AssertFalse('mas nao invalidada por erro', LConn.IsBroken);
    TAssert.AssertTrue('e ainda aberta', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Ping_PongDevolveTrue;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  LConn := NovaConexao(LFake, ['+PONG' + CRLF], RedisDefaultParams);
  try
    LConn.Open;
    TAssert.AssertTrue(LConn.Ping);
    TAssert.AssertEquals(Wire(['PING']), LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Ping_ErroDevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  // Health check nao levanta por erro de servidor: devolve False e quem chamou
  // (o pool, no M3) decide o que fazer com a conexao.
  LConn := NovaConexao(LFake, ['-NOAUTH Authentication required' + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    TAssert.AssertFalse(LConn.Ping);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Select_AtualizaOBanco;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  LConn := NovaConexao(LFake, ['+OK' + CRLF], RedisDefaultParams);
  try
    LConn.Open;
    LConn.Select(5);
    TAssert.AssertEquals(Wire(['SELECT', '5']), LFake.WrittenText);
    TAssert.AssertEquals(5, LConn.Database);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Select_ComErro_NaoAtualizaOBanco;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  // O banco vive na conexao, e e' ele que a reconexao do M3 vai replayar:
  // memorizar um SELECT que o servidor recusou faria a conexao reconectar
  // apontando para o banco errado.
  LConn := NovaConexao(LFake, ['-ERR DB index is out of range' + CRLF],
    RedisDefaultParams);
  try
    LConn.Open;
    try
      LConn.Select(99);
    except
      on E: ERedisReplyError do
        ;
    end;
    TAssert.AssertEquals(0, LConn.Database);
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Close_FechaEPermiteDestruir;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
begin
  LConn := NovaConexao(LFake, ['+PONG' + CRLF], RedisDefaultParams);
  try
    LConn.Open;
    LConn.Ping;
    LConn.Close;
    TAssert.AssertFalse('nao pode continuar aberta', LConn.IsOpen);
    // Fechar duas vezes nao pode explodir: e' o caminho normal de teardown de
    // app (Close explicito e, logo depois, o destrutor).
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

procedure TRedisExecuteTests.Adotada_NaoReabreDepoisDoClose;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LClasse: string;
begin
  // O stream veio pronto de fora; fechado, nao ha' de onde tirar outro. Sem a
  // guarda, reabrir sairia conectando no servidor de verdade — num teste, o
  // pior tipo de surpresa.
  LConn := NovaConexao(LFake, ['+PONG' + CRLF], RedisDefaultParams);
  try
    LConn.Open;
    LConn.Close;
    LClasse := '';
    try
      LConn.Open;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisException', LClasse);
  finally
    LConn.Free;
  end;
end;

{ TRedisConnectionPipelineTests }

procedure TRedisConnectionPipelineTests.Pipeline_EnviaOLoteNumaEscritaSo;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
begin
  LConn := NovaConexao(LFake, ['+OK' + CRLF, ':1' + CRLF], RedisDefaultParams);
  LPipe := TRedisPipeline.Create;
  try
    LConn.Open;
    LPipe.Queue('SET', ['k', 'v']);
    LPipe.Queue('DEL', ['k']);
    LConn.ExecutePipeline(LPipe);
    TAssert.AssertEquals(Wire(['SET', 'k', 'v']) + Wire(['DEL', 'k']),
      LFake.WrittenText);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

procedure TRedisConnectionPipelineTests.Pipeline_UmaRespostaPorComando;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
begin
  LConn := NovaConexao(LFake,
    ['+OK' + CRLF, '$1' + CRLF + 'v' + CRLF, ':1' + CRLF], RedisDefaultParams);
  LPipe := TRedisPipeline.Create;
  try
    LConn.Open;
    LPipe.Queue('SET', ['k', 'v']);
    LPipe.Queue('GET', ['k']);
    LPipe.Queue('DEL', ['k']);
    LReplies := LConn.ExecutePipeline(LPipe);
    TAssert.AssertEquals(3, Length(LReplies));
    TAssert.AssertEquals('OK', LReplies[0].AsString);
    TAssert.AssertEquals('v', LReplies[1].AsString);
    TAssert.AssertEquals(Int64(1), LReplies[2].AsInteger);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

procedure TRedisConnectionPipelineTests.Pipeline_ErroNoMeio_NaoLevanta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
begin
  // Num lote, saber QUAL comando falhou importa mais do que abortar no
  // primeiro erro: o servidor executou todos de qualquer jeito.
  LConn := NovaConexao(LFake,
    ['+OK' + CRLF, '-WRONGTYPE nao da' + CRLF, '$1' + CRLF + 'v' + CRLF],
    RedisDefaultParams);
  LPipe := TRedisPipeline.Create;
  try
    LConn.Open;
    LPipe.Queue('SET', ['k', 'v']);
    LPipe.Queue('LPUSH', ['k', 'x']);
    LPipe.Queue('GET', ['k']);
    LReplies := LConn.ExecutePipeline(LPipe);
    TAssert.AssertEquals(3, Length(LReplies));
    TAssert.AssertFalse('o primeiro deu certo', LReplies[0].IsError);
    TAssert.AssertTrue('o segundo falhou', LReplies[1].IsError);
    TAssert.AssertEquals('WRONGTYPE', LReplies[1].ErrorCode);
    TAssert.AssertEquals('v', LReplies[2].AsString);
    TAssert.AssertTrue('a conexao continua sa', LConn.IsUsable);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

procedure TRedisConnectionPipelineTests.Pipeline_Vazio_NaoVaiAoServidor;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
begin
  LConn := NovaConexao(LFake, [], RedisDefaultParams);
  LPipe := TRedisPipeline.Create;
  try
    LConn.Open;
    TAssert.AssertEquals(0, Length(LConn.ExecutePipeline(LPipe)));
    TAssert.AssertEquals('', LFake.WrittenText);
    TAssert.AssertTrue('sem round-trip, sem estrago', LConn.IsUsable);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

procedure TRedisConnectionPipelineTests.Pipeline_RespostaFaltando_LevantaEInvalida;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LClasse: string;
begin
  LConn := NovaConexao(LFake, ['+OK' + CRLF], RedisDefaultParams);
  LPipe := TRedisPipeline.Create;
  try
    LConn.Open;
    LPipe.Queue('SET', ['k', 'v']);
    LPipe.Queue('GET', ['k']);
    LClasse := '';
    try
      LConn.ExecutePipeline(LPipe);
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisConnectionLost', LClasse);
    TAssert.AssertTrue('devia estar invalidada', LConn.IsBroken);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

procedure TRedisConnectionPipelineTests.Pipeline_NaoDeixaResto_ConexaoContinuaLimpa;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
begin
  // Lidas as N respostas do lote, o buffer tem de estar vazio: e' a mesma
  // invariante do comando isolado, e o que separa "pipeline" de "conexao
  // suja".
  LConn := NovaConexao(LFake, ['+OK' + CRLF, ':1' + CRLF, '+PONG' + CRLF],
    RedisDefaultParams);
  LPipe := TRedisPipeline.Create;
  try
    LConn.Open;
    LPipe.Queue('SET', ['k', 'v']);
    LPipe.Queue('DEL', ['k']);
    LConn.ExecutePipeline(LPipe);
    TAssert.AssertFalse('o lote nao pode sujar a conexao', LConn.IsDirty);
    TAssert.AssertTrue('e o proximo comando le a resposta dele', LConn.Ping);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

{ TRedisTlsTests }

procedure TRedisTlsTests.DefaultTlsParams_TrocaAPortaELigaOTls;
var
  LTls: TRedisParams;
begin
  LTls := RedisDefaultTlsParams;
  // A porta anda JUNTO com o TLS porque o Redis nao faz upgrade em banda: nao
  // existe STARTTLS, o listener cifrado e' outro. Ligar UseTls e esquecer a
  // porta manda um ClientHello para o listener de texto claro.
  TAssert.AssertEquals(Integer(REDIS_DEFAULT_TLS_PORT), Integer(LTls.Port));
  TAssert.AssertTrue('UseTls ligado', LTls.UseTls);
  // O resto continua sendo o default: quem chama ajusta so' o que precisa.
  TAssert.AssertEquals('localhost', LTls.Host);
  TAssert.AssertEquals(REDIS_DEFAULT_DATABASE, LTls.Database);
end;

procedure TRedisTlsTests.DefaultTlsParams_MantemAVerificacaoLigada;
begin
  // Este teste existe para travar uma decisao, nao para conferir um valor:
  // a lib NAO oferece um atalho que ja' venha com a validacao do certificado
  // desligada. Aceitar qualquer certificado anula a defesa contra
  // man-in-the-middle, entao tem de ser uma linha explicita de quem escolheu —
  // e aparecer no diff dessa pessoa.
  TAssert.AssertTrue('TlsVerifyPeer continua True',
    RedisDefaultTlsParams.TlsVerifyPeer);
end;

procedure TRedisTlsTests.BackendTlsDesteBuild_SeIdentifica;
var
  LNome: string;
begin
  LNome := RedisTlsBackendName;
  TAssert.AssertTrue('o build declara um backend', LNome <> '');
  // O Info acrescenta o detalhe de runtime quando ha' (versao e biblioteca
  // carregada, no OpenSSL); sem detalhe, repete o nome. Nos dois casos o nome
  // esta' dentro dele.
  TAssert.AssertTrue('e o Info carrega o nome dentro',
    Pos(LNome, RedisTlsBackendInfo) > 0);
end;

procedure TRedisTlsTests.ConexaoAdotada_IgnoraUseTls;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LParams: TRedisParams;
begin
  // Stream adotado JA' e' o transporte pronto — quem o injetou decidiu o que
  // ha' embaixo. Cifrar por cima seria TLS dentro de TLS, e o servidor falso
  // (que nao fala TLS) nem chegaria a responder. E' o que mantem as suites
  // unitarias rodando sem rede mesmo com UseTls nos parametros.
  LParams := RedisDefaultParams;
  LParams.UseTls := True;
  LConn := NovaConexao(LFake, ['+PONG' + CRLF], LParams);
  try
    LConn.Open;
    TAssert.AssertTrue('abriu sobre o stream adotado', LConn.IsOpen);
    TAssert.AssertTrue('e falou RESP em texto claro', LConn.Ping);
    TAssert.AssertEquals(Wire(['PING']), LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisPipelineTests);
  TDUnitX.RegisterTestFixture(TRedisHandshakeTests);
  TDUnitX.RegisterTestFixture(TRedisExecuteTests);
  TDUnitX.RegisterTestFixture(TRedisConnectionPipelineTests);
  TDUnitX.RegisterTestFixture(TRedisTlsTests);

end.
