unit Redis.TransactionTests;

{ Testes de Redis.Transaction e Redis.Commands.Scripting (DUnitX). Mesma
  cobertura do tests\Unit\fpc\Redis.TransactionTests.pas (FPCUnit) — as duas
  suites sao mantidas linha a linha equivalentes, entao toda mudanca aqui vai
  para la' na mesma sessao.

  Sem servidor: tudo roda sobre o TRedisFakeServerStream da
  Redis.ConnectionTests. E' o que permite verificar duas coisas que um teste de
  integracao nao distingue:

  1. **A forma exata do lote.** Um MULTI/EXEC correto e um MULTI/EXEC que
     esqueceu o EXEC produzem resultados diferentes no servidor, mas so' os
     bytes contam a historia — e o servidor falso deixa ler o que saiu.
  2. **Os caminhos que o servidor real quase nunca produz sob demanda:** EXEC
     nulo (chave vigiada mudou), comando recusado na fila, e o NOSCRIPT que so'
     aparece quando alguem esvazia o cache de scripts entre uma chamada e
     outra.

  Os SHA-1 de referencia foram calculados por fora (hashlib do Python), nao por
  esta lib: um teste que confere o SHA contra o proprio codigo que o produz nao
  testa nada. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  Redis.Types,
  Redis.Resp,
  Redis.Connection,
  Redis.Commands,
  Redis.Commands.Scripting,
  Redis.Transaction,
  Redis.Client,
  Redis.ConnectionTests,
  Redis.DUnitXCompat;

type
  [TestFixture]
  TRedisScriptShaTests = class
  public
    [Test] procedure Sha_TextoVazio;
    [Test] procedure Sha_VetorDeReferencia;
    [Test] procedure Sha_ScriptLua;
    [Test] procedure Sha_SaiEmMinusculas;
    [Test] procedure Sha_MedeOsBytesUtf8_NaoAString;
  end;

  [TestFixture]
  TRedisTransactionTests = class
  public
    [Test] procedure Commit_EnviaMultiComandosEExec;
    [Test] procedure Commit_DevolveUmaRespostaPorComando;
    [Test] procedure Commit_BlocoVazio_AindaVaiAoServidor;
    [Test] procedure Commit_ErroDeExecucao_VemComoItemESemRollback;
    [Test] procedure TryCommit_ExecNulo_DevolveFalse;
    [Test] procedure Commit_ExecNulo_LevantaAborted;
    [Test] procedure Commit_ComandoRecusadoNaFila_LevantaCitandoAPosicao;
    [Test] procedure Commit_MultiRecusado_Levanta;
    [Test] procedure Queue_DepoisDoCommit_Levanta;
    [Test] procedure Count_NaoConta0Multi;
    [Test] procedure Watch_VaiAoServidorNaHora;
    [Test] procedure Watch_SemChave_Levanta;
    [Test] procedure Commit_LimpaOEstadoDeWatch;
    [Test] procedure Discard_MandaUnwatchENaoMandaDiscard;
    [Test] procedure Discard_SemWatch_NaoVaiAoServidor;
    [Test] procedure Destrutor_ComWatchPendente_MandaUnwatch;
    [Test] procedure Destrutor_SemWatch_NaoMandaNada;
    [Test] procedure Destrutor_DevolveAConexaoAQuemEmprestou;
  end;

  [TestFixture]
  TRedisScriptingTests = class
  public
    [Test] procedure Eval_MontaNumkeysChavesEArgumentos;
    [Test] procedure Eval_SemChave_MandaZero;
    [Test] procedure Run_PrimeiraVez_MandaEval;
    [Test] procedure Run_SegundaVez_MandaEvalSha;
    [Test] procedure Run_Noscript_ReenviaOEval;
    [Test] procedure Run_ErroDeLua_LevantaEmVezDeReenviar;
    [Test] procedure Run_ScriptComErro_NaoEntraNoCache;
    [Test] procedure ScriptLoad_DevolveEMemorizaOSha;
    [Test] procedure ScriptFlush_LimpaOCacheLocal;
    [Test] procedure ScriptExists_LeOArrayDeUmElemento;
    [Test] procedure ClearCache_EsqueceSemFalarComOServidor;
  end;

implementation

const
  CRLF = #13#10;

  { Vetores de referencia, calculados por fora desta lib. }
  SHA_VAZIO = 'da39a3ee5e6b4b0d3255bfef95601890afd80709';
  SHA_ABC   = 'a9993e364706816aba3e25717850c26c9cd0d89d';

  SCRIPT_GET = 'return redis.call("GET", KEYS[1])';
  SHA_GET    = 'd1ad8397c172dc0a63e271f0c4c4250ca8d5d1fb';

  { O mesmo script com e sem acento. O acentuado ocupa 18 bytes em UTF-8 para
    16 caracteres, e o SHA de referencia foi calculado sobre esses 18 bytes —
    que e' exatamente o que o servidor mediria. }
  SCRIPT_SEM_ACENTO = 'return "coracao"';
  SHA_SEM_ACENTO    = '1cfa8f87af421a3e95bf599b64071b4a842a7efe';
  SCRIPT_COM_ACENTO = 'return "coração"';
  SHA_COM_ACENTO    = 'acd9b4bafcc82e38d2f3727c5f494853dbf0de3e';

{ Helpers compartilhados }

// Monta o unified request protocol esperado no fio. So' ASCII.
function Wire(const AArgs: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AArgs)) + CRLF;
  for I := 0 to High(AArgs) do
    Result := Result + '$' + IntToStr(Length(AArgs[I])) + CRLF + AArgs[I] + CRLF;
end;

function Bulk(const AValue: string): string;
begin
  Result := '$' + IntToStr(Length(AValue)) + CRLF + AValue + CRLF;
end;

function Arr(const AItems: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AItems)) + CRLF;
  for I := 0 to High(AItems) do
    Result := Result + Bulk(AItems[I]);
end;

// Respostas de um commit de N comandos: +OK do MULTI, N x +QUEUED e o EXEC.
// Tudo numa entrada so', porque o lote sai numa escrita so'.
function RespostaCommit(ACount: Integer; const AExec: string): string;
var
  I: Integer;
begin
  Result := '+OK' + CRLF;
  for I := 1 to ACount do
    Result := Result + '+QUEUED' + CRLF;
  Result := Result + AExec;
end;

function NovaConexaoFake(out AFake: TRedisFakeServerStream;
  const AResponses: array of string): TRedisConnection;
begin
  AFake := TRedisFakeServerStream.Create(AResponses);
  Result := TRedisConnection.CreateOnStream(AFake, RedisDefaultParams);
  try
    Result.Open;
  except
    Result.Free;
    raise;
  end;
end;

// Cliente amarrado a uma conexao sobre o servidor falso.
function NovoCliente(out AFake: TRedisFakeServerStream;
  const AResponses: array of string): TRedisClient;
var
  LConn: TRedisConnection;
begin
  LConn := NovaConexaoFake(AFake, AResponses);
  try
    Result := TRedisClient.CreateOnConnection(LConn, True);
  except
    LConn.Free;
    raise;
  end;
end;

{ Registra a devolucao da conexao, para provar que o destrutor da transacao
  chama de volta quem a emprestou. }
type
  TDevolucaoEspia = class
  private
    FDevolvidas: Integer;
    FUltima: TRedisConnection;
  public
    procedure Devolve(AConnection: TRedisConnection);
    property Devolvidas: Integer read FDevolvidas;
    property Ultima: TRedisConnection read FUltima;
  end;

procedure TDevolucaoEspia.Devolve(AConnection: TRedisConnection);
begin
  Inc(FDevolvidas);
  FUltima := AConnection;
end;

{ TRedisScriptShaTests }

procedure TRedisScriptShaTests.Sha_TextoVazio;
begin
  TAssert.AssertEquals(SHA_VAZIO, RedisScriptSha(''));
end;

procedure TRedisScriptShaTests.Sha_VetorDeReferencia;
begin
  TAssert.AssertEquals(SHA_ABC, RedisScriptSha('abc'));
end;

procedure TRedisScriptShaTests.Sha_ScriptLua;
begin
  // E' o SHA que o SCRIPT LOAD devolveria para este script — o que torna o
  // EVALSHA possivel sem perguntar ao servidor.
  TAssert.AssertEquals(SHA_GET, RedisScriptSha(SCRIPT_GET));
end;

procedure TRedisScriptShaTests.Sha_SaiEmMinusculas;
var
  LSha: string;
begin
  LSha := RedisScriptSha(SCRIPT_GET);
  TAssert.AssertEquals(40, Length(LSha));
  TAssert.AssertEquals(LowerCase(LSha), LSha);
end;

procedure TRedisScriptShaTests.Sha_MedeOsBytesUtf8_NaoAString;
begin
  // Script so' com ASCII: bytes e caracteres coincidem, e qualquer digestor
  // acerta.
  TAssert.AssertEquals(SHA_SEM_ACENTO, RedisScriptSha(SCRIPT_SEM_ACENTO));

  // Com acento os dois deixam de coincidir — 16 caracteres, 18 bytes — e o SHA
  // de referencia e' o dos BYTES. Este e' o teste que trava a decisao: hashear
  // a representacao local da string daria outro digest no FPC, o EVALSHA
  // responderia NOSCRIPT para sempre, e o cache pareceria funcionar enquanto
  // nunca acertava um so' script.
  TAssert.AssertEquals(SHA_COM_ACENTO, RedisScriptSha(SCRIPT_COM_ACENTO));
  TAssert.AssertEquals(18, Length(RedisUtf8Encode(SCRIPT_COM_ACENTO)));
end;

{ TRedisTransactionTests }

procedure TRedisTransactionTests.Commit_EnviaMultiComandosEExec;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake, [RespostaCommit(2, Arr(['OK', 'OK']))]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('SET', ['a', '1']);
      LTx.Queue('SET', ['b', '2']);
      LTx.Commit;
      // O bloco inteiro sai numa escrita so': MULTI na frente, EXEC no fim.
      TAssert.AssertEquals(
        Wire(['MULTI']) + Wire(['SET', 'a', '1']) + Wire(['SET', 'b', '2']) +
        Wire(['EXEC']),
        LFake.WrittenText);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Commit_DevolveUmaRespostaPorComando;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LRespostas: TRedisReplyArray;
begin
  LConn := NovaConexaoFake(LFake,
    [RespostaCommit(2, '*2' + CRLF + ':1' + CRLF + ':11' + CRLF)]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('INCR', ['n']);
      LTx.Queue('INCRBY', ['n', '10']);
      LRespostas := LTx.Commit;
      // As respostas do EXEC, e nao os QUEUED: quem chamou nunca ve o
      // protocolo de enfileiramento.
      TAssert.AssertEquals(2, Length(LRespostas));
      TAssert.AssertEquals(Int64(1), LRespostas[0].AsInteger);
      TAssert.AssertEquals(Int64(11), LRespostas[1].AsInteger);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Commit_BlocoVazio_AindaVaiAoServidor;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LRespostas: TRedisReplyArray;
begin
  LConn := NovaConexaoFake(LFake, [RespostaCommit(0, '*0' + CRLF)]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LRespostas := LTx.Commit;
      TAssert.AssertEquals(0, Length(LRespostas));
      // AO CONTRARIO do pipeline vazio, que nao sai de casa: sob WATCH, um
      // MULTI/EXEC sem comando nenhum e' a pergunta "alguem mexeu no que eu
      // vigiava?", e a resposta so' existe no servidor.
      TAssert.AssertEquals(Wire(['MULTI']) + Wire(['EXEC']), LFake.WrittenText);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Commit_ErroDeExecucao_VemComoItemESemRollback;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LRespostas: TRedisReplyArray;
begin
  LConn := NovaConexaoFake(LFake,
    [RespostaCommit(3, '*3' + CRLF + '+OK' + CRLF +
      '-WRONGTYPE tipo errado' + CRLF + '+OK' + CRLF)]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('SET', ['a', '1']);
      LTx.Queue('LPUSH', ['a', 'x']);
      LTx.Queue('SET', ['b', '2']);
      // NAO levanta: o erro e' de UM comando, e os outros dois rodaram e
      // ficaram gravados. Nao existe rollback no Redis, e esconder isso atras
      // de uma excecao faria o chamador acreditar que nada foi gravado.
      LRespostas := LTx.Commit;
      TAssert.AssertEquals(3, Length(LRespostas));
      TAssert.AssertEquals('OK', LRespostas[0].AsString);
      TAssert.AssertTrue('o do meio falhou', LRespostas[1].IsError);
      TAssert.AssertEquals('WRONGTYPE', LRespostas[1].ErrorCode);
      TAssert.AssertEquals('OK', LRespostas[2].AsString);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.TryCommit_ExecNulo_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LRespostas: TRedisReplyArray;
begin
  LConn := NovaConexaoFake(LFake, [RespostaCommit(1, '*-1' + CRLF)]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('SET', ['a', '1']);
      // Chave vigiada mudou. E' o caso NORMAL do check-and-set, nao um erro:
      // por isso a forma primaria da API devolve Boolean.
      TAssert.AssertFalse('abortou', LTx.TryCommit(LRespostas));
      TAssert.AssertEquals(0, Length(LRespostas));
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Commit_ExecNulo_LevantaAborted;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LConn := NovaConexaoFake(LFake, [RespostaCommit(1, '*-1' + CRLF)]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('SET', ['a', '1']);
      try
        LTx.Commit;
      except
        on E: ERedisTransactionAborted do
          LLevantou := True;
      end;
      TAssert.AssertTrue('Commit levanta onde TryCommit devolve False',
        LLevantou);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Commit_ComandoRecusadoNaFila_LevantaCitandoAPosicao;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LMensagem: string;
begin
  LMensagem := '';
  // O servidor aceita o primeiro, recusa o segundo (comando inexistente) e o
  // EXEC vem com EXECABORT. O erro util e' o do comando, nao o do EXEC.
  LConn := NovaConexaoFake(LFake,
    ['+OK' + CRLF + '+QUEUED' + CRLF + '-ERR unknown command' + CRLF +
     '-EXECABORT Transaction discarded' + CRLF]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('SET', ['a', '1']);
      LTx.Queue('NAOEXISTE', ['x']);
      try
        LTx.Commit;
      except
        on E: ERedisTransactionError do
          LMensagem := E.Message;
      end;
      TAssert.AssertTrue('cita a posicao do comando torto',
        Pos('comando 1', LMensagem) > 0);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Commit_MultiRecusado_Levanta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LConn := NovaConexaoFake(LFake,
    ['-ERR MULTI calls can not be nested' + CRLF + '+QUEUED' + CRLF +
     '-ERR EXEC without MULTI' + CRLF]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('SET', ['a', '1']);
      try
        LTx.Commit;
      except
        on E: ERedisTransactionError do
          LLevantou := True;
      end;
      TAssert.AssertTrue('MULTI recusado levanta', LLevantou);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Queue_DepoisDoCommit_Levanta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LConn := NovaConexaoFake(LFake, [RespostaCommit(0, '*0' + CRLF)]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Commit;
      try
        LTx.Queue('SET', ['a', '1']);
      except
        on E: ERedisTransactionError do
          LLevantou := True;
      end;
      // Reaproveitar uma transacao commitada mandaria o segundo lote SEM
      // MULTI, e o servidor executaria os comandos soltos — atomicidade
      // perdida em silencio.
      TAssert.AssertTrue('transacao encerrada nao aceita mais comando',
        LLevantou);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Count_NaoConta0Multi;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake, []);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      TAssert.AssertEquals(0, LTx.Count);
      LTx.Queue('SET', ['a', '1']);
      TAssert.AssertEquals(1, LTx.Count);
      LTx.Queue('SET', ['b', '2']);
      TAssert.AssertEquals(2, LTx.Count);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Watch_VaiAoServidorNaHora;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake, ['+OK' + CRLF, '+OK' + CRLF]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Watch(['a', 'b']);
      // Ao contrario do Queue, o WATCH nao pode esperar pelo commit: e' entre
      // ele e o MULTI que a leitura acontece.
      TAssert.AssertEquals(Wire(['WATCH', 'a', 'b']), LFake.WrittenText);
      TAssert.AssertTrue('vigiando', LTx.IsWatching);
      LTx.Unwatch;
      TAssert.AssertFalse('parou de vigiar', LTx.IsWatching);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Watch_SemChave_Levanta;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LConn := NovaConexaoFake(LFake, []);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      try
        LTx.Watch([]);
      except
        on E: ERedisTransactionError do
          LLevantou := True;
      end;
      TAssert.AssertTrue('WATCH sem chave', LLevantou);
      TAssert.AssertEquals('', LFake.WrittenText);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Commit_LimpaOEstadoDeWatch;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake,
    ['+OK' + CRLF, RespostaCommit(0, '*0' + CRLF)]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Watch(['a']);
      LTx.Commit;
      // O servidor limpa o WATCH no EXEC. Se a lib nao acompanhasse, o
      // destrutor mandaria um UNWATCH desnecessario — e, pior, o faria numa
      // conexao que ja' voltou ao pool.
      TAssert.AssertFalse('o EXEC ja limpou o watch', LTx.IsWatching);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Discard_MandaUnwatchENaoMandaDiscard;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake, ['+OK' + CRLF, '+OK' + CRLF]);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Watch(['a']);
      LTx.Queue('SET', ['a', '1']);
      LTx.Discard;
      // O MULTI ficou no buffer local e nunca saiu, entao nao ha' o que
      // descartar no servidor. O que PRECISA sair e' o UNWATCH.
      TAssert.AssertEquals(Wire(['WATCH', 'a']) + Wire(['UNWATCH']),
        LFake.WrittenText);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Discard_SemWatch_NaoVaiAoServidor;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake, []);
  try
    LTx := TRedisTransaction.Create(LConn);
    try
      LTx.Queue('SET', ['a', '1']);
      LTx.Discard;
      TAssert.AssertEquals('', LFake.WrittenText);
    finally
      LTx.Free;
    end;
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Destrutor_ComWatchPendente_MandaUnwatch;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake, ['+OK' + CRLF, '+OK' + CRLF]);
  try
    LTx := TRedisTransaction.Create(LConn);
    LTx.Watch(['a']);
    LTx.Free;
    // Conexao devolvida ao pool com WATCH pendente faria o EXEC do PROXIMO
    // usuario abortar sem motivo aparente — e ele nao teria como descobrir por
    // que. Mesma familia do bug de conexao suja.
    TAssert.AssertEquals(Wire(['WATCH', 'a']) + Wire(['UNWATCH']),
      LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Destrutor_SemWatch_NaoMandaNada;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
begin
  LConn := NovaConexaoFake(LFake, []);
  try
    LTx := TRedisTransaction.Create(LConn);
    LTx.Queue('SET', ['a', '1']);
    LTx.Free;
    // Sem WATCH nao ha' estado no servidor: um UNWATCH aqui seria um
    // round-trip por transacao, cobrado de todo mundo para nada.
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LConn.Free;
  end;
end;

procedure TRedisTransactionTests.Destrutor_DevolveAConexaoAQuemEmprestou;
var
  LFake: TRedisFakeServerStream;
  LConn: TRedisConnection;
  LTx: TRedisTransaction;
  LEspia: TDevolucaoEspia;
begin
  LEspia := TDevolucaoEspia.Create;
  LConn := NovaConexaoFake(LFake, []);
  try
    LTx := TRedisTransaction.Create(LConn, LEspia.Devolve);
    TAssert.AssertEquals(0, LEspia.Devolvidas);
    LTx.Free;
    // Sem isso, cada transacao vazaria uma conexao do pool.
    TAssert.AssertEquals(1, LEspia.Devolvidas);
    TAssert.AssertTrue('devolveu a mesma conexao', LEspia.Ultima = LConn);
  finally
    LConn.Free;
    LEspia.Free;
  end;
end;

{ TRedisScriptingTests }

procedure TRedisScriptingTests.Eval_MontaNumkeysChavesEArgumentos;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    LClient.Scripting.Eval(SCRIPT_GET, ['k1', 'k2'], ['a1']);
    // O numero de chaves e' o que separa KEYS de ARGV; errar isso manda um
    // argumento para o lado errado do script, sem erro nenhum no caminho.
    TAssert.AssertEquals(
      Wire(['EVAL', SCRIPT_GET, '2', 'k1', 'k2', 'a1']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.Eval_SemChave_MandaZero;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  try
    LClient.Scripting.Eval('return 1', [], []);
    TAssert.AssertEquals(Wire(['EVAL', 'return 1', '0']), LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.Run_PrimeiraVez_MandaEval;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('v')]);
  try
    TAssert.AssertEquals(0, LClient.Scripting.CachedCount);
    LClient.Scripting.Run(SCRIPT_GET, ['k'], []);
    // Primeira vez manda o texto: o servidor executa E guarda, entao o
    // "aquecimento" nao custa um round-trip extra.
    TAssert.AssertEquals(Wire(['EVAL', SCRIPT_GET, '1', 'k']),
      LFake.WrittenText);
    TAssert.AssertEquals(1, LClient.Scripting.CachedCount);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.Run_SegundaVez_MandaEvalSha;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('v'), Bulk('v')]);
  try
    LClient.Scripting.Run(SCRIPT_GET, ['k'], []);
    LClient.Scripting.Run(SCRIPT_GET, ['k'], []);
    // Da' segunda em diante troca kilobytes de Lua por 40 bytes de SHA.
    TAssert.AssertEquals(
      Wire(['EVAL', SCRIPT_GET, '1', 'k']) +
      Wire(['EVALSHA', SHA_GET, '1', 'k']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.Run_Noscript_ReenviaOEval;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LReply: IRedisReply;
begin
  LClient := NovoCliente(LFake,
    [Bulk('v'), '-NOSCRIPT No matching script' + CRLF, Bulk('depois')]);
  try
    LClient.Scripting.Run(SCRIPT_GET, ['k'], []);   // EVAL, memoriza
    LReply := LClient.Scripting.Run(SCRIPT_GET, ['k'], []);  // EVALSHA -> NOSCRIPT -> EVAL
    // O cache do SERVIDOR foi limpo por fora (SCRIPT FLUSH, restart, failover
    // para replica). A lib reensina o script sozinha e quem chamou nao ve erro
    // nenhum — e' o que torna o cache uma otimizacao que se autocorrige.
    TAssert.AssertEquals('depois', LReply.AsString);
    TAssert.AssertEquals(
      Wire(['EVAL', SCRIPT_GET, '1', 'k']) +
      Wire(['EVALSHA', SHA_GET, '1', 'k']) +
      Wire(['EVAL', SCRIPT_GET, '1', 'k']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.Run_ErroDeLua_LevantaEmVezDeReenviar;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LCodigo: string;
begin
  LCodigo := '';
  LClient := NovoCliente(LFake,
    [Bulk('v'), '-ERR Error running script: bad argument' + CRLF]);
  try
    LClient.Scripting.Run(SCRIPT_GET, ['k'], []);
    try
      LClient.Scripting.Run(SCRIPT_GET, ['k'], []);
    except
      on E: ERedisReplyError do
        LCodigo := E.Code;
    end;
    // So' o NOSCRIPT dispara o reenvio. Um erro do proprio Lua tem de subir:
    // reenviar o EVAL apenas repetiria o mesmo erro, escondendo a causa e
    // dobrando o trafego.
    TAssert.AssertEquals('ERR', LCodigo);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.Run_ScriptComErro_NaoEntraNoCache;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLevantou: Boolean;
begin
  LLevantou := False;
  LClient := NovoCliente(LFake, ['-ERR compile error' + CRLF]);
  try
    try
      LClient.Scripting.Run('isto nao e lua', [], []);
    except
      on E: ERedisReplyError do
        LLevantou := True;
    end;
    TAssert.AssertTrue('erro de compilacao sobe', LLevantou);
    // Script que o servidor recusou nao fica no cache DELE; memorizar aqui
    // condenaria toda chamada seguinte a um EVALSHA inutil seguido de EVAL.
    TAssert.AssertEquals(0, LClient.Scripting.CachedCount);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.ScriptLoad_DevolveEMemorizaOSha;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk(SHA_GET)]);
  try
    TAssert.AssertEquals(SHA_GET, LClient.Scripting.ScriptLoad(SCRIPT_GET));
    TAssert.AssertEquals(Wire(['SCRIPT', 'LOAD', SCRIPT_GET]),
      LFake.WrittenText);
    // Pre-aquecer no start da aplicacao ja' deixa o cache local pronto.
    TAssert.AssertEquals(1, LClient.Scripting.CachedCount);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.ScriptFlush_LimpaOCacheLocal;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('v'), '+OK' + CRLF]);
  try
    LClient.Scripting.Run(SCRIPT_GET, ['k'], []);
    TAssert.AssertEquals(1, LClient.Scripting.CachedCount);
    LClient.Scripting.ScriptFlush;
    // Manter o cache local depois de esvaziar o do servidor seria continuar
    // apostando num EVALSHA que ja' se sabe que vai dar NOSCRIPT.
    TAssert.AssertEquals(0, LClient.Scripting.CachedCount);
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.ScriptExists_LeOArrayDeUmElemento;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake,
    ['*1' + CRLF + ':1' + CRLF, '*1' + CRLF + ':0' + CRLF]);
  try
    TAssert.AssertTrue('o servidor tem', LClient.Scripting.ScriptExists(SHA_GET));
    TAssert.AssertFalse('o servidor nao tem',
      LClient.Scripting.ScriptExists(SHA_ABC));
  finally
    LClient.Free;
  end;
end;

procedure TRedisScriptingTests.ClearCache_EsqueceSemFalarComOServidor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
begin
  LClient := NovoCliente(LFake, [Bulk('v')]);
  try
    LClient.Scripting.Run(SCRIPT_GET, ['k'], []);
    LClient.Scripting.ClearCache;
    TAssert.AssertEquals(0, LClient.Scripting.CachedCount);
    // Nenhum comando alem do EVAL da primeira chamada.
    TAssert.AssertEquals(Wire(['EVAL', SCRIPT_GET, '1', 'k']),
      LFake.WrittenText);
  finally
    LClient.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisScriptShaTests);
  TDUnitX.RegisterTestFixture(TRedisTransactionTests);
  TDUnitX.RegisterTestFixture(TRedisScriptingTests);

end.
