unit Redis.LockTests;

{ Testes de Redis.Lock (DUnitX). E' o mestre do espelho FPCUnit
  tests\Unit\fpc\Redis.LockTests.pas, gerado daqui por
  tools/gen_fpc_mirror.py: mude este arquivo, nunca o espelho.

  Sem servidor: o cliente fala com o TRedisFakeServerStream da
  Redis.ConnectionTests, e o teste confere os bytes que foram para o fio (SET
  com NX e PX na ordem, o script de dono com a chave em KEYS e o token em
  ARGV) e como cada resposta virou desfecho. Um servidor falso sem resposta
  roteirizada "fecha a conexao": e' o ERedisConnectionLost que o lock trata
  como indisponibilidade. A expiracao de verdade fica para a integracao. }

interface

uses
  DUnitX.TestFramework,
  Redis.DUnitXCompat,
  SysUtils,
  Classes,
  Redis.Types,
  Redis.Connection,
  Redis.Client,
  Redis.Lock,
  Redis.ConnectionTests;

type
  { Guarda os avisos do OnUnavailable. }
  TRedisLockAvisos = class
  public
    Texto: string;
    procedure Aviso(ASender: TObject; const AOperation, AKey: string; AError: Exception);
  end;

  [TestFixture]
  TRedisLockTests = class
  public
    [Test] procedure NewToken_32HexDiferentesACadaVez;
    [Test] procedure TryAcquire_Livre_SetNxPxComOTokenEOPrefixo;
    [Test] procedure TryAcquire_Ocupado_DevolveFalseSemToken;
    [Test] procedure TryAcquire_RedisFora_Levanta;
    [Test] procedure Acquire_Livre_Ocupado;
    [Test] procedure Acquire_RedisFora_IndisponivelEAvisa;
    [Test] procedure Acquire_ErroDoServidor_Sobe;
    [Test] procedure Release_ScriptComChaveETokenSeparados;
    [Test] procedure Release_NaoEraMais_DevolveFalse;
    [Test] procedure Release_SemToken_NaoVaiAoServidor;
    [Test] procedure Release_RedisFora_FalseEAvisaSemOToken;
    [Test] procedure Extend_MandaOPrazoEmArgv2;
    [Test] procedure Prazo_ZeroOuNegativo_Levanta;
  end;

implementation

const
  CRLF = #13#10;

// Unified request protocol esperado no fio. So' ASCII.
function Wire(const AArgs: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AArgs)) + CRLF;
  for I := 0 to High(AArgs) do
    Result := Result + '$' + IntToStr(Length(AArgs[I])) + CRLF + AArgs[I] + CRLF;
end;

// Cliente amarrado a uma conexao sobre o servidor falso; quem chama e' dono.
function NovoCliente(out AFake: TRedisFakeServerStream;
  const AResponses: array of string): TRedisClient;
var
  LConn: TRedisConnection;
begin
  AFake := TRedisFakeServerStream.Create(AResponses);
  LConn := TRedisConnection.CreateOnStream(AFake, RedisDefaultParams);
  try
    LConn.Open;
    Result := TRedisClient.CreateOnConnection(LConn, True);
  except
    LConn.Free;
    raise;
  end;
end;

// A classe da excecao de TryAcquire, ou '' quando nao levantou.
function ClasseDoTryAcquire(ALock: TRedisLock; const AKey: string): string;
var
  LToken: string;
begin
  Result := '';
  try
    ALock.TryAcquire(AKey, 1000, LToken);
  except
    on E: Exception do
      Result := E.ClassName;
  end;
end;

{ TRedisLockAvisos }

procedure TRedisLockAvisos.Aviso(ASender: TObject; const AOperation, AKey: string;
  AError: Exception);
begin
  Texto := Texto + AOperation + ' ' + AKey + ' ' + AError.ClassName + ';';
end;

{ TRedisLockTests }

procedure TRedisLockTests.NewToken_32HexDiferentesACadaVez;
var
  A, B: string;
  I: Integer;
begin
  A := TRedisLock.NewToken;
  B := TRedisLock.NewToken;
  TAssert.AssertEquals(32, Length(A));
  for I := 1 to Length(A) do
    TAssert.AssertTrue('hex minusculo', CharInSet(A[I], ['0'..'9', 'a'..'f']));
  TAssert.AssertFalse('um token por aquisicao', A = B);
end;

procedure TRedisLockTests.TryAcquire_Livre_SetNxPxComOTokenEOPrefixo;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken: string;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF]);
  LLock := TRedisLock.Create(LClient, 'app:lock:');
  try
    TAssert.AssertTrue(LLock.TryAcquire('pedido:7', 10000, LToken));
    TAssert.AssertEquals(32, Length(LToken));
    // NX e PX na ordem da especificacao; o prefixo antes da chave.
    TAssert.AssertEquals(Wire(['SET', 'app:lock:pedido:7', LToken, 'NX', 'PX', '10000']),
      LFake.WrittenText);
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.TryAcquire_Ocupado_DevolveFalseSemToken;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken: string;
begin
  LClient := NovoCliente(LFake, ['$-1' + CRLF]);
  LLock := TRedisLock.Create(LClient);
  try
    TAssert.AssertFalse(LLock.TryAcquire('k', 1000, LToken));
    TAssert.AssertEquals('', LToken);
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.TryAcquire_RedisFora_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
begin
  LClient := NovoCliente(LFake, []);
  LLock := TRedisLock.Create(LClient);
  try
    TAssert.AssertEquals('ERedisConnectionLost', ClasseDoTryAcquire(LLock, 'k'));
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Acquire_Livre_Ocupado;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken: string;
begin
  LClient := NovoCliente(LFake, ['+OK' + CRLF, '$-1' + CRLF]);
  LLock := TRedisLock.Create(LClient);
  try
    TAssert.AssertTrue('livre', LLock.Acquire('k', 1000, LToken) = rlAcquired);
    TAssert.AssertEquals(32, Length(LToken));
    TAssert.AssertTrue('ocupado', LLock.Acquire('k', 1000, LToken) = rlBusy);
    TAssert.AssertEquals('', LToken);
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Acquire_RedisFora_IndisponivelEAvisa;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LAvisos: TRedisLockAvisos;
  LToken: string;
begin
  LClient := NovoCliente(LFake, []);
  LLock := TRedisLock.Create(LClient, 'app:');
  LAvisos := TRedisLockAvisos.Create;
  try
    LLock.OnUnavailable := LAvisos.Aviso;
    TAssert.AssertTrue(LLock.Acquire('k', 1000, LToken) = rlUnavailable);
    TAssert.AssertEquals('', LToken);
    TAssert.AssertEquals('acquire app:k ERedisConnectionLost;', LAvisos.Texto);
  finally
    LAvisos.Free;
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Acquire_ErroDoServidor_Sobe;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken, LClasse: string;
begin
  // Um erro que nao e' de disponibilidade e' bug: nao vira "seguir sem lock".
  LClient := NovoCliente(LFake, ['-WRONGTYPE Operation against a key holding the wrong kind' + CRLF]);
  LLock := TRedisLock.Create(LClient);
  try
    LClasse := '';
    try
      LLock.Acquire('k', 1000, LToken);
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisReplyError', LClasse);
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Release_ScriptComChaveETokenSeparados;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF]);
  LLock := TRedisLock.Create(LClient, 'app:');
  try
    TAssert.AssertTrue(LLock.Release('k', 'tok'));
    // Primeira vez: EVAL com o texto (o Run cuida do EVALSHA depois). A chave
    // em KEYS, o token em ARGV: trocar os dois compararia a chave com ela
    // mesma.
    TAssert.AssertEquals(Wire(['EVAL', REDIS_LOCK_RELEASE_SCRIPT, '1', 'app:k', 'tok']),
      LFake.WrittenText);
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Release_NaoEraMais_DevolveFalse;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
begin
  LClient := NovoCliente(LFake, [':0' + CRLF]);
  LLock := TRedisLock.Create(LClient);
  try
    TAssert.AssertFalse(LLock.Release('k', 'tok'));
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Release_SemToken_NaoVaiAoServidor;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
begin
  LClient := NovoCliente(LFake, []);
  LLock := TRedisLock.Create(LClient);
  try
    TAssert.AssertFalse(LLock.Release('k', ''));
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Release_RedisFora_FalseEAvisaSemOToken;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LAvisos: TRedisLockAvisos;
begin
  LClient := NovoCliente(LFake, []);
  LLock := TRedisLock.Create(LClient);
  LAvisos := TRedisLockAvisos.Create;
  try
    LLock.OnUnavailable := LAvisos.Aviso;
    TAssert.AssertFalse(LLock.Release('k', 'segredo-do-dono'));
    TAssert.AssertEquals('release k ERedisConnectionLost;', LAvisos.Texto);
    TAssert.AssertTrue('o token nao aparece', Pos('segredo', LAvisos.Texto) = 0);
  finally
    LAvisos.Free;
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Extend_MandaOPrazoEmArgv2;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
begin
  LClient := NovoCliente(LFake, [':1' + CRLF, ':0' + CRLF]);
  LLock := TRedisLock.Create(LClient);
  try
    TAssert.AssertTrue(LLock.Extend('k', 'tok', 5000));
    TAssert.AssertEquals(Wire(['EVAL', REDIS_LOCK_EXTEND_SCRIPT, '1', 'k', 'tok', '5000']),
      LFake.WrittenText);
    TAssert.AssertFalse('nao era mais sua', LLock.Extend('k', 'tok', 5000));
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

procedure TRedisLockTests.Prazo_ZeroOuNegativo_Levanta;
var
  LFake: TRedisFakeServerStream;
  LClient: TRedisClient;
  LLock: TRedisLock;
  LToken, LClasse: string;
begin
  LClient := NovoCliente(LFake, []);
  LLock := TRedisLock.Create(LClient);
  try
    LClasse := '';
    try
      LLock.TryAcquire('k', 0, LToken);
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('EArgumentException', LClasse);
    TAssert.AssertEquals('nada foi ao servidor', '', LFake.WrittenText);
  finally
    LLock.Free;
    LClient.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisLockTests);

end.
