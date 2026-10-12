unit Redis.Tracing;

{ Spans do OpenTelemetry para os comandos da lib, sobre o PascalCommon.Tracing
  da pascal-common-faa (fase D do docs/observability-design.md da
  pascal-api-infra-faa).

  Quem abre os spans e' a propria lib — a conexao (um por comando, um por
  pipeline) e o pool (a espera por uma conexao livre). A aplicacao so' liga o
  tracer com TPcTracing.Start; os spans entram na trace do span corrente da
  thread (o span de servidor de uma requisicao HTTP, tipicamente). Esta unit
  so' tem as pecas: nome da operacao, texto sanitizado e os atributos.

  Decisoes (racional na secao 51 de docs/DECISOES.md):
  - Desligado nao custa nada: com TPcTracing.Enabled falso nao se cria span,
    nem os ids, nem o texto do comando.
  - db.query.text leva so' o comando e a PRIMEIRA CHAVE, com o resto trocado
    por um '?': 'SET sessao:42 ?'. Valor no Redis e' com frequencia segredo
    (token, sessao, senha num AUTH chamado pelo Execute), e a chave e' o que
    se precisa para achar a chave quente. Comando sem chave (PING, INFO,
    AUTH, HELLO) nao mostra argumento nenhum.
  - O nome do span e' a operacao: o comando em maiusculas, mais o subcomando
    nos comandos que sao containers (CLIENT SETNAME, XGROUP CREATE). Baixa
    cardinalidade, nunca a chave.
  - Resposta de erro do servidor e' status de erro, com o codigo
    (WRONGTYPE, NOSCRIPT) em db.response.status_code e error.type — inclusive
    os erros que a lib trata como recado (o NOSCRIPT que o Scripting.Run
    reenvia, o BUSYGROUP do XGroupTryCreate): no nivel da conexao nao se sabe
    quem vai tratar o erro, e o span conta o que o servidor respondeu.
  - O span nasce e termina na mesma chamada, na mesma thread: e' a regra de
    threads do PascalCommon.Tracing, e por isso nao precisa de span
    destacado. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  PascalCommon.Tracing;

/// Nome do span e db.operation.name: o comando em maiusculas, mais o
/// subcomando quando o comando e' um container (CLIENT, CONFIG, XGROUP...).
/// AArgs[AFirst] e' o primeiro argumento depois do nome.
function RedisSpanOperation(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer): string;

/// db.query.text: a operacao, a primeira chave quando o comando tem uma, e um
/// ' ?' no lugar do que sobrar ('SET sessao:42 ?', 'GET sessao:42',
/// 'AUTH ?'). Chave longa e' cortada em 128 caracteres.
function RedisSpanQueryText(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer): string;

/// Abre o span de um comando (kind client), que vira o span corrente da
/// thread. nil quando o tracer esta' desligado.
function RedisStartCommandSpan(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer; const AParams: TRedisParams; ADatabase: Integer): IPcSpan;

/// Abre o span de um lote: AOperation e' 'PIPELINE' ou 'MULTI'. nil quando o
/// tracer esta' desligado.
function RedisStartBatchSpan(const AOperation: string; ABatchSize: Integer;
  const AParams: TRedisParams; ADatabase: Integer): IPcSpan;

/// Termina o span; com AError, status de erro com a mensagem e error.type com
/// a classe. ASpan nil e' no-op.
procedure RedisFinishSpan(const ASpan: IPcSpan; AError: Exception);

/// Termina o span de um comando que devolveu AReply: um rkError vira status
/// de erro, com o codigo em db.response.status_code e error.type.
procedure RedisFinishReplySpan(const ASpan: IPcSpan; const AReply: IRedisReply);

/// Termina o span de um lote. Itens rkError (e, num MULTI, os do array do
/// EXEC) contam em pascalredis.batch.error_count, e o primeiro vira o status.
procedure RedisFinishBatchSpan(const ASpan: IPcSpan; const AReplies: TRedisReplyArray;
  AIsMulti: Boolean);

/// Abre o span 'pool wait' (kind internal). nil quando o tracer esta'
/// desligado.
function RedisStartPoolWaitSpan(AMaxSize: Integer; const AParams: TRedisParams): IPcSpan;

/// Termina o span 'pool wait' com o numero de esperas.
procedure RedisFinishPoolWaitSpan(const ASpan: IPcSpan; AAttempts: Integer;
  AError: Exception);

implementation

const
  REDIS_SPAN_KEY_MAX = 128;
  REDIS_SPAN_SUBCOMMAND_MAX = 32;

function SameWord(const AWord: string; const AList: array of string): Boolean;
var
  I: Integer;
begin
  for I := Low(AList) to High(AList) do
    if AWord = AList[I] then
      Exit(True);
  Result := False;
end;

/// Comandos cujo primeiro argumento e' um subcomando.
function IsContainer(const AName: string): Boolean;
begin
  Result := SameWord(AName, ['ACL', 'CLIENT', 'CLUSTER', 'COMMAND', 'CONFIG',
    'DEBUG', 'FUNCTION', 'LATENCY', 'MEMORY', 'MODULE', 'OBJECT', 'PUBSUB',
    'SCRIPT', 'SLOWLOG', 'XGROUP', 'XINFO']);
end;

/// Containers cuja chave vem logo depois do subcomando (XGROUP CREATE chave).
function IsKeyedContainer(const AName: string): Boolean;
begin
  Result := SameWord(AName, ['MEMORY', 'OBJECT', 'XGROUP', 'XINFO']);
end;

/// Comandos sem chave: o primeiro argumento, se houver, nao vai para o span.
function HasNoKey(const AName: string): Boolean;
begin
  Result := SameWord(AName, ['ASKING', 'AUTH', 'BGREWRITEAOF', 'BGSAVE',
    'DBSIZE', 'DISCARD', 'ECHO', 'EXEC', 'FAILOVER', 'FLUSHALL', 'FLUSHDB',
    'HELLO', 'INFO', 'LASTSAVE', 'LOLWUT', 'MIGRATE', 'MONITOR', 'MULTI',
    'PING', 'PSYNC', 'QUIT', 'RANDOMKEY', 'READONLY', 'READWRITE', 'REPLCONF',
    'REPLICAOF', 'RESET', 'ROLE', 'SAVE', 'SCAN', 'SELECT', 'SHUTDOWN',
    'SLAVEOF', 'SWAPDB', 'SYNC', 'TIME', 'UNWATCH', 'WAIT', 'WAITAOF']);
end;

/// Posicao do numkeys nos comandos em que a lista de chaves vem depois dele;
/// -1 nos demais.
function NumKeysIndex(const AName: string): Integer;
begin
  if SameWord(AName, ['LMPOP', 'ZMPOP', 'SINTERCARD', 'ZDIFF', 'ZINTER',
    'ZINTERCARD', 'ZUNION']) then
    Result := 0
  else if SameWord(AName, ['BLMPOP', 'BZMPOP', 'EVAL', 'EVALSHA', 'EVAL_RO',
    'EVALSHA_RO', 'FCALL', 'FCALL_RO']) then
    Result := 1
  else
    Result := -1;
end;

function IsSubcommandWord(const AText: string): Boolean;
var
  I: Integer;
begin
  Result := (AText <> '') and (Length(AText) <= REDIS_SPAN_SUBCOMMAND_MAX);
  if not Result then
    Exit;
  for I := 1 to Length(AText) do
    if not CharInSet(AText[I], ['A'..'Z', 'a'..'z', '0'..'9', '-', '_']) then
      Exit(False);
end;

function ArgText(const AArgs: array of TRedisArg; AIndex: Integer): string;
begin
  Result := RedisUtf8Decode(AArgs[AIndex].Bytes);
end;

function CommandName(const AName: string): string;
begin
  Result := UpperCase(Trim(AName));
end;

/// Subcomando a juntar na operacao, ou '' quando nao ha'.
function SubcommandOf(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer): string;
begin
  Result := '';
  if IsContainer(AName) and (Length(AArgs) > AFirst) then
  begin
    Result := UpperCase(ArgText(AArgs, AFirst));
    if not IsSubcommandWord(Result) then
      Result := '';
  end;
end;

/// Posicao (absoluta em AArgs) da primeira chave; -1 quando nao ha'.
function KeyIndex(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer; AHasSubcommand: Boolean): Integer;
var
  LNumKeysAt, LNumKeys, I: Integer;
begin
  Result := -1;
  if IsContainer(AName) then
  begin
    if AHasSubcommand and IsKeyedContainer(AName) then
      Result := AFirst + 1;
  end
  else if HasNoKey(AName) then
    Exit
  else if (AName = 'XREAD') or (AName = 'XREADGROUP') then
  begin
    for I := AFirst to High(AArgs) - 1 do
      if UpperCase(ArgText(AArgs, I)) = 'STREAMS' then
        Exit(I + 1);
  end
  else
  begin
    LNumKeysAt := NumKeysIndex(AName);
    if LNumKeysAt < 0 then
      Result := AFirst
    else if (Length(AArgs) > AFirst + LNumKeysAt) and
      TryStrToInt(ArgText(AArgs, AFirst + LNumKeysAt), LNumKeys) and (LNumKeys >= 1) then
      Result := AFirst + LNumKeysAt + 1;
  end;
  if Result >= Length(AArgs) then
    Result := -1;
end;

function RedisSpanOperation(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer): string;
var
  LSub: string;
begin
  Result := CommandName(AName);
  LSub := SubcommandOf(Result, AArgs, AFirst);
  if LSub <> '' then
    Result := Result + ' ' + LSub;
end;

function RedisSpanQueryText(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer): string;
var
  LName, LSub, LKey: string;
  LShown, LKeyAt: Integer;
begin
  LName := CommandName(AName);
  LSub := SubcommandOf(LName, AArgs, AFirst);
  Result := LName;
  LShown := 0;
  if LSub <> '' then
  begin
    Result := Result + ' ' + LSub;
    Inc(LShown);
  end;
  LKeyAt := KeyIndex(LName, AArgs, AFirst, LSub <> '');
  if LKeyAt >= 0 then
  begin
    LKey := ArgText(AArgs, LKeyAt);
    if Length(LKey) > REDIS_SPAN_KEY_MAX then
      LKey := Copy(LKey, 1, REDIS_SPAN_KEY_MAX) + '...';
    Result := Result + ' ' + LKey;
    Inc(LShown);
  end;
  if Length(AArgs) - AFirst > LShown then
    Result := Result + ' ?';
end;

procedure SetServerAttributes(const ASpan: IPcSpan; const AParams: TRedisParams;
  ADatabase: Integer);
begin
  ASpan.SetAttribute('db.system.name', 'redis');
  ASpan.SetAttribute('db.namespace', IntToStr(ADatabase));
  if AParams.Host <> '' then
  begin
    ASpan.SetAttribute('server.address', AParams.Host);
    ASpan.SetIntAttribute('server.port', AParams.Port);
  end;
end;

function RedisStartCommandSpan(const AName: string; const AArgs: array of TRedisArg;
  AFirst: Integer; const AParams: TRedisParams; ADatabase: Integer): IPcSpan;
var
  LOperation: string;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  LOperation := RedisSpanOperation(AName, AArgs, AFirst);
  Result := TPcTracing.StartSpan(LOperation, skClient);
  Result.SetAttribute('db.operation.name', LOperation);
  Result.SetAttribute('db.query.text', RedisSpanQueryText(AName, AArgs, AFirst));
  SetServerAttributes(Result, AParams, ADatabase);
end;

function RedisStartBatchSpan(const AOperation: string; ABatchSize: Integer;
  const AParams: TRedisParams; ADatabase: Integer): IPcSpan;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  Result := TPcTracing.StartSpan(AOperation, skClient);
  Result.SetAttribute('db.operation.name', AOperation);
  Result.SetIntAttribute('db.operation.batch.size', ABatchSize);
  SetServerAttributes(Result, AParams, ADatabase);
end;

procedure RedisFinishSpan(const ASpan: IPcSpan; AError: Exception);
begin
  if ASpan = nil then
    Exit;
  if Assigned(AError) then
  begin
    ASpan.SetAttribute('error.type', AError.ClassName);
    ASpan.SetStatus(ssError, AError.Message);
  end;
  ASpan.Finish;
end;

procedure SetReplyError(const ASpan: IPcSpan; const AReply: IRedisReply;
  const AMessage: string);
var
  LCode: string;
begin
  LCode := AReply.ErrorCode;
  if LCode = '' then
    LCode := 'ERR';
  ASpan.SetAttribute('db.response.status_code', LCode);
  ASpan.SetAttribute('error.type', LCode);
  ASpan.SetStatus(ssError, AMessage);
end;

procedure RedisFinishReplySpan(const ASpan: IPcSpan; const AReply: IRedisReply);
begin
  if ASpan = nil then
    Exit;
  if (AReply <> nil) and AReply.IsError then
    SetReplyError(ASpan, AReply, AReply.ErrorMessage);
  ASpan.Finish;
end;

procedure RedisFinishBatchSpan(const ASpan: IPcSpan; const AReplies: TRedisReplyArray;
  AIsMulti: Boolean);
var
  LErrors, LTotal, I: Integer;
  LFirst, LExec: IRedisReply;

  procedure Tally(const AReply: IRedisReply);
  begin
    Inc(LTotal);
    if (AReply <> nil) and AReply.IsError then
    begin
      Inc(LErrors);
      if LFirst = nil then
        LFirst := AReply;
    end;
  end;

begin
  if ASpan = nil then
    Exit;
  LErrors := 0;
  LTotal := 0;
  LFirst := nil;
  for I := 0 to High(AReplies) do
    Tally(AReplies[I]);
  // Num MULTI, o erro de EXECUCAO de um comando vem dentro do array do EXEC
  // (os outros ficam gravados: nao e' rollback). Contar so' o nivel de cima
  // esconderia justo esse.
  if AIsMulti and (Length(AReplies) > 0) then
  begin
    LExec := AReplies[High(AReplies)];
    if (LExec <> nil) and (LExec.Kind = rkArray) then
      for I := 0 to LExec.Count - 1 do
        Tally(LExec[I]);
  end;
  if LErrors > 0 then
  begin
    ASpan.SetIntAttribute('pascalredis.batch.error_count', LErrors);
    SetReplyError(ASpan, LFirst, Format('%d de %d respostas com erro; a primeira: %s',
      [LErrors, LTotal, LFirst.ErrorMessage]));
  end;
  ASpan.Finish;
end;

function RedisStartPoolWaitSpan(AMaxSize: Integer; const AParams: TRedisParams): IPcSpan;
begin
  Result := nil;
  if not TPcTracing.Enabled then
    Exit;
  Result := TPcTracing.StartSpan('pool wait', skInternal);
  Result.SetAttribute('db.system.name', 'redis');
  Result.SetIntAttribute('pascalredis.pool.max_size', AMaxSize);
  if AParams.Host <> '' then
  begin
    Result.SetAttribute('server.address', AParams.Host);
    Result.SetIntAttribute('server.port', AParams.Port);
  end;
end;

procedure RedisFinishPoolWaitSpan(const ASpan: IPcSpan; AAttempts: Integer;
  AError: Exception);
begin
  if ASpan = nil then
    Exit;
  ASpan.SetIntAttribute('pascalredis.pool.wait_attempts', AAttempts);
  RedisFinishSpan(ASpan, AError);
end;

end.
