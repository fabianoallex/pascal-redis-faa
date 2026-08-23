unit Redis.Commands.Lists;

{ Comandos de lista: uma sequencia ordenada com insercao e remocao baratas nas
  duas pontas. E' a fila de trabalho classica do Redis (LPUSH de um lado, BRPOP
  do outro).

  Esta e' a primeira familia com comandos BLOQUEANTES, e eles nao sao um
  Execute como os outros: um BLPOP de 30 s numa conexao do pool com timeout de
  5 s morreria de timeout de socket ANTES de o comando terminar, e a resposta
  chegaria depois, contaminando a proxima conexao. Por isso passam por
  TRedisCommandExecutor.ExecuteBlocking, que os manda por uma conexao fora do
  pool comum, com o read timeout esticado para alem do timeout do comando.
  Ver docs/DECISOES.md.

  ATimeoutSeconds aceita fracao (Redis 6+) e ZERO significa esperar para
  sempre — no comando e no socket. Espera infinita so' faz sentido em thread
  dedicada: nao ha' como cancela-la a nao ser derrubando a conexao (Abort). }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

type
  /// Ponta da lista, para LMOVE/BLMOVE.
  TRedisListEnd = (leLeft, leRight);

  /// Posicao relativa ao pivo, para LINSERT.
  TRedisInsertWhere = (iwBefore, iwAfter);

  /// Comandos de lista (LPUSH, RPOP, LRANGE, BLPOP...).
  TRedisListsCommands = class(TRedisCommandFamily)
  public
    /// LPUSH/RPUSH de um valor. Devolve o tamanho da lista depois da insercao.
    function LPush(const AKey, AValue: TRedisArg): Int64;
    function RPush(const AKey, AValue: TRedisArg): Int64;
    /// LPUSH de varios. ATENCAO a ordem: LPUSH k a b c deixa a lista como
    /// c, b, a — cada valor entra na frente do anterior.
    function LPushMany(const AKey: TRedisArg;
      const AValues: array of TRedisArg): Int64;
    /// RPUSH de varios; aqui a ordem do array e' a ordem final.
    function RPushMany(const AKey: TRedisArg;
      const AValues: array of TRedisArg): Int64;
    /// LPUSHX/RPUSHX: so' insere se a lista JA' existir. Devolve 0 se nao.
    function LPushX(const AKey, AValue: TRedisArg): Int64;
    function RPushX(const AKey, AValue: TRedisArg): Int64;

    /// LPOP/RPOP cru: NULO quando a lista esta' vazia ou nao existe.
    function LPop(const AKey: TRedisArg): IRedisReply;
    function RPop(const AKey: TRedisArg): IRedisReply;
    /// LPOP com contagem (Redis 6.2+): tira ate' ACount elementos de uma vez.
    function LPopCount(const AKey: TRedisArg; ACount: Integer): TRedisStringArray;
    function RPopCount(const AKey: TRedisArg; ACount: Integer): TRedisStringArray;

    function LLen(const AKey: TRedisArg): Int64;

    /// LRANGE com os dois extremos INCLUIDOS e indice negativo contando do fim.
    /// LRange(k, 0, -1) e' a lista inteira.
    function LRange(const AKey: TRedisArg; AStart, AStop: Int64): TRedisStringArray;
    /// LINDEX cru: NULO quando o indice esta' fora da lista.
    function LIndex(const AKey: TRedisArg; AIndex: Int64): IRedisReply;
    /// LSET. Levanta ERedisReplyError se o indice estiver fora da lista.
    procedure LSet(const AKey: TRedisArg; AIndex: Int64; const AValue: TRedisArg);
    /// LTRIM: mantem so' a faixa e descarta o resto. E' o jeito de limitar o
    /// tamanho de um log em lista.
    procedure LTrim(const AKey: TRedisArg; AStart, AStop: Int64);

    /// LINSERT antes ou depois da primeira ocorrencia do pivo. Devolve o novo
    /// tamanho, ou -1 quando o pivo nao foi encontrado (e 0 quando a chave nao
    /// existe) — nao e' erro, e por isso o retorno nao e' Boolean.
    function LInsert(const AKey: TRedisArg; AWhere: TRedisInsertWhere;
      const APivot, AValue: TRedisArg): Int64;

    /// LREM. ACount > 0 remove do inicio para o fim, < 0 do fim para o inicio
    /// e 0 remove todas as ocorrencias. Devolve quantas saiu.
    function LRem(const AKey: TRedisArg; ACount: Int64;
      const AValue: TRedisArg): Int64;

    /// LPOS: indice da primeira ocorrencia do valor. NULO se nao houver.
    function LPos(const AKey, AValue: TRedisArg): IRedisReply;

    /// RPOPLPUSH: tira do fim da origem e poe no inicio do destino, atomico.
    /// Origem e destino podem ser a MESMA chave (fila circular).
    /// Substituido pelo LMove no Redis 6.2+, mas ainda funciona.
    function RPopLPush(const ASource, ADestination: TRedisArg): IRedisReply;
    /// LMOVE: o RPOPLPUSH generalizado, com as duas pontas escolhiveis
    /// (Redis 6.2+).
    function LMove(const ASource, ADestination: TRedisArg;
      AFrom, ATo: TRedisListEnd): IRedisReply;

    { --- Bloqueantes: vao por conexao fora do pool comum --- }

    /// BLPOP em varias chaves. Espera ate' ATimeoutSeconds (0 = para sempre).
    ///
    /// Devolve False quando o prazo venceu sem nada chegar — que e' o caso
    /// NORMAL de um worker ocioso, nao um erro. Quando devolve True, AKey diz
    /// de QUAL das chaves veio o valor: com varias chaves na chamada, e' a
    /// unica forma de saber.
    function BLPop(const AKeys: array of TRedisArg; ATimeoutSeconds: Double;
      out AKey, AValue: string): Boolean;
    function BLPopBytes(const AKeys: array of TRedisArg; ATimeoutSeconds: Double;
      out AKey: string; out AValue: TBytes): Boolean;
    /// BRPOP: identico, tirando do fim da lista. Com LPUSH do outro lado, e' a
    /// fila FIFO.
    function BRPop(const AKeys: array of TRedisArg; ATimeoutSeconds: Double;
      out AKey, AValue: string): Boolean;
    function BRPopBytes(const AKeys: array of TRedisArg; ATimeoutSeconds: Double;
      out AKey: string; out AValue: TBytes): Boolean;

    /// BRPOPLPUSH / BLMOVE: a fila confiavel. O valor sai da origem e entra na
    /// lista de "em processamento" no mesmo instante, entao um worker que
    /// morre no meio nao leva a tarefa junto. NULO quando o prazo vence.
    function BRPopLPush(const ASource, ADestination: TRedisArg;
      ATimeoutSeconds: Double): IRedisReply;
    function BLMove(const ASource, ADestination: TRedisArg;
      AFrom, ATo: TRedisListEnd; ATimeoutSeconds: Double): IRedisReply;
  end;

implementation

{ Helpers }

function EndName(AValue: TRedisListEnd): string;
begin
  if AValue = leLeft then
    Result := 'LEFT'
  else
    Result := 'RIGHT';
end;

{ TRedisListsCommands }

function TRedisListsCommands.LPush(const AKey, AValue: TRedisArg): Int64;
begin
  Result := CmdInt('LPUSH', [AKey, AValue]);
end;

function TRedisListsCommands.RPush(const AKey, AValue: TRedisArg): Int64;
begin
  Result := CmdInt('RPUSH', [AKey, AValue]);
end;

function TRedisListsCommands.LPushMany(const AKey: TRedisArg;
  const AValues: array of TRedisArg): Int64;
begin
  if Length(AValues) = 0 then
    raise ERedisException.Create('LPUSH sem valor');
  Result := CmdInt('LPUSH', RedisArgs([AKey], AValues));
end;

function TRedisListsCommands.RPushMany(const AKey: TRedisArg;
  const AValues: array of TRedisArg): Int64;
begin
  if Length(AValues) = 0 then
    raise ERedisException.Create('RPUSH sem valor');
  Result := CmdInt('RPUSH', RedisArgs([AKey], AValues));
end;

function TRedisListsCommands.LPushX(const AKey, AValue: TRedisArg): Int64;
begin
  Result := CmdInt('LPUSHX', [AKey, AValue]);
end;

function TRedisListsCommands.RPushX(const AKey, AValue: TRedisArg): Int64;
begin
  Result := CmdInt('RPUSHX', [AKey, AValue]);
end;

function TRedisListsCommands.LPop(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('LPOP', [AKey]);
end;

function TRedisListsCommands.RPop(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('RPOP', [AKey]);
end;

function TRedisListsCommands.LPopCount(const AKey: TRedisArg;
  ACount: Integer): TRedisStringArray;
begin
  Result := CmdStrings('LPOP', [AKey, ACount]);
end;

function TRedisListsCommands.RPopCount(const AKey: TRedisArg;
  ACount: Integer): TRedisStringArray;
begin
  Result := CmdStrings('RPOP', [AKey, ACount]);
end;

function TRedisListsCommands.LLen(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('LLEN', [AKey]);
end;

function TRedisListsCommands.LRange(const AKey: TRedisArg;
  AStart, AStop: Int64): TRedisStringArray;
begin
  Result := CmdStrings('LRANGE', [AKey, AStart, AStop]);
end;

function TRedisListsCommands.LIndex(const AKey: TRedisArg;
  AIndex: Int64): IRedisReply;
begin
  Result := Cmd('LINDEX', [AKey, AIndex]);
end;

procedure TRedisListsCommands.LSet(const AKey: TRedisArg; AIndex: Int64;
  const AValue: TRedisArg);
begin
  CmdVoid('LSET', [AKey, AIndex, AValue]);
end;

procedure TRedisListsCommands.LTrim(const AKey: TRedisArg; AStart, AStop: Int64);
begin
  CmdVoid('LTRIM', [AKey, AStart, AStop]);
end;

function TRedisListsCommands.LInsert(const AKey: TRedisArg;
  AWhere: TRedisInsertWhere; const APivot, AValue: TRedisArg): Int64;
var
  LWhere: string;
begin
  if AWhere = iwBefore then
    LWhere := 'BEFORE'
  else
    LWhere := 'AFTER';
  Result := CmdInt('LINSERT', [AKey, LWhere, APivot, AValue]);
end;

function TRedisListsCommands.LRem(const AKey: TRedisArg; ACount: Int64;
  const AValue: TRedisArg): Int64;
begin
  Result := CmdInt('LREM', [AKey, ACount, AValue]);
end;

function TRedisListsCommands.LPos(const AKey, AValue: TRedisArg): IRedisReply;
begin
  Result := Cmd('LPOS', [AKey, AValue]);
end;

function TRedisListsCommands.RPopLPush(const ASource,
  ADestination: TRedisArg): IRedisReply;
begin
  Result := Cmd('RPOPLPUSH', [ASource, ADestination]);
end;

function TRedisListsCommands.LMove(const ASource, ADestination: TRedisArg;
  AFrom, ATo: TRedisListEnd): IRedisReply;
begin
  Result := Cmd('LMOVE', [ASource, ADestination, EndName(AFrom), EndName(ATo)]);
end;

{ Bloqueantes }

function TRedisListsCommands.BLPopBytes(const AKeys: array of TRedisArg;
  ATimeoutSeconds: Double; out AKey: string; out AValue: TBytes): Boolean;
var
  LReply: IRedisReply;
begin
  AKey := '';
  AValue := nil;
  if Length(AKeys) = 0 then
    raise ERedisException.Create('BLPOP sem chave');
  LReply := Executor.ExecuteBlocking('BLPOP',
    RedisArgs(AKeys, [ATimeoutSeconds]), ATimeoutSeconds);
  // Prazo vencido: NULO em RESP2 ($-1 / *-1) e tambem em RESP3 (_).
  Result := not LReply.IsNull;
  if not Result then
    Exit;
  if (not LReply.IsAggregate) or (LReply.Count <> 2) then
    raise ERedisTypeError.Create('BLPOP: esperava par [chave, valor]');
  AKey := LReply[0].AsString;
  AValue := LReply[1].AsBytes;
end;

function TRedisListsCommands.BLPop(const AKeys: array of TRedisArg;
  ATimeoutSeconds: Double; out AKey, AValue: string): Boolean;
var
  LBytes: TBytes;
begin
  Result := BLPopBytes(AKeys, ATimeoutSeconds, AKey, LBytes);
  if Result then
    AValue := RedisUtf8Decode(LBytes)
  else
    AValue := '';
end;

function TRedisListsCommands.BRPopBytes(const AKeys: array of TRedisArg;
  ATimeoutSeconds: Double; out AKey: string; out AValue: TBytes): Boolean;
var
  LReply: IRedisReply;
begin
  AKey := '';
  AValue := nil;
  if Length(AKeys) = 0 then
    raise ERedisException.Create('BRPOP sem chave');
  LReply := Executor.ExecuteBlocking('BRPOP',
    RedisArgs(AKeys, [ATimeoutSeconds]), ATimeoutSeconds);
  Result := not LReply.IsNull;
  if not Result then
    Exit;
  if (not LReply.IsAggregate) or (LReply.Count <> 2) then
    raise ERedisTypeError.Create('BRPOP: esperava par [chave, valor]');
  AKey := LReply[0].AsString;
  AValue := LReply[1].AsBytes;
end;

function TRedisListsCommands.BRPop(const AKeys: array of TRedisArg;
  ATimeoutSeconds: Double; out AKey, AValue: string): Boolean;
var
  LBytes: TBytes;
begin
  Result := BRPopBytes(AKeys, ATimeoutSeconds, AKey, LBytes);
  if Result then
    AValue := RedisUtf8Decode(LBytes)
  else
    AValue := '';
end;

function TRedisListsCommands.BRPopLPush(const ASource, ADestination: TRedisArg;
  ATimeoutSeconds: Double): IRedisReply;
begin
  Result := Executor.ExecuteBlocking('BRPOPLPUSH',
    [ASource, ADestination, ATimeoutSeconds], ATimeoutSeconds);
end;

function TRedisListsCommands.BLMove(const ASource, ADestination: TRedisArg;
  AFrom, ATo: TRedisListEnd; ATimeoutSeconds: Double): IRedisReply;
begin
  Result := Executor.ExecuteBlocking('BLMOVE',
    [ASource, ADestination, EndName(AFrom), EndName(ATo), ATimeoutSeconds],
    ATimeoutSeconds);
end;

end.
