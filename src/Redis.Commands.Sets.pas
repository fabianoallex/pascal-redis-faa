unit Redis.Commands.Sets;

{ Comandos de conjunto: colecao sem ordem e sem repeticao, com teste de
  pertinencia em tempo constante.

  E' o tipo de "quem ja' viu este id", "tags deste post", "usuarios online" — e
  as operacoes de conjunto (SINTER, SUNION, SDIFF) resolvem no servidor o que
  do lado do cliente exigiria trazer as duas colecoes inteiras pela rede.

  Cuidado com SMEMBERS em conjunto grande: ele traz TUDO numa resposta so' e
  bloqueia o servidor enquanto monta. Acima de alguns milhares de elementos, o
  certo e' SScan. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

type
  /// Comandos de conjunto (SADD, SMEMBERS, SINTER...).
  TRedisSetsCommands = class(TRedisCommandFamily)
  public
    /// SADD de um membro. True se ele foi ACRESCENTADO — False significa que
    /// ja' estava la', o que nao e' erro.
    function SAdd(const AKey, AMember: TRedisArg): Boolean;
    /// SADD de varios. Devolve quantos entraram de fato.
    function SAddMany(const AKey: TRedisArg;
      const AMembers: array of TRedisArg): Int64;

    /// SREM de um membro. True se ele estava la'.
    function SRem(const AKey, AMember: TRedisArg): Boolean;
    function SRemMany(const AKey: TRedisArg;
      const AMembers: array of TRedisArg): Int64;

    function SIsMember(const AKey, AMember: TRedisArg): Boolean;
    /// SMISMEMBER: testa varios de uma vez, na ordem em que foram passados
    /// (Redis 6.2+). Uma ida e volta em vez de N.
    function SMIsMember(const AKey: TRedisArg;
      const AMembers: array of TRedisArg): TRedisBooleanArray;

    function SCard(const AKey: TRedisArg): Int64;

    /// SMEMBERS. Ordem NAO garantida — conjunto nao tem ordem, e a que o
    /// servidor devolve depende da representacao interna.
    function SMembers(const AKey: TRedisArg): TRedisStringArray;
    function SMembersBytes(const AKey: TRedisArg): TRedisBytesArray;

    /// SPOP: tira um membro ao acaso. NULO se o conjunto estiver vazio.
    /// Diferente de SRandMember, o membro sai do conjunto.
    function SPop(const AKey: TRedisArg): IRedisReply;
    function SPopCount(const AKey: TRedisArg; ACount: Integer): TRedisStringArray;

    /// SRANDMEMBER: sorteia sem remover. NULO se o conjunto estiver vazio.
    function SRandMember(const AKey: TRedisArg): IRedisReply;
    /// SRANDMEMBER com contagem. Positivo devolve membros DISTINTOS (no
    /// maximo o conjunto inteiro); NEGATIVO devolve exatamente |ACount|
    /// membros, podendo repetir.
    function SRandMemberCount(const AKey: TRedisArg;
      ACount: Integer): TRedisStringArray;

    /// SMOVE: move um membro entre conjuntos, atomico. False se ele nao estava
    /// na origem.
    function SMove(const ASource, ADestination, AMember: TRedisArg): Boolean;

    /// Operacoes de conjunto. A intersecao custa O(N*M) no pior caso — em
    /// conjuntos grandes, prefira as versoes ...Store, que nao trazem o
    /// resultado pela rede.
    function SInter(const AKeys: array of TRedisArg): TRedisStringArray;
    function SUnion(const AKeys: array of TRedisArg): TRedisStringArray;
    /// SDIFF: o que esta' no PRIMEIRO conjunto e em nenhum dos outros. A ordem
    /// dos argumentos importa.
    function SDiff(const AKeys: array of TRedisArg): TRedisStringArray;

    /// Versoes que gravam o resultado numa chave e devolvem a cardinalidade.
    /// O destino e' SOBRESCRITO (e apagado, se o resultado for vazio).
    function SInterStore(const ADestination: TRedisArg;
      const AKeys: array of TRedisArg): Int64;
    function SUnionStore(const ADestination: TRedisArg;
      const AKeys: array of TRedisArg): Int64;
    function SDiffStore(const ADestination: TRedisArg;
      const AKeys: array of TRedisArg): Int64;

    /// SINTERCARD: quantos elementos a intersecao teria, sem materializa-la
    /// (Redis 7+). ALimit > 0 faz o servidor parar de contar ao chegar la'.
    function SInterCard(const AKeys: array of TRedisArg;
      ALimit: Int64 = 0): Int64;

    /// Um passo do SSCAN, com a mesma mecanica de cursor do SCAN (ver
    /// TRedisKeysCommands.Scan).
    function SScan(const AKey: TRedisArg; var ACursor: Int64;
      const AMatch: string = ''; ACount: Integer = 0): TRedisStringArray;
  end;

implementation

{ TRedisSetsCommands }

function TRedisSetsCommands.SAdd(const AKey, AMember: TRedisArg): Boolean;
begin
  Result := CmdInt('SADD', [AKey, AMember]) > 0;
end;

function TRedisSetsCommands.SAddMany(const AKey: TRedisArg;
  const AMembers: array of TRedisArg): Int64;
begin
  if Length(AMembers) = 0 then
    raise ERedisException.Create('SADD sem membro');
  Result := CmdInt('SADD', RedisArgs([AKey], AMembers));
end;

function TRedisSetsCommands.SRem(const AKey, AMember: TRedisArg): Boolean;
begin
  Result := CmdInt('SREM', [AKey, AMember]) > 0;
end;

function TRedisSetsCommands.SRemMany(const AKey: TRedisArg;
  const AMembers: array of TRedisArg): Int64;
begin
  if Length(AMembers) = 0 then
    Exit(0);
  Result := CmdInt('SREM', RedisArgs([AKey], AMembers));
end;

function TRedisSetsCommands.SIsMember(const AKey, AMember: TRedisArg): Boolean;
begin
  Result := CmdBool('SISMEMBER', [AKey, AMember]);
end;

function TRedisSetsCommands.SMIsMember(const AKey: TRedisArg;
  const AMembers: array of TRedisArg): TRedisBooleanArray;
begin
  if Length(AMembers) = 0 then
    raise ERedisException.Create('SMISMEMBER sem membro');
  Result := RedisReplyToBooleans(Cmd('SMISMEMBER', RedisArgs([AKey], AMembers)));
end;

function TRedisSetsCommands.SCard(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('SCARD', [AKey]);
end;

function TRedisSetsCommands.SMembers(const AKey: TRedisArg): TRedisStringArray;
begin
  Result := CmdStrings('SMEMBERS', [AKey]);
end;

function TRedisSetsCommands.SMembersBytes(const AKey: TRedisArg): TRedisBytesArray;
begin
  Result := RedisReplyToBytesArray(Cmd('SMEMBERS', [AKey]));
end;

function TRedisSetsCommands.SPop(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('SPOP', [AKey]);
end;

function TRedisSetsCommands.SPopCount(const AKey: TRedisArg;
  ACount: Integer): TRedisStringArray;
begin
  Result := CmdStrings('SPOP', [AKey, ACount]);
end;

function TRedisSetsCommands.SRandMember(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('SRANDMEMBER', [AKey]);
end;

function TRedisSetsCommands.SRandMemberCount(const AKey: TRedisArg;
  ACount: Integer): TRedisStringArray;
begin
  Result := CmdStrings('SRANDMEMBER', [AKey, ACount]);
end;

function TRedisSetsCommands.SMove(const ASource, ADestination,
  AMember: TRedisArg): Boolean;
begin
  Result := CmdBool('SMOVE', [ASource, ADestination, AMember]);
end;

function TRedisSetsCommands.SInter(const AKeys: array of TRedisArg): TRedisStringArray;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('SINTER sem chave');
  Result := CmdStrings('SINTER', AKeys);
end;

function TRedisSetsCommands.SUnion(const AKeys: array of TRedisArg): TRedisStringArray;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('SUNION sem chave');
  Result := CmdStrings('SUNION', AKeys);
end;

function TRedisSetsCommands.SDiff(const AKeys: array of TRedisArg): TRedisStringArray;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('SDIFF sem chave');
  Result := CmdStrings('SDIFF', AKeys);
end;

function TRedisSetsCommands.SInterStore(const ADestination: TRedisArg;
  const AKeys: array of TRedisArg): Int64;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('SINTERSTORE sem chave');
  Result := CmdInt('SINTERSTORE', RedisArgs([ADestination], AKeys));
end;

function TRedisSetsCommands.SUnionStore(const ADestination: TRedisArg;
  const AKeys: array of TRedisArg): Int64;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('SUNIONSTORE sem chave');
  Result := CmdInt('SUNIONSTORE', RedisArgs([ADestination], AKeys));
end;

function TRedisSetsCommands.SDiffStore(const ADestination: TRedisArg;
  const AKeys: array of TRedisArg): Int64;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('SDIFFSTORE sem chave');
  Result := CmdInt('SDIFFSTORE', RedisArgs([ADestination], AKeys));
end;

function TRedisSetsCommands.SInterCard(const AKeys: array of TRedisArg;
  ALimit: Int64): Int64;
var
  LArgs: TRedisArgs;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('SINTERCARD sem chave');
  // O numero de chaves vai explicito no comando: e' o que permite ao servidor
  // saber onde a lista de chaves termina e o LIMIT comeca.
  LArgs := RedisArgs([Length(AKeys)], AKeys);
  if ALimit > 0 then
  begin
    RedisAddArg(LArgs, 'LIMIT');
    RedisAddArg(LArgs, ALimit);
  end;
  Result := CmdInt('SINTERCARD', LArgs);
end;

function TRedisSetsCommands.SScan(const AKey: TRedisArg; var ACursor: Int64;
  const AMatch: string; ACount: Integer): TRedisStringArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, ACursor], []);
  if AMatch <> '' then
  begin
    RedisAddArg(LArgs, 'MATCH');
    RedisAddArg(LArgs, AMatch);
  end;
  if ACount > 0 then
  begin
    RedisAddArg(LArgs, 'COUNT');
    RedisAddArg(LArgs, ACount);
  end;
  Result := ExecScan('SSCAN', LArgs, ACursor);
end;

end.
