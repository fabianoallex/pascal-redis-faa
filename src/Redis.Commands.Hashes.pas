unit Redis.Commands.Hashes;

{ Comandos de hash: um dicionario campo -> valor dentro de UMA chave.

  E' o tipo certo para objeto ("session:42" com campos user, ip, expires): da'
  para ler e escrever um campo sem trazer o objeto inteiro, e o Redis guarda
  hashes pequenos numa representacao compacta que gasta bem menos memoria que
  as chaves separadas equivalentes.

  Duas ausencias de proposito: HRANDFIELD e a expiracao por campo (HEXPIRE, do
  Redis 7.4) ficaram de fora do M4 — o Execute generico alcanca as duas. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

type
  /// Comandos de hash (HSET, HGET, HGETALL...).
  TRedisHashesCommands = class(TRedisCommandFamily)
  public
    /// HSET de um campo. Devolve 1 se o campo foi CRIADO e 0 se foi
    /// sobrescrito — em nenhum dos casos e' erro.
    function HSet(const AKey, AField, AValue: TRedisArg): Int64;
    /// HSET de varios campos: campo, valor, campo, valor... Devolve quantos
    /// campos foram criados.
    function HSetMany(const AKey: TRedisArg;
      const AFieldValues: array of TRedisArg): Int64;
    /// HSETNX: so' grava se o campo ainda nao existir.
    function HSetNx(const AKey, AField, AValue: TRedisArg): Boolean;

    /// HGET cru: IsNull separa "campo ausente" de "campo que vale ''".
    function HGet(const AKey, AField: TRedisArg): IRedisReply;
    function HGetString(const AKey, AField: TRedisArg): string;
    function HGetBytes(const AKey, AField: TRedisArg): TBytes;
    function HTryGet(const AKey, AField: TRedisArg; out AValue: string): Boolean;

    /// HGETALL. A resposta vem ACHATADA — campo em 0, valor em 1, campo em 2,
    /// e assim por diante — tanto em RESP2 (array) quanto em RESP3 (mapa),
    /// porque o leitor guarda mapa achatado justamente para isso. Entao:
    ///
    ///   LRep := LClient.Hashes.HGetAll('session:42');
    ///   LIp  := LRep.ValueByKey('ip').AsString;      // acesso por campo
    ///   I := 0;
    ///   while I < LRep.Count do                      // ou varredura
    ///   begin
    ///     ...LRep[I].AsString... LRep[I + 1].AsString...
    ///     Inc(I, 2);
    ///   end;
    ///
    /// Hash inexistente devolve agregado VAZIO, nao nulo — a mesma forma de um
    /// hash que existe e esta' vazio, porque no Redis hash vazio nao existe.
    function HGetAll(const AKey: TRedisArg): IRedisReply;

    /// HMGET. Cru de proposito: campo ausente vira item NULO no meio da lista.
    function HMGet(const AKey: TRedisArg;
      const AFields: array of TRedisArg): IRedisReply;

    /// HDEL de um campo. True se o campo existia.
    function HDel(const AKey, AField: TRedisArg): Boolean;
    /// HDEL de varios. Devolve quantos foram apagados.
    function HDelMany(const AKey: TRedisArg;
      const AFields: array of TRedisArg): Int64;

    function HExists(const AKey, AField: TRedisArg): Boolean;
    function HLen(const AKey: TRedisArg): Int64;
    /// HSTRLEN: tamanho do valor de um campo, em bytes. 0 se o campo nao
    /// existe.
    function HStrLen(const AKey, AField: TRedisArg): Int64;

    /// HKEYS / HVALS. Ordem NAO garantida.
    function HKeys(const AKey: TRedisArg): TRedisStringArray;
    function HVals(const AKey: TRedisArg): TRedisStringArray;

    function HIncrBy(const AKey, AField: TRedisArg; ADelta: Int64): Int64;
    function HIncrByFloat(const AKey, AField: TRedisArg; ADelta: Double): Double;

    /// Um passo do HSCAN, com a mesma mecanica de cursor do SCAN (ver
    /// TRedisKeysCommands.Scan). Os elementos vem ACHATADOS: campo, valor,
    /// campo, valor...
    function HScan(const AKey: TRedisArg; var ACursor: Int64;
      const AMatch: string = ''; ACount: Integer = 0): TRedisStringArray;
  end;

implementation

{ TRedisHashesCommands }

function TRedisHashesCommands.HSet(const AKey, AField, AValue: TRedisArg): Int64;
begin
  Result := CmdInt('HSET', [AKey, AField, AValue]);
end;

function TRedisHashesCommands.HSetMany(const AKey: TRedisArg;
  const AFieldValues: array of TRedisArg): Int64;
begin
  if Length(AFieldValues) = 0 then
    raise ERedisException.Create('HSET sem par campo/valor');
  if Odd(Length(AFieldValues)) then
    raise ERedisException.Create('HSET espera campo, valor, campo, valor...');
  Result := CmdInt('HSET', RedisArgs([AKey], AFieldValues));
end;

function TRedisHashesCommands.HSetNx(const AKey, AField,
  AValue: TRedisArg): Boolean;
begin
  Result := CmdBool('HSETNX', [AKey, AField, AValue]);
end;

function TRedisHashesCommands.HGet(const AKey, AField: TRedisArg): IRedisReply;
begin
  Result := Cmd('HGET', [AKey, AField]);
end;

function TRedisHashesCommands.HGetString(const AKey, AField: TRedisArg): string;
begin
  Result := Cmd('HGET', [AKey, AField]).AsString;
end;

function TRedisHashesCommands.HGetBytes(const AKey, AField: TRedisArg): TBytes;
begin
  Result := Cmd('HGET', [AKey, AField]).AsBytes;
end;

function TRedisHashesCommands.HTryGet(const AKey, AField: TRedisArg;
  out AValue: string): Boolean;
var
  LReply: IRedisReply;
begin
  LReply := Cmd('HGET', [AKey, AField]);
  Result := not LReply.IsNull;
  if Result then
    AValue := LReply.AsString
  else
    AValue := '';
end;

function TRedisHashesCommands.HGetAll(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('HGETALL', [AKey]);
end;

function TRedisHashesCommands.HMGet(const AKey: TRedisArg;
  const AFields: array of TRedisArg): IRedisReply;
begin
  if Length(AFields) = 0 then
    raise ERedisException.Create('HMGET sem campo');
  Result := Cmd('HMGET', RedisArgs([AKey], AFields));
end;

function TRedisHashesCommands.HDel(const AKey, AField: TRedisArg): Boolean;
begin
  Result := CmdInt('HDEL', [AKey, AField]) > 0;
end;

function TRedisHashesCommands.HDelMany(const AKey: TRedisArg;
  const AFields: array of TRedisArg): Int64;
begin
  if Length(AFields) = 0 then
    Exit(0);
  Result := CmdInt('HDEL', RedisArgs([AKey], AFields));
end;

function TRedisHashesCommands.HExists(const AKey, AField: TRedisArg): Boolean;
begin
  Result := CmdBool('HEXISTS', [AKey, AField]);
end;

function TRedisHashesCommands.HLen(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('HLEN', [AKey]);
end;

function TRedisHashesCommands.HStrLen(const AKey, AField: TRedisArg): Int64;
begin
  Result := CmdInt('HSTRLEN', [AKey, AField]);
end;

function TRedisHashesCommands.HKeys(const AKey: TRedisArg): TRedisStringArray;
begin
  Result := CmdStrings('HKEYS', [AKey]);
end;

function TRedisHashesCommands.HVals(const AKey: TRedisArg): TRedisStringArray;
begin
  Result := CmdStrings('HVALS', [AKey]);
end;

function TRedisHashesCommands.HIncrBy(const AKey, AField: TRedisArg;
  ADelta: Int64): Int64;
begin
  Result := CmdInt('HINCRBY', [AKey, AField, ADelta]);
end;

function TRedisHashesCommands.HIncrByFloat(const AKey, AField: TRedisArg;
  ADelta: Double): Double;
begin
  Result := CmdDouble('HINCRBYFLOAT', [AKey, AField, ADelta]);
end;

function TRedisHashesCommands.HScan(const AKey: TRedisArg; var ACursor: Int64;
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
  Result := ExecScan('HSCAN', LArgs, ACursor);
end;

end.
