unit Redis.Commands.Keys;

{ Comandos que valem para QUALQUER tipo de chave: existencia, expiracao,
  renomeacao e varredura.

  Tres nomes fogem do nome do comando Redis de proposito, porque colidiriam com
  rotinas do System dentro da propria implementation e o compilador escolheria
  o metodo em silencio:

    CopyKey  -> COPY   (System.Copy)
    MoveKey  -> MOVE   (System.Move)
    KeyType  -> TYPE   ('type' e' palavra reservada)

  O resto mantem o nome do comando. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

const
  /// TTL de uma chave que NAO existe. O Redis usa o mesmo comando para
  /// "sem prazo" e "sem chave", separando os dois so' por este valor.
  REDIS_TTL_NO_KEY = -2;

  /// TTL de uma chave que existe e nao tem prazo de validade.
  REDIS_TTL_NO_EXPIRY = -1;

type
  /// Comandos genericos de chave (DEL, EXPIRE, TTL, SCAN...).
  TRedisKeysCommands = class(TRedisCommandFamily)
  public
    /// DEL de uma chave. True se ela existia.
    function Del(const AKey: TRedisArg): Boolean;
    /// DEL de varias. Devolve quantas foram apagadas.
    function DelMany(const AKeys: array of TRedisArg): Int64;

    /// UNLINK: mesma coisa que DEL, mas a liberacao da memoria acontece numa
    /// thread do servidor. Vale a pena em chave grande (hash de milhoes de
    /// campos), onde o DEL sincrono trava o servidor inteiro por milissegundos.
    function Unlink(const AKeys: array of TRedisArg): Int64;

    function Exists(const AKey: TRedisArg): Boolean;
    /// EXISTS com varias chaves conta REPETICOES: EXISTS k k devolve 2 se k
    /// existe. E' assim no servidor, e nao seria honesto esconder.
    function ExistsMany(const AKeys: array of TRedisArg): Int64;

    /// TOUCH: atualiza o horario de ultimo acesso (afeta a politica LRU) sem
    /// ler o valor. Devolve quantas chaves existiam.
    function Touch(const AKeys: array of TRedisArg): Int64;

    /// EXPIRE. False quando a chave nao existe.
    function Expire(const AKey: TRedisArg; ASeconds: Int64): Boolean;
    function PExpire(const AKey: TRedisArg; AMilliseconds: Int64): Boolean;
    /// EXPIREAT com horario absoluto em segundos desde a epoca Unix.
    function ExpireAt(const AKey: TRedisArg; AUnixSeconds: Int64): Boolean;
    function PExpireAt(const AKey: TRedisArg; AUnixMilliseconds: Int64): Boolean;
    /// PERSIST: remove o prazo. False se a chave nao existia ou ja' era
    /// perpetua.
    function Persist(const AKey: TRedisArg): Boolean;

    /// TTL em segundos. REDIS_TTL_NO_KEY (-2) quando a chave nao existe e
    /// REDIS_TTL_NO_EXPIRY (-1) quando existe sem prazo — teste contra as
    /// constantes, nunca contra "menor que zero", que junta os dois casos.
    function Ttl(const AKey: TRedisArg): Int64;
    function PTtl(const AKey: TRedisArg): Int64;

    /// TYPE: 'string', 'list', 'set', 'zset', 'hash', 'stream' — ou 'none'
    /// quando a chave nao existe.
    function KeyType(const AKey: TRedisArg): string;

    /// RENAME. Levanta ERedisReplyError se a chave de origem nao existir.
    procedure Rename(const AKey, ANewKey: TRedisArg);
    /// RENAMENX: so' renomeia se o destino ainda nao existir.
    function RenameNx(const AKey, ANewKey: TRedisArg): Boolean;

    /// COPY. AReplace sobrescreve o destino existente.
    function CopyKey(const ASource, ADestination: TRedisArg;
      AReplace: Boolean = False): Boolean;
    /// MOVE: passa a chave para outro banco da MESMA conexao. False se o
    /// destino ja' tem a chave.
    function MoveKey(const AKey: TRedisArg; ADatabase: Integer): Boolean;

    /// RANDOMKEY. String vazia quando o banco esta' vazio.
    function RandomKey: string;

    /// KEYS: varre o banco INTEIRO de uma vez, bloqueando o servidor enquanto
    /// isso. Aceitavel num banco de desenvolvimento; em producao use Scan.
    function Keys(const APattern: string): TRedisStringArray;

    /// Um passo do SCAN. O laco correto e':
    ///
    ///   LCursor := 0;
    ///   repeat
    ///     LLote := LClient.Keys.Scan(LCursor, 'user:*', 100);
    ///     ...
    ///   until LCursor = 0;
    ///
    /// ACount e' uma DICA de quanto trabalho fazer por passo, nao um limite de
    /// resultados: um passo pode devolver mais, menos ou nenhum item e ainda
    /// assim ter cursor diferente de zero. E o SCAN pode repetir uma chave
    /// entre passos — quem precisa de lista sem repeticao deduplica do lado de
    /// ca'. AType filtra por tipo ('string', 'hash'...) e exige Redis 6+.
    function Scan(var ACursor: Int64; const AMatch: string = '';
      ACount: Integer = 0; const AType: string = ''): TRedisStringArray;
  end;

implementation

{ TRedisKeysCommands }

function TRedisKeysCommands.Del(const AKey: TRedisArg): Boolean;
begin
  Result := CmdInt('DEL', [AKey]) > 0;
end;

function TRedisKeysCommands.DelMany(const AKeys: array of TRedisArg): Int64;
begin
  if Length(AKeys) = 0 then
    Exit(0);
  Result := CmdInt('DEL', AKeys);
end;

function TRedisKeysCommands.Unlink(const AKeys: array of TRedisArg): Int64;
begin
  if Length(AKeys) = 0 then
    Exit(0);
  Result := CmdInt('UNLINK', AKeys);
end;

function TRedisKeysCommands.Exists(const AKey: TRedisArg): Boolean;
begin
  Result := CmdInt('EXISTS', [AKey]) > 0;
end;

function TRedisKeysCommands.ExistsMany(const AKeys: array of TRedisArg): Int64;
begin
  if Length(AKeys) = 0 then
    Exit(0);
  Result := CmdInt('EXISTS', AKeys);
end;

function TRedisKeysCommands.Touch(const AKeys: array of TRedisArg): Int64;
begin
  if Length(AKeys) = 0 then
    Exit(0);
  Result := CmdInt('TOUCH', AKeys);
end;

function TRedisKeysCommands.Expire(const AKey: TRedisArg; ASeconds: Int64): Boolean;
begin
  Result := CmdBool('EXPIRE', [AKey, ASeconds]);
end;

function TRedisKeysCommands.PExpire(const AKey: TRedisArg;
  AMilliseconds: Int64): Boolean;
begin
  Result := CmdBool('PEXPIRE', [AKey, AMilliseconds]);
end;

function TRedisKeysCommands.ExpireAt(const AKey: TRedisArg;
  AUnixSeconds: Int64): Boolean;
begin
  Result := CmdBool('EXPIREAT', [AKey, AUnixSeconds]);
end;

function TRedisKeysCommands.PExpireAt(const AKey: TRedisArg;
  AUnixMilliseconds: Int64): Boolean;
begin
  Result := CmdBool('PEXPIREAT', [AKey, AUnixMilliseconds]);
end;

function TRedisKeysCommands.Persist(const AKey: TRedisArg): Boolean;
begin
  Result := CmdBool('PERSIST', [AKey]);
end;

function TRedisKeysCommands.Ttl(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('TTL', [AKey]);
end;

function TRedisKeysCommands.PTtl(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('PTTL', [AKey]);
end;

function TRedisKeysCommands.KeyType(const AKey: TRedisArg): string;
begin
  Result := CmdString('TYPE', [AKey]);
end;

procedure TRedisKeysCommands.Rename(const AKey, ANewKey: TRedisArg);
begin
  CmdVoid('RENAME', [AKey, ANewKey]);
end;

function TRedisKeysCommands.RenameNx(const AKey, ANewKey: TRedisArg): Boolean;
begin
  Result := CmdBool('RENAMENX', [AKey, ANewKey]);
end;

function TRedisKeysCommands.CopyKey(const ASource, ADestination: TRedisArg;
  AReplace: Boolean): Boolean;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([ASource, ADestination], []);
  if AReplace then
    RedisAddArg(LArgs, 'REPLACE');
  Result := CmdBool('COPY', LArgs);
end;

function TRedisKeysCommands.MoveKey(const AKey: TRedisArg;
  ADatabase: Integer): Boolean;
begin
  Result := CmdBool('MOVE', [AKey, ADatabase]);
end;

function TRedisKeysCommands.RandomKey: string;
begin
  Result := CmdString('RANDOMKEY', []);
end;

function TRedisKeysCommands.Keys(const APattern: string): TRedisStringArray;
begin
  Result := CmdStrings('KEYS', [APattern]);
end;

function TRedisKeysCommands.Scan(var ACursor: Int64; const AMatch: string;
  ACount: Integer; const AType: string): TRedisStringArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([ACursor], []);
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
  if AType <> '' then
  begin
    RedisAddArg(LArgs, 'TYPE');
    RedisAddArg(LArgs, AType);
  end;
  Result := ExecScan('SCAN', LArgs, ACursor);
end;

end.
