unit Redis.Cache;

{ TRedisCacheStore: o IPcCacheStore da pascal-common-faa (PascalCommon.Cache,
  1.29.0) sobre o Redis. E' a fase F2 do docs/cache-design.md de la'.

  O store so' guarda; as regras (chave, tags, single-flight, falhas, metricas)
  sao do TPcCache. Uso:

      LStore := TRedisCacheStore.Create(LRedisPool);
      FCache := TPcCache.Create(LStore, 'app', OnCacheError);

  O pool (ou o cliente) NAO e' do store: tem de viver mais que ele. Thread-safe:
  cada chamada empresta uma conexao do pool e a devolve.

  Valores (C2): string enviada como UTF-8 e lida como UTF-8, byte a byte, pelo
  mesmo RedisUtf8Encode/Decode de toda a lib. Put e' PSETEX (prazo em ms);
  TtlMs <= 0 apaga a chave e nao grava nada.

  Contadores de tag (C4), sem TTL:

  - Generations le todos num MGET, uma ida e volta. Contador ausente nasce com
    SET NX valendo o horario Unix em ms DESTE processo (RedisUnixTimeMs) e e'
    relido no mesmo pipeline (SET NX ... + MGET dos ausentes): quem perdeu a
    corrida do SET NX le o valor de quem ganhou. Nunca nasce em 0. So' a
    criacao custa a segunda ida e volta.
  - Bump e' um script Lua: INCR so' no contador que EXISTE. O INCR puro numa
    chave ausente criaria o contador em 1, e as entradas gravadas sob 1, 2,
    3... antes de um eviction voltariam a ser lidas. Contador ausente fica
    ausente (o contrato permite): o proximo Generations o cria pelo relogio,
    maior que qualquer valor que ele teve. Deixar ausente, e nao criar, tambem
    impede que invalidar tags que ninguem le (uma por linha alterada) encha o
    Redis de chaves sem TTL.

  Por que Lua so' no Bump (medido em 2026-11-04, Docker Desktop no Windows,
  ~0,5 ms por ida e volta): MGET, um script de leitura+criacao e um
  MULTI/SET NX/INCR/EXEC em pipeline custaram todos uma ida e volta, sem
  diferenca acima do ruido. Lua na leitura nao economiza nada no caminho
  quente; no Bump ele e' o unico jeito de ter um INCR condicional atomico.
  Redis com scripting bloqueado por ACL faz o Bump levantar ERedisReplyError
  (NOPERM), que e' configuracao e sobe.

  Relogio do contador novo: o do cliente, e nao o TIME do servidor (que
  custaria mais uma ida e volta na criacao). Instancias com relogios
  defasados de S ms enfraquecem a garantia do C4 em S ms; um contador so'
  renasce depois de eviction ou restart, quando T1 - T0 costuma ser de horas.

  Erros (C6): o que diz "o Redis nao pode responder agora" vira
  EPcCacheUnavailable, que o TPcCache trata como miss: ERedisConnectionLost,
  ERedisTimeout, ERedisPoolExhausted, ERedisPoolClosed, ERedisTransport (e a
  filha ERedisTransportTimeout, que a lib ja' nao levanta mas ainda declara)
  e ERedisTls. TLS entra porque um certificado vencido no servidor e' falha
  de operacao, nao de codigo, e o cache fora do ar nao pode derrubar a
  aplicacao. A mensagem leva a classe original. ERedisReplyError (WRONGTYPE,
  um contador que nao e' numero) e ERedisTypeError sao bug e sobem como estao.

  Racional na secao 54 de docs/DECISOES.md. }

{$I redis.inc}

interface

uses
  SysUtils,
  PascalCommon.Version,
  PascalCommon.Cache,
  Redis.Types,
  Redis.Pool,
  Redis.Client;

{$IF PASCALCOMMON_VERSION < 12900}
  {$MESSAGE FATAL 'Redis.Cache precisa da pascal-common-faa 1.29.0 ou mais nova (PascalCommon.Cache)'}
{$IFEND}

const
  /// KEYS: os contadores. INCR so' nos que existem; os ausentes ficam ausentes.
  REDIS_CACHE_BUMP_SCRIPT =
    'for _, k in ipairs(KEYS) do ' +
    'if redis.call("EXISTS", k) == 1 then redis.call("INCR", k) end ' +
    'end return 0';

type
  TRedisCacheStore = class(TInterfacedObject, IPcCacheStore)
  private
    FClient: TRedisClient;
    FOwnsClient: Boolean;
    function DoTryGet(const AKey: string; out AValue: string): Boolean;
    procedure DoPut(const AKey, AValue: string; ATtlMs: Int64);
    function DoGenerations(const ATags: array of string): TArray<Int64>;
    procedure DoBump(const ATags: array of string);
  public
    /// APool nao e' do store e tem de viver mais que ele.
    constructor Create(APool: TRedisPool); overload;
    /// AClient nao e' do store (um cliente por processo, como no TRedisLock).
    constructor Create(AClient: TRedisClient); overload;
    destructor Destroy; override;
    function TryGet(const AKey: string; out AValue: string): Boolean;
    procedure Put(const AKey, AValue: string; ATtlMs: Int64);
    procedure Delete(const AKey: string);
    function Generations(const ATags: array of string): TArray<Int64>;
    procedure Bump(const ATags: array of string);
    property Client: TRedisClient read FClient;
  end;

/// Horario Unix em milissegundos (UTC), o valor com que um contador nasce.
function RedisUnixTimeMs: Int64;

/// True para as excecoes que viram EPcCacheUnavailable (ver o topo da unit).
function RedisCacheIsUnavailable(E: Exception): Boolean;

implementation

uses
  {$IFDEF REDIS_WINDOWS}
  Windows,
  {$ELSE}
  {$IFDEF FPC}
  BaseUnix,
  Unix,
  {$ELSE}
  DateUtils,
  {$ENDIF}
  {$ENDIF}
  Redis.Transport,
  Redis.Connection,
  Redis.Commands.Keys,
  Redis.Commands.Strings,
  Redis.Commands.Scripting;

{$IFDEF REDIS_WINDOWS}
function RedisUnixTimeMs: Int64;
const
  // 1601-01-01 ate' 1970-01-01, em unidades de 100 ns.
  FILETIME_UNIX_EPOCH = Int64(116444736000000000);
var
  LFileTime: TFileTime;
  LTicks: Int64;
begin
  GetSystemTimeAsFileTime(LFileTime);
  LTicks := (Int64(LFileTime.dwHighDateTime) shl 32) or Int64(LFileTime.dwLowDateTime);
  Result := (LTicks - FILETIME_UNIX_EPOCH) div 10000;
end;
{$ELSE}
{$IFDEF FPC}
function RedisUnixTimeMs: Int64;
var
  LTime: TTimeVal;
begin
  fpgettimeofday(@LTime, nil);
  Result := Int64(LTime.tv_sec) * 1000 + LTime.tv_usec div 1000;
end;
{$ELSE}
function RedisUnixTimeMs: Int64;
begin
  Result := DateTimeToUnix(Now, False) * 1000;
end;
{$ENDIF}
{$ENDIF}

function RedisCacheIsUnavailable(E: Exception): Boolean;
begin
  Result := (E is ERedisConnectionLost) or (E is ERedisTimeout) or
    (E is ERedisPoolExhausted) or (E is ERedisPoolClosed) or
    (E is ERedisTransport) or (E is ERedisTls);
end;

// Chamar so' de dentro de um except: troca a falha de disponibilidade por
// EPcCacheUnavailable e deixa o resto subir intacto.
procedure Translate(E: Exception);
begin
  if RedisCacheIsUnavailable(E) then
    raise EPcCacheUnavailable.CreateFmt('Redis indisponivel (%s): %s',
      [E.ClassName, E.Message]);
end;

function ToArgs(const AValues: array of string): TRedisArgs;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, Length(AValues));
  for I := 0 to High(AValues) do
    Result[I] := AValues[I];
end;

{ TRedisCacheStore }

constructor TRedisCacheStore.Create(APool: TRedisPool);
begin
  inherited Create;
  if APool = nil then
    raise EArgumentException.Create('TRedisCacheStore: APool e'' nil');
  FClient := TRedisClient.CreateOnPool(APool, False);
  FOwnsClient := True;
end;

constructor TRedisCacheStore.Create(AClient: TRedisClient);
begin
  inherited Create;
  if AClient = nil then
    raise EArgumentException.Create('TRedisCacheStore: AClient e'' nil');
  FClient := AClient;
  FOwnsClient := False;
end;

destructor TRedisCacheStore.Destroy;
begin
  if FOwnsClient then
    FClient.Free;
  inherited Destroy;
end;

function TRedisCacheStore.DoTryGet(const AKey: string; out AValue: string): Boolean;
begin
  Result := FClient.Strings.TryGet(AKey, AValue);
end;

function TRedisCacheStore.TryGet(const AKey: string; out AValue: string): Boolean;
begin
  AValue := '';
  try
    Result := DoTryGet(AKey, AValue);
  except
    on E: Exception do
    begin
      Translate(E);
      raise;
    end;
  end;
end;

procedure TRedisCacheStore.DoPut(const AKey, AValue: string; ATtlMs: Int64);
begin
  if ATtlMs <= 0 then
    FClient.Keys.Del(AKey)
  else
    FClient.Strings.PSetEx(AKey, ATtlMs, AValue);
end;

procedure TRedisCacheStore.Put(const AKey, AValue: string; ATtlMs: Int64);
begin
  try
    DoPut(AKey, AValue, ATtlMs);
  except
    on E: Exception do
    begin
      Translate(E);
      raise;
    end;
  end;
end;

procedure TRedisCacheStore.Delete(const AKey: string);
begin
  try
    FClient.Keys.Del(AKey);
  except
    on E: Exception do
    begin
      Translate(E);
      raise;
    end;
  end;
end;

function TRedisCacheStore.DoGenerations(const ATags: array of string): TArray<Int64>;
var
  LReply: IRedisReply;
  LReplies: TRedisReplyArray;
  LMissing: array of Integer;
  LMissingKeys: TRedisArgs;
  LPipeline: TRedisPipeline;
  LNow: Int64;
  LCreated: Boolean;
  I, J: Integer;
begin
  Result := nil;
  SetLength(Result, Length(ATags));
  if Length(ATags) = 0 then
    Exit;

  LReply := FClient.Strings.MGet(ToArgs(ATags));
  LMissing := nil;
  for I := 0 to High(ATags) do
    if LReply[I].IsNull then
    begin
      SetLength(LMissing, Length(LMissing) + 1);
      LMissing[High(LMissing)] := I;
    end
    else
      Result[I] := LReply[I].AsInteger;
  if Length(LMissing) = 0 then
    Exit;

  // Criacao: SET NX de cada ausente com o relogio, e o MGET deles no MESMO
  // pipeline. Quem perdeu a corrida (outra instancia criou antes) le o valor
  // de quem ganhou; quem ganhou le o seu, ou um INCR que ja' veio depois.
  LNow := RedisUnixTimeMs;
  LMissingKeys := nil;
  SetLength(LMissingKeys, Length(LMissing));
  LPipeline := TRedisPipeline.Create;
  try
    for J := 0 to High(LMissing) do
    begin
      LMissingKeys[J] := ATags[LMissing[J]];
      LPipeline.Queue('SET', [ATags[LMissing[J]], LNow, 'NX']);
    end;
    LPipeline.Queue('MGET', LMissingKeys);
    LReplies := FClient.ExecutePipeline(LPipeline);
  finally
    LPipeline.Free;
  end;
  for J := 0 to High(LReplies) do
    LReplies[J].RaiseIfError;
  LReply := LReplies[High(LReplies)];
  for J := 0 to High(LMissing) do
  begin
    I := LMissing[J];
    LCreated := not LReplies[J].IsNull;
    if not LReply[J].IsNull then
      Result[I] := LReply[J].AsInteger
    else if LCreated then
      // Criado e sumido antes do MGET (eviction no meio): o valor era LNow.
      Result[I] := LNow
    else
      // O SET NX achou a chave e o MGET a le como nula: ela existe, mas nao
      // e' string (um hash, uma lista). Bug de quem reusou o nome.
      raise ERedisTypeError.CreateFmt(
        'o contador de tag "%s" existe mas nao e'' uma string', [ATags[I]]);
  end;
end;

function TRedisCacheStore.Generations(const ATags: array of string): TArray<Int64>;
begin
  try
    Result := DoGenerations(ATags);
  except
    on E: Exception do
    begin
      Translate(E);
      raise;
    end;
  end;
end;

procedure TRedisCacheStore.DoBump(const ATags: array of string);
begin
  if Length(ATags) = 0 then
    Exit;
  FClient.Scripting.Run(REDIS_CACHE_BUMP_SCRIPT, ToArgs(ATags), []);
end;

procedure TRedisCacheStore.Bump(const ATags: array of string);
begin
  try
    DoBump(ATags);
  except
    on E: Exception do
    begin
      Translate(E);
      raise;
    end;
  end;
end;

end.
