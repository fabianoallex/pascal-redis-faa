unit Redis.Lock;

{ TRedisLock: o lock distribuido de instancia unica (um Redis so', NAO e'
  Redlock), com as tres pecas que toda aplicacao copiava do README e do
  sample LockDistribuidoVcl:

  1. **Adquirir:** SET chave token NX PX ttl. NX garante "so' um chega
     primeiro"; PX garante que um dono que morreu nao prende a chave para
     sempre. O token e' aleatorio (32 hex de um GUID), um por aquisicao.
  2. **Liberar:** um script Lua que apaga a chave SO' enquanto ela ainda guarda
     o token de quem chama. GET seguido de DEL seria a corrida classica: entre
     os dois o lock pode expirar e ser tomado por outro, e o DEL apagaria o
     lock DELE.
  3. **Indisponivel nao e' ocupado.** So' tres excecoes dizem "o Redis nao
     pode responder agora": ERedisConnectionLost, ERedisTimeout e
     ERedisPoolExhausted. Acquire as devolve como rlUnavailable (e avisa o
     OnUnavailable), para a aplicacao decidir se segue sem o lock -- por
     exemplo, quando o banco e' a palavra final e o lock so' poupa trabalho.
     Qualquer outro erro (WRONGTYPE, um script recusado) e' bug e sobe.

  Uso:

      LLock := TRedisLock.Create(LClient, 'app:lock:');
      case LLock.Acquire('pedido:7', 10000, LToken) of
        rlAcquired:    try ... finally LLock.Release('pedido:7', LToken); end;
        rlBusy:        // outro tem a posse: responder "ocupado"
        rlUnavailable: // o Redis nao respondeu: seguir sem lock, ou recusar
      end;

  TryAcquire e' o atalho booleano (True so' com a posse); nele, um Redis
  indisponivel levanta a propria excecao. Release devolve se apagou; um Redis
  indisponivel na liberacao so' deixa a chave viver ate' o PX vencer, entao
  vira False + OnUnavailable, nunca excecao. Extend estende o prazo com a
  mesma regra do token (PEXPIRE so' se ainda for seu).

  O token nunca vai para o OnUnavailable nem para mensagem de erro: quem tem
  o token libera o lock de outro. A chave vai (com o prefixo).

  Veio da pascal-agenda-faa (Agenda.Locks, a trava do horario no agendamento,
  2026-10-20), promovida em 2026-10-21. Racional na secao 52 de
  docs/DECISOES.md. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Client;

type
  TRedisLockResult = (
    rlAcquired,   // a posse e' de quem chamou, ate' o PX vencer
    rlBusy,       // outro tem a posse
    rlUnavailable // o Redis nao respondeu (conexao, timeout, pool esgotado)
  );

  /// AOperation: 'acquire', 'release' ou 'extend'; AKey ja' com o prefixo.
  TRedisLockUnavailableEvent = procedure(ASender: TObject;
    const AOperation, AKey: string; AError: Exception) of object;

  TRedisLock = class
  private
    FClient: TRedisClient;
    FPrefix: string;
    FOnUnavailable: TRedisLockUnavailableEvent;
    procedure Unavailable(const AOperation, AKey: string; AError: Exception);
    function OwnerScript(const AScript, AKey, AToken: string;
      const AExtra: string): Int64;
  public
    /// AClient nao e' do lock (um cliente por processo). APrefix vai antes de
    /// toda chave ('app:lock:').
    constructor Create(AClient: TRedisClient; const APrefix: string = '');
    /// Classifica o desfecho; so' levanta para erro que nao seja de
    /// disponibilidade. AToken: o token da posse, '' fora de rlAcquired.
    function Acquire(const AKey: string; ATtlMs: Integer; out AToken: string): TRedisLockResult;
    /// True com a posse; False quando outro a tem. Redis indisponivel:
    /// levanta (ERedisConnectionLost, ERedisTimeout, ERedisPoolExhausted).
    function TryAcquire(const AKey: string; ATtlMs: Integer; out AToken: string): Boolean;
    /// Apaga AKey se ela ainda guarda AToken. False: ja' nao era sua (venceu,
    /// outro tomou), AToken '' ou o Redis nao respondeu (OnUnavailable).
    function Release(const AKey, AToken: string): Boolean;
    /// Novo prazo de ATtlMs se AKey ainda guarda AToken. Mesmos False de
    /// Release.
    function Extend(const AKey, AToken: string; ATtlMs: Integer): Boolean;
    /// Um token novo (32 hex), o que Acquire usa.
    class function NewToken: string; static;
    property Prefix: string read FPrefix;
    /// Chamado (na thread de quem chamou) quando o Redis nao respondeu.
    property OnUnavailable: TRedisLockUnavailableEvent read FOnUnavailable write FOnUnavailable;
  end;

const
  /// KEYS[1] a chave, ARGV[1] o token: 1 quando apagou, 0 quando nao era sua.
  REDIS_LOCK_RELEASE_SCRIPT =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then ' +
    'return redis.call("DEL", KEYS[1]) else return 0 end';
  /// KEYS[1] a chave, ARGV[1] o token, ARGV[2] o prazo em ms.
  REDIS_LOCK_EXTEND_SCRIPT =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then ' +
    'return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end';

/// ERedisConnectionLost, ERedisTimeout ou ERedisPoolExhausted: o Redis nao
/// pode responder agora. Qualquer outra excecao, False.
function RedisIsUnavailable(E: Exception): Boolean;

implementation

uses
  Redis.Pool,
  Redis.Commands.Strings,
  Redis.Commands.Scripting;

function RedisIsUnavailable(E: Exception): Boolean;
begin
  Result := (E is ERedisConnectionLost) or (E is ERedisTimeout) or
    (E is ERedisPoolExhausted);
end;

{ TRedisLock }

constructor TRedisLock.Create(AClient: TRedisClient; const APrefix: string);
begin
  inherited Create;
  if AClient = nil then
    raise EArgumentException.Create('TRedisLock: AClient e'' nil');
  FClient := AClient;
  FPrefix := APrefix;
end;

class function TRedisLock.NewToken: string;
var
  LGuid: TGUID;
  LText: string;
  I: Integer;
begin
  if CreateGUID(LGuid) <> 0 then
    raise ERedisException.Create('TRedisLock: CreateGUID falhou');
  LText := GUIDToString(LGuid); // {XXXXXXXX-XXXX-...}
  Result := '';
  for I := 1 to Length(LText) do
    if CharInSet(LText[I], ['0'..'9', 'A'..'F']) then
      Result := Result + LText[I];
  Result := LowerCase(Result);
end;

procedure TRedisLock.Unavailable(const AOperation, AKey: string; AError: Exception);
begin
  if Assigned(FOnUnavailable) then
    FOnUnavailable(Self, AOperation, AKey, AError);
end;

function TRedisLock.Acquire(const AKey: string; ATtlMs: Integer;
  out AToken: string): TRedisLockResult;
var
  LToken: string;
begin
  AToken := '';
  try
    if TryAcquire(AKey, ATtlMs, LToken) then
    begin
      AToken := LToken;
      Result := rlAcquired;
    end
    else
      Result := rlBusy;
  except
    on E: Exception do
    begin
      if not RedisIsUnavailable(E) then
        raise;
      Unavailable('acquire', FPrefix + AKey, E);
      Result := rlUnavailable;
    end;
  end;
end;

function TRedisLock.TryAcquire(const AKey: string; ATtlMs: Integer;
  out AToken: string): Boolean;
var
  LOptions: TRedisSetOptions;
  LToken: string;
begin
  if ATtlMs <= 0 then
    raise EArgumentException.Create('TRedisLock: o prazo (ATtlMs) tem de ser positivo');
  AToken := '';
  LToken := NewToken;
  LOptions := RedisDefaultSetOptions;
  LOptions.Condition := scNotExists;
  LOptions.Expiry := seMilliseconds;
  LOptions.ExpiryValue := ATtlMs;
  // Nulo: a chave ja' existia e nada foi gravado.
  Result := not FClient.Strings.SetWithOptions(FPrefix + AKey, LToken, LOptions).IsNull;
  if Result then
    AToken := LToken;
end;

// Um dos scripts de dono (RELEASE, EXTEND); AExtra vai em ARGV[2] quando
// nao vazio.
function TRedisLock.OwnerScript(const AScript, AKey, AToken: string;
  const AExtra: string): Int64;
begin
  if AExtra = '' then
    Result := FClient.Scripting.Run(AScript, [FPrefix + AKey], [AToken]).AsInteger
  else
    Result := FClient.Scripting.Run(AScript, [FPrefix + AKey], [AToken, AExtra]).AsInteger;
end;

function TRedisLock.Release(const AKey, AToken: string): Boolean;
begin
  Result := False;
  if AToken = '' then
    Exit;
  try
    Result := OwnerScript(REDIS_LOCK_RELEASE_SCRIPT, AKey, AToken, '') = 1;
  except
    on E: Exception do
    begin
      // A chave vence sozinha: uma liberacao que falhou so' a mantem presa
      // ate' o PX.
      if not RedisIsUnavailable(E) then
        raise;
      Unavailable('release', FPrefix + AKey, E);
    end;
  end;
end;

function TRedisLock.Extend(const AKey, AToken: string; ATtlMs: Integer): Boolean;
begin
  Result := False;
  if ATtlMs <= 0 then
    raise EArgumentException.Create('TRedisLock: o prazo (ATtlMs) tem de ser positivo');
  if AToken = '' then
    Exit;
  try
    Result := OwnerScript(REDIS_LOCK_EXTEND_SCRIPT, AKey, AToken, IntToStr(ATtlMs)) = 1;
  except
    on E: Exception do
    begin
      if not RedisIsUnavailable(E) then
        raise;
      Unavailable('extend', FPrefix + AKey, E);
    end;
  end;
end;

end.
