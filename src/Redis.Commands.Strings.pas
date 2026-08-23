unit Redis.Commands.Strings;

{ Comandos de string: o tipo mais simples do Redis e o mais usado — cache,
  contador, flag, lock.

  "String" aqui e' o nome do TIPO no Redis, nao "texto": o valor e' uma
  sequencia de bytes de ate' 512 MB, que pode ser um JPEG. Por isso as
  assinaturas trabalham com TRedisArg (aceita string e TBytes na mesma chamada)
  e todo getter tem um par ...Bytes.

  Os nomes seguem o comando, com uma excecao: SET vira SetValue, porque um
  metodo chamado 'Set' colidiria com a palavra reservada 'set' do Pascal. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

type
  /// Condicao de existencia do SET.
  ///   scAlways     — grava sempre (padrao)
  ///   scNotExists  — NX: so' se a chave NAO existe (e' o lock distribuido)
  ///   scExists     — XX: so' se a chave JA' existe
  TRedisSetCondition = (scAlways, scNotExists, scExists);

  /// Forma de expiracao do SET.
  ///   seNone              — sem prazo, e APAGA o prazo que houvesse
  ///   seSeconds           — EX
  ///   seMilliseconds      — PX
  ///   seUnixSeconds       — EXAT
  ///   seUnixMilliseconds  — PXAT
  ///   seKeepTtl           — KEEPTTL: preserva o prazo que a chave ja' tinha
  TRedisSetExpiry = (seNone, seSeconds, seMilliseconds, seUnixSeconds,
    seUnixMilliseconds, seKeepTtl);

  /// Modificadores do SET. Monte com RedisDefaultSetOptions e mexa so' no que
  /// interessa.
  ///
  /// Uma armadilha que vale registrar: um SET sem KEEPTTL **remove** o prazo
  /// de validade da chave. Reescrever um valor cacheado com SetValue simples
  /// transforma o cache com TTL num vazamento permanente.
  TRedisSetOptions = record
    Condition: TRedisSetCondition;
    Expiry: TRedisSetExpiry;
    /// Segundos, milissegundos ou horario absoluto, conforme Expiry.
    /// Ignorado em seNone e seKeepTtl.
    ExpiryValue: Int64;
    /// GET: faz o SET devolver o valor ANTERIOR da chave em vez de +OK.
    /// Exige Redis 6.2+.
    ReturnOldValue: Boolean;
  end;

  /// Comandos de string (GET, SET, INCR, MGET...).
  TRedisStringsCommands = class(TRedisCommandFamily)
  public
    /// SET simples. Levanta em erro de servidor.
    ///
    /// ATENCAO: apaga o TTL da chave, se houver. Para preservar, use
    /// SetWithOptions com Expiry = seKeepTtl.
    procedure SetValue(const AKey, AValue: TRedisArg);

    /// SET com modificadores. Devolve a resposta crua porque ela muda de forma
    /// conforme as opcoes:
    ///   - sem GET: 'OK' (AsBoolean = True) ou NULO quando o NX/XX barrou;
    ///   - com GET: o valor anterior, ou NULO se a chave nao existia.
    /// Nos dois casos IsNull distingue "nao gravou" de "gravou".
    function SetWithOptions(const AKey, AValue: TRedisArg;
      const AOptions: TRedisSetOptions): IRedisReply;

    /// SET NX. True se gravou (a chave nao existia).
    function SetNx(const AKey, AValue: TRedisArg): Boolean;
    /// SET com prazo em segundos (SETEX).
    procedure SetEx(const AKey: TRedisArg; ASeconds: Int64; const AValue: TRedisArg);
    /// SET com prazo em milissegundos (PSETEX).
    procedure PSetEx(const AKey: TRedisArg; AMilliseconds: Int64;
      const AValue: TRedisArg);

    /// GET cru: use IsNull para separar "chave ausente" de "chave que vale ''".
    function Get(const AKey: TRedisArg): IRedisReply;
    /// GET como texto. Chave ausente devolve '' — se a diferenca importa, use
    /// TryGet ou Get.
    function GetString(const AKey: TRedisArg): string;
    /// GET sem interpretar codepage. Chave ausente devolve nil.
    function GetBytes(const AKey: TRedisArg): TBytes;
    /// GET com presenca explicita. False (e AValue = '') quando a chave nao
    /// existe.
    function TryGet(const AKey: TRedisArg; out AValue: string): Boolean;
    function TryGetBytes(const AKey: TRedisArg; out AValue: TBytes): Boolean;

    /// GETDEL: le e apaga numa operacao atomica (Redis 6.2+).
    function GetDel(const AKey: TRedisArg): IRedisReply;
    /// GETSET: grava o novo valor e devolve o anterior.
    function GetSet(const AKey, AValue: TRedisArg): IRedisReply;
    /// GETEX com prazo em segundos: le e reestica o TTL de uma vez
    /// (Redis 6.2+).
    function GetEx(const AKey: TRedisArg; ASeconds: Int64): IRedisReply;
    /// GETEX PERSIST: le e remove o prazo.
    function GetExPersist(const AKey: TRedisArg): IRedisReply;

    /// APPEND: concatena e devolve o tamanho final.
    function Append(const AKey, AValue: TRedisArg): Int64;
    /// STRLEN em BYTES, nao em caracteres — um 'ç' em UTF-8 conta 2.
    function StrLen(const AKey: TRedisArg): Int64;
    /// GETRANGE por indice de byte, com os dois extremos incluidos e indice
    /// negativo contando do fim (-1 e' o ultimo byte).
    function GetRange(const AKey: TRedisArg; AStart, AStop: Int64): string;
    function GetRangeBytes(const AKey: TRedisArg; AStart, AStop: Int64): TBytes;
    /// SETRANGE: sobrescreve a partir do offset, preenchendo com zeros o que
    /// faltar. Devolve o tamanho final.
    function SetRange(const AKey: TRedisArg; AOffset: Int64;
      const AValue: TRedisArg): Int64;

    /// INCR e familia. Levantam ERedisReplyError se o valor atual nao for um
    /// inteiro valido; chave ausente conta como zero.
    function Incr(const AKey: TRedisArg): Int64;
    function IncrBy(const AKey: TRedisArg; ADelta: Int64): Int64;
    function Decr(const AKey: TRedisArg): Int64;
    function DecrBy(const AKey: TRedisArg; ADelta: Int64): Int64;
    /// INCRBYFLOAT. O delta vai formatado com ponto decimal
    /// (RedisFormatDouble), e nao pelo locale da maquina.
    function IncrByFloat(const AKey: TRedisArg; ADelta: Double): Double;

    /// MSET: grava varias chaves de uma vez. O array e' chave, valor, chave,
    /// valor... Levanta se vier com tamanho impar.
    procedure MSet(const AKeyValues: array of TRedisArg);
    /// MSETNX: so' grava se NENHUMA das chaves existir. Ou tudo, ou nada.
    function MSetNx(const AKeyValues: array of TRedisArg): Boolean;
    /// MGET. Devolve a resposta crua: a chave ausente aparece como item NULO
    /// no meio da lista, e rebaixar isso para '' apagaria a informacao.
    function MGet(const AKeys: array of TRedisArg): IRedisReply;
  end;

/// Opcoes de SET com os defaults: grava sempre, sem prazo, devolve +OK.
function RedisDefaultSetOptions: TRedisSetOptions;

implementation

function RedisDefaultSetOptions: TRedisSetOptions;
begin
  Result.Condition := scAlways;
  Result.Expiry := seNone;
  Result.ExpiryValue := 0;
  Result.ReturnOldValue := False;
end;

{ TRedisStringsCommands }

procedure TRedisStringsCommands.SetValue(const AKey, AValue: TRedisArg);
begin
  CmdVoid('SET', [AKey, AValue]);
end;

function TRedisStringsCommands.SetWithOptions(const AKey, AValue: TRedisArg;
  const AOptions: TRedisSetOptions): IRedisReply;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AValue], []);

  // A ordem NX/XX -> GET -> expiracao e' a da especificacao do comando; o
  // servidor recusa argumentos fora de ordem.
  case AOptions.Condition of
    scNotExists: RedisAddArg(LArgs, 'NX');
    scExists:    RedisAddArg(LArgs, 'XX');
  end;

  if AOptions.ReturnOldValue then
    RedisAddArg(LArgs, 'GET');

  case AOptions.Expiry of
    seSeconds:
      begin
        RedisAddArg(LArgs, 'EX');
        RedisAddArg(LArgs, AOptions.ExpiryValue);
      end;
    seMilliseconds:
      begin
        RedisAddArg(LArgs, 'PX');
        RedisAddArg(LArgs, AOptions.ExpiryValue);
      end;
    seUnixSeconds:
      begin
        RedisAddArg(LArgs, 'EXAT');
        RedisAddArg(LArgs, AOptions.ExpiryValue);
      end;
    seUnixMilliseconds:
      begin
        RedisAddArg(LArgs, 'PXAT');
        RedisAddArg(LArgs, AOptions.ExpiryValue);
      end;
    seKeepTtl:
      RedisAddArg(LArgs, 'KEEPTTL');
  end;

  Result := Cmd('SET', LArgs);
end;

function TRedisStringsCommands.SetNx(const AKey, AValue: TRedisArg): Boolean;
begin
  Result := CmdBool('SETNX', [AKey, AValue]);
end;

procedure TRedisStringsCommands.SetEx(const AKey: TRedisArg; ASeconds: Int64;
  const AValue: TRedisArg);
begin
  CmdVoid('SETEX', [AKey, ASeconds, AValue]);
end;

procedure TRedisStringsCommands.PSetEx(const AKey: TRedisArg;
  AMilliseconds: Int64; const AValue: TRedisArg);
begin
  CmdVoid('PSETEX', [AKey, AMilliseconds, AValue]);
end;

function TRedisStringsCommands.Get(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('GET', [AKey]);
end;

function TRedisStringsCommands.GetString(const AKey: TRedisArg): string;
begin
  Result := Cmd('GET', [AKey]).AsString;
end;

function TRedisStringsCommands.GetBytes(const AKey: TRedisArg): TBytes;
begin
  Result := Cmd('GET', [AKey]).AsBytes;
end;

function TRedisStringsCommands.TryGet(const AKey: TRedisArg;
  out AValue: string): Boolean;
var
  LReply: IRedisReply;
begin
  LReply := Cmd('GET', [AKey]);
  Result := not LReply.IsNull;
  if Result then
    AValue := LReply.AsString
  else
    AValue := '';
end;

function TRedisStringsCommands.TryGetBytes(const AKey: TRedisArg;
  out AValue: TBytes): Boolean;
var
  LReply: IRedisReply;
begin
  LReply := Cmd('GET', [AKey]);
  Result := not LReply.IsNull;
  if Result then
    AValue := LReply.AsBytes
  else
    AValue := nil;
end;

function TRedisStringsCommands.GetDel(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('GETDEL', [AKey]);
end;

function TRedisStringsCommands.GetSet(const AKey, AValue: TRedisArg): IRedisReply;
begin
  Result := Cmd('GETSET', [AKey, AValue]);
end;

function TRedisStringsCommands.GetEx(const AKey: TRedisArg;
  ASeconds: Int64): IRedisReply;
begin
  Result := Cmd('GETEX', [AKey, 'EX', ASeconds]);
end;

function TRedisStringsCommands.GetExPersist(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('GETEX', [AKey, 'PERSIST']);
end;

function TRedisStringsCommands.Append(const AKey, AValue: TRedisArg): Int64;
begin
  Result := CmdInt('APPEND', [AKey, AValue]);
end;

function TRedisStringsCommands.StrLen(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('STRLEN', [AKey]);
end;

function TRedisStringsCommands.GetRange(const AKey: TRedisArg;
  AStart, AStop: Int64): string;
begin
  Result := CmdString('GETRANGE', [AKey, AStart, AStop]);
end;

function TRedisStringsCommands.GetRangeBytes(const AKey: TRedisArg;
  AStart, AStop: Int64): TBytes;
begin
  Result := Cmd('GETRANGE', [AKey, AStart, AStop]).AsBytes;
end;

function TRedisStringsCommands.SetRange(const AKey: TRedisArg; AOffset: Int64;
  const AValue: TRedisArg): Int64;
begin
  Result := CmdInt('SETRANGE', [AKey, AOffset, AValue]);
end;

function TRedisStringsCommands.Incr(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('INCR', [AKey]);
end;

function TRedisStringsCommands.IncrBy(const AKey: TRedisArg; ADelta: Int64): Int64;
begin
  Result := CmdInt('INCRBY', [AKey, ADelta]);
end;

function TRedisStringsCommands.Decr(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('DECR', [AKey]);
end;

function TRedisStringsCommands.DecrBy(const AKey: TRedisArg; ADelta: Int64): Int64;
begin
  Result := CmdInt('DECRBY', [AKey, ADelta]);
end;

function TRedisStringsCommands.IncrByFloat(const AKey: TRedisArg;
  ADelta: Double): Double;
begin
  Result := CmdDouble('INCRBYFLOAT', [AKey, ADelta]);
end;

procedure TRedisStringsCommands.MSet(const AKeyValues: array of TRedisArg);
begin
  if Length(AKeyValues) = 0 then
    raise ERedisException.Create('MSET sem par chave/valor');
  if Odd(Length(AKeyValues)) then
    raise ERedisException.Create('MSET espera chave, valor, chave, valor...');
  CmdVoid('MSET', AKeyValues);
end;

function TRedisStringsCommands.MSetNx(const AKeyValues: array of TRedisArg): Boolean;
begin
  if Length(AKeyValues) = 0 then
    raise ERedisException.Create('MSETNX sem par chave/valor');
  if Odd(Length(AKeyValues)) then
    raise ERedisException.Create('MSETNX espera chave, valor, chave, valor...');
  Result := CmdBool('MSETNX', AKeyValues);
end;

function TRedisStringsCommands.MGet(const AKeys: array of TRedisArg): IRedisReply;
begin
  if Length(AKeys) = 0 then
    raise ERedisException.Create('MGET sem chave');
  Result := Cmd('MGET', AKeys);
end;

end.
