unit Redis.Commands;

{ Base das fachadas tipadas por familia de comandos (M4).

  O kernel do M2 ja' alcanca qualquer comando com Execute('SET', ['k','v']).
  O que falta e' conveniencia: converter '+OK' para Boolean, ':3' para Int64,
  um array de bulk strings para array of string, e lembrar a ordem exata dos
  modificadores de cada comando. E' o que as units Redis.Commands.* fazem, e
  esta aqui e' o chao comum delas.

  Duas pecas:

  - **TRedisCommandExecutor** — abstracao de "algo que executa comando". Existe
    para quebrar a dependencia circular: as familias precisam de quem execute,
    mas quem as reune (TRedisClient) precisa das familias. Com a abstracao
    aqui embaixo, a seta aponta em um sentido so':
    Redis.Commands <- Redis.Commands.Strings <- Redis.Client.

  - **TRedisCommandFamily** — base das familias. Guarda o executor e traz os
    conversores repetidos (CmdInt, CmdBool, CmdStrings, ExecScan).

  Convencao de tipos de retorno, valida em TODAS as familias:

  1. Comando que devolve escalar devolve tipo nativo: Boolean, Int64, Double,
     string.
  2. Comando cuja resposta pode ser NULA devolve IRedisReply (ou tem um par
     TryXxx com out), porque no Redis "ausente" e "vazio" sao coisas
     diferentes e rebaixar nulo para '' ou 0 apagaria a diferenca em silencio.
     Ver a decisao 14 em docs/DECISOES.md.
  3. Comando que devolve lista de valores devolve TRedisStringArray, com item
     nulo virando '' — nesses comandos (LRANGE, SMEMBERS, HKEYS) o servidor
     nao produz nulo no meio da lista. Onde produz (MGET, HMGET), o retorno e'
     IRedisReply de proposito.

  Binario-seguro por contrato: chaves, campos e valores entram como TRedisArg,
  que aceita string (via RedisUtf8Encode) e TBytes (byte a byte) na MESMA
  assinatura, sem sobrecarga duplicada. Onde o retorno pode ser binario ha'
  sempre um par ...Bytes. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types;

type
  /// Lista de valores textuais devolvida pelos comandos de agregado.
  /// Array simples de proposito: TArray<T> nao existe de forma equivalente no
  /// Generics.Collections do FPC 3.2.2.
  TRedisStringArray = array of string;

  /// Mesma lista, sem interpretar codepage — para valores que podem nao ser
  /// texto.
  TRedisBytesArray = array of TBytes;

  /// Resposta de comandos que devolvem um 0/1 por elemento (SMISMEMBER).
  TRedisBooleanArray = array of Boolean;

  /// Algo capaz de executar comandos Redis.
  ///
  /// Implementado por TRedisClient (Redis.Client), que resolve sozinho se o
  /// comando vai por uma conexao do pool ou pela conexao unica a que o cliente
  /// esta' amarrado. As familias so' conhecem esta abstracao — e' o que as
  /// mantem ignorantes de pool, socket e protocolo.
  TRedisCommandExecutor = class
  public
    /// Executa e levanta ERedisReplyError se o servidor respondeu erro.
    function Execute(const AName: string;
      const AArgs: array of TRedisArg): IRedisReply; overload; virtual; abstract;

    /// Executa e devolve o erro do servidor como no' rkError, sem levantar.
    function ExecuteRaw(const AName: string;
      const AArgs: array of TRedisArg): IRedisReply; overload; virtual; abstract;

    /// Executa um comando BLOQUEANTE (BLPOP, BRPOP, BLMOVE), garantindo que a
    /// espera pela resposta seja maior que o timeout do proprio comando.
    ///
    /// ATimeoutSeconds e' o timeout que vai no comando, na unidade que o Redis
    /// usa (segundos, fracionarios desde a 6.0). Zero ou negativo significa
    /// "espera indefinida", tanto para o servidor quanto para o socket.
    ///
    /// Nunca sai do pool comum: um comando que segura a conexao por 30 s
    /// esvaziaria o pool inteiro. Ver docs/DECISOES.md.
    function ExecuteBlocking(const AName: string; const AArgs: array of TRedisArg;
      ATimeoutSeconds: Double): IRedisReply; virtual; abstract;

    /// Atalhos para comando sem argumento.
    function Execute(const AName: string): IRedisReply; overload;
    function ExecuteRaw(const AName: string): IRedisReply; overload;
  end;

  /// Base das fachadas por familia. Nao e' dona do executor.
  TRedisCommandFamily = class
  private
    FExecutor: TRedisCommandExecutor;
  protected
    /// Executa levantando em erro de servidor. E' o default das familias: numa
    /// fachada tipada, um WRONGTYPE e' bug de quem chamou, nao um valor de
    /// retorno a ser ignorado.
    function Cmd(const AName: string; const AArgs: array of TRedisArg): IRedisReply;
    /// Executa sem levantar em erro de servidor.
    function CmdRaw(const AName: string; const AArgs: array of TRedisArg): IRedisReply;
    /// Executa e descarta a resposta (comandos que so' respondem +OK).
    procedure CmdVoid(const AName: string; const AArgs: array of TRedisArg);
    function CmdInt(const AName: string; const AArgs: array of TRedisArg): Int64;
    function CmdBool(const AName: string; const AArgs: array of TRedisArg): Boolean;
    function CmdDouble(const AName: string; const AArgs: array of TRedisArg): Double;
    function CmdString(const AName: string; const AArgs: array of TRedisArg): string;
    function CmdStrings(const AName: string;
      const AArgs: array of TRedisArg): TRedisStringArray;

    /// Roda um passo de SCAN/HSCAN/SSCAN/ZSCAN: le o cursor devolvido para
    /// ACursor e devolve os elementos daquele passo.
    ///
    /// O cursor NAO e' um indice: e' opaco, pode repetir elementos entre
    /// passos e so' o valor 0 significa "terminou". Por isso o laco correto e'
    /// repeat ... until ACursor = 0, e nunca uma contagem.
    function ExecScan(const AName: string; const AArgs: array of TRedisArg;
      var ACursor: Int64): TRedisStringArray;
  public
    constructor Create(AExecutor: TRedisCommandExecutor);

    /// Quem executa os comandos desta familia.
    property Executor: TRedisCommandExecutor read FExecutor;
  end;

/// Concatena argumentos fixos com uma lista variavel, na ordem.
/// RedisArgs([AKey], AValues) monta o array que LPUSH chave v1 v2 ... precisa.
function RedisArgs(const AFixed: array of TRedisArg;
  const ARest: array of TRedisArg): TRedisArgs;

/// Acrescenta um argumento ao fim (para montar modificadores opcionais).
procedure RedisAddArg(var AArgs: TRedisArgs; const AArg: TRedisArg);

/// Converte um agregado em lista de strings. Nulo (chave ausente) vira lista
/// vazia; item nulo vira ''.
function RedisReplyToStrings(const AReply: IRedisReply): TRedisStringArray;

/// Mesma coisa sem interpretar codepage.
function RedisReplyToBytesArray(const AReply: IRedisReply): TRedisBytesArray;

/// Converte um agregado de 0/1 em lista de booleanos (SMISMEMBER).
function RedisReplyToBooleans(const AReply: IRedisReply): TRedisBooleanArray;

implementation

{ Helpers de unit }

function RedisArgs(const AFixed: array of TRedisArg;
  const ARest: array of TRedisArg): TRedisArgs;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, Length(AFixed) + Length(ARest));
  for I := 0 to High(AFixed) do
    Result[I] := AFixed[I];
  for I := 0 to High(ARest) do
    Result[Length(AFixed) + I] := ARest[I];
end;

procedure RedisAddArg(var AArgs: TRedisArgs; const AArg: TRedisArg);
begin
  SetLength(AArgs, Length(AArgs) + 1);
  AArgs[High(AArgs)] := AArg;
end;

function RedisReplyToStrings(const AReply: IRedisReply): TRedisStringArray;
var
  I: Integer;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.CreateFmt(
      'esperava lista, veio %s', [RedisReplyKindName(AReply.Kind)]);
  SetLength(Result, AReply.Count);
  for I := 0 to AReply.Count - 1 do
    Result[I] := AReply[I].AsString;
end;

function RedisReplyToBytesArray(const AReply: IRedisReply): TRedisBytesArray;
var
  I: Integer;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.CreateFmt(
      'esperava lista, veio %s', [RedisReplyKindName(AReply.Kind)]);
  SetLength(Result, AReply.Count);
  for I := 0 to AReply.Count - 1 do
    Result[I] := AReply[I].AsBytes;
end;

function RedisReplyToBooleans(const AReply: IRedisReply): TRedisBooleanArray;
var
  I: Integer;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.CreateFmt(
      'esperava lista, veio %s', [RedisReplyKindName(AReply.Kind)]);
  SetLength(Result, AReply.Count);
  for I := 0 to AReply.Count - 1 do
    Result[I] := AReply[I].AsBoolean;
end;

{ TRedisCommandExecutor }

function TRedisCommandExecutor.Execute(const AName: string): IRedisReply;
begin
  Result := Execute(AName, []);
end;

function TRedisCommandExecutor.ExecuteRaw(const AName: string): IRedisReply;
begin
  Result := ExecuteRaw(AName, []);
end;

{ TRedisCommandFamily }

constructor TRedisCommandFamily.Create(AExecutor: TRedisCommandExecutor);
begin
  inherited Create;
  if AExecutor = nil then
    raise ERedisException.Create('familia de comandos sem executor');
  FExecutor := AExecutor;
end;

function TRedisCommandFamily.Cmd(const AName: string;
  const AArgs: array of TRedisArg): IRedisReply;
begin
  Result := FExecutor.Execute(AName, AArgs);
end;

function TRedisCommandFamily.CmdRaw(const AName: string;
  const AArgs: array of TRedisArg): IRedisReply;
begin
  Result := FExecutor.ExecuteRaw(AName, AArgs);
end;

procedure TRedisCommandFamily.CmdVoid(const AName: string;
  const AArgs: array of TRedisArg);
begin
  FExecutor.Execute(AName, AArgs);
end;

function TRedisCommandFamily.CmdInt(const AName: string;
  const AArgs: array of TRedisArg): Int64;
begin
  Result := FExecutor.Execute(AName, AArgs).AsInteger;
end;

function TRedisCommandFamily.CmdBool(const AName: string;
  const AArgs: array of TRedisArg): Boolean;
begin
  Result := FExecutor.Execute(AName, AArgs).AsBoolean;
end;

function TRedisCommandFamily.CmdDouble(const AName: string;
  const AArgs: array of TRedisArg): Double;
begin
  Result := FExecutor.Execute(AName, AArgs).AsDouble;
end;

function TRedisCommandFamily.CmdString(const AName: string;
  const AArgs: array of TRedisArg): string;
begin
  Result := FExecutor.Execute(AName, AArgs).AsString;
end;

function TRedisCommandFamily.CmdStrings(const AName: string;
  const AArgs: array of TRedisArg): TRedisStringArray;
begin
  Result := RedisReplyToStrings(FExecutor.Execute(AName, AArgs));
end;

function TRedisCommandFamily.ExecScan(const AName: string;
  const AArgs: array of TRedisArg; var ACursor: Int64): TRedisStringArray;
var
  LReply: IRedisReply;
  LCursor: Int64;
begin
  LReply := Cmd(AName, AArgs);
  // A resposta de um SCAN e' sempre um par [cursor, elementos]. Qualquer outra
  // forma significa que o comando nao era um SCAN ou que o fluxo desandou.
  if (not LReply.IsAggregate) or (LReply.Count <> 2) then
    raise ERedisTypeError.CreateFmt(
      '%s: esperava par [cursor, elementos], veio %s',
      [AName, RedisReplyKindName(LReply.Kind)]);
  if not RedisTryParseInt64(LReply[0].AsString, LCursor) then
    raise ERedisTypeError.CreateFmt('%s: cursor invalido (%s)',
      [AName, LReply[0].AsString]);
  ACursor := LCursor;
  Result := RedisReplyToStrings(LReply[1]);
end;

end.
