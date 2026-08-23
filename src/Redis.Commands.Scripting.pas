unit Redis.Commands.Scripting;

{ EVAL / EVALSHA: Lua rodando dentro do servidor.

  E' a unica forma de atomicidade REAL do Redis. O MULTI/EXEC garante que
  ninguem intercala comando no meio do bloco, mas nao deixa ler nada la' dentro
  e nao desfaz nada quando um comando falha. Um script Lua le, decide e escreve
  numa unica passagem, e enquanto ele roda o servidor nao atende mais ninguem —
  o que tem a contrapartida obvia: **script demorado trava o servidor inteiro**.
  Script bom e' script curto.

  O exemplo classico e o release de lock distribuido, que nao existe sem script:

      const LIBERA =
        'if redis.call("GET", KEYS[1]) == ARGV[1] then' + sLineBreak +
        '  return redis.call("DEL", KEYS[1])' + sLineBreak +
        'else' + sLineBreak +
        '  return 0' + sLineBreak +
        'end';

      LClient.Scripting.Run(LIBERA, ['lock:pedido:7'], [LMeuToken]);

  Fazer isso com GET seguido de DEL seria a corrida classica: entre os dois, o
  lock pode expirar e ser tomado por outra pessoa, e o DEL apagaria o lock
  DELA. Compare e apague na mesma passagem, ou nao compare.

  ## O cache de SHA

  Mandar o texto do script em toda chamada desperdica banda: o Redis guarda os
  scripts que ja' viu e aceita EVALSHA com os 40 caracteres do SHA-1 no lugar.
  Esta familia faz isso sozinha, no metodo Run:

  1. Calcula o SHA-1 **localmente**, sobre os mesmos bytes UTF-8 que iriam ao
     fio. Nao ha' round-trip para descobri-lo — o servidor calcularia o mesmo.
  2. Primeira vez que ve o script: manda EVAL direto. O servidor executa E
     guarda; o SHA entra no cache local. Custo: uma ida e volta, igual a
     qualquer comando — o "aquecimento" nao custa nada a mais.
  3. Da' segunda em diante: EVALSHA, que troca kilobytes de Lua por 40 bytes.
  4. Se o servidor responder NOSCRIPT — cache dele limpo por SCRIPT FLUSH,
     restart, ou failover para uma replica que nunca viu o script — a familia
     reenvia o EVAL sozinha. Nenhum erro chega a quem chamou.

  O passo 4 e' o que torna o cache seguro: ele e' uma otimizacao que se
  autocorrige, nunca uma suposicao sobre o estado do servidor.

  O cache vive no CLIENTE (todas as conexoes do pool falam com o mesmo
  servidor), e por isso e' protegido por lock: varias threads podem estar
  rodando o mesmo script pela primeira vez ao mesmo tempo. }

{$I redis.inc}

interface

uses
  SysUtils,
  Classes,
  SyncObjs,
  {$IFDEF FPC}
  sha1,
  {$ELSE}
  Hash,
  {$ENDIF}
  Redis.Types,
  Redis.Commands;

type
  /// EVAL, EVALSHA e a familia SCRIPT, com cache de SHA embutido.
  TRedisScriptingCommands = class(TRedisCommandFamily)
  private
    FLock: TCriticalSection;
    /// SHAs que este cliente ja' viu o servidor aceitar. Ordenada: a busca e'
    /// binaria e o dupIgnore evita entrada repetida.
    FConhecidos: TStringList;
    function JaConhecido(const ASha: string): Boolean;
    procedure Memoriza(const ASha: string);
  public
    constructor Create(AExecutor: TRedisCommandExecutor); override;
    destructor Destroy; override;

    /// Executa o script mandando o TEXTO (EVAL). Sempre funciona, sempre custa
    /// a banda do script inteiro.
    function Eval(const AScript: string;
      const AKeys, AArgs: array of TRedisArg): IRedisReply;

    /// Executa pelo SHA (EVALSHA). Levanta ERedisReplyError com codigo
    /// NOSCRIPT se o servidor nao tiver o script — use Run para nao precisar
    /// tratar isso.
    function EvalSha(const ASha: string;
      const AKeys, AArgs: array of TRedisArg): IRedisReply;

    /// **A forma recomendada.** Usa EVALSHA quando o script ja' e' conhecido,
    /// EVAL na primeira vez, e reenvia sozinha se o servidor tiver esquecido o
    /// script. Uma ida e volta em qualquer dos casos.
    ///
    /// AKeys sao as chaves que o script toca (viram KEYS[1..n] no Lua) e AArgs
    /// o resto (ARGV[1..n]). Separar as duas coisas nao e' burocracia: e' o que
    /// permite ao servidor saber quais chaves o script acessa.
    function Run(const AScript: string;
      const AKeys, AArgs: array of TRedisArg): IRedisReply;

    /// SCRIPT LOAD: manda o script e devolve o SHA, sem executar. Serve para
    /// pre-aquecer o servidor no start da aplicacao. O SHA devolvido e' igual
    /// ao que RedisScriptSha calcula aqui — se algum dia diferir, e' porque os
    /// bytes enviados nao sao os que foram medidos.
    function ScriptLoad(const AScript: string): string;

    /// SCRIPT EXISTS: o servidor tem este SHA em cache AGORA? Consulta o
    /// servidor, nao o cache local.
    function ScriptExists(const ASha: string): Boolean;

    /// SCRIPT FLUSH: apaga o cache DO SERVIDOR (e, junto, o local — manter o
    /// nosso seria continuar apostando num EVALSHA que vai dar NOSCRIPT).
    procedure ScriptFlush;

    /// Esquece o que este cliente memorizou, sem tocar no servidor. Raramente
    /// necessario; existe para testes e para quem troca de servidor por baixo.
    procedure ClearCache;

    /// Quantos SHAs este cliente tem memorizados.
    function CachedCount: Integer;
  end;

/// SHA-1 do script, em hexadecimal minusculo — o mesmo identificador que o
/// SCRIPT LOAD devolveria.
///
/// Calculado sobre os BYTES UTF-8 do texto, e nao sobre a string: no FPC a
/// 'string' carrega codepage dinamico, e hashear a representacao local daria um
/// SHA diferente do que o servidor calcula assim que houvesse um acento no
/// script (num comentario Lua, por exemplo). O sintoma seria EVALSHA sempre
/// respondendo NOSCRIPT, com o cache "funcionando" e nunca acertando.
function RedisScriptSha(const AScript: string): string;

implementation

function RedisScriptSha(const AScript: string): string;
var
  LBytes: TBytes;
  LTamanho: Integer;
  {$IFNDEF FPC}
  LHash: THashSHA1;
  {$ENDIF}
begin
  LBytes := RedisUtf8Encode(AScript);
  LTamanho := Length(LBytes);
  // Os dois digestores recebem o buffer por parametro UNTYPED, que exige um
  // endereco valido mesmo quando o comprimento e' zero. Um byte de folga
  // resolve sem mexer no que sera' lido (LTamanho continua zero).
  if LTamanho = 0 then
    SetLength(LBytes, 1);
  {$IFDEF FPC}
  Result := SHA1Print(SHA1Buffer(LBytes[0], LTamanho));
  {$ELSE}
  LHash := THashSHA1.Create;
  LHash.Update(LBytes[0], LTamanho);
  Result := LHash.HashAsString;
  {$ENDIF}
  Result := LowerCase(Result);
end;

{ Monta os argumentos de EVAL/EVALSHA: <script|sha> <numkeys> <chaves...>
  <argumentos...>. O numero de chaves vai explicito porque e' o que separa as
  duas listas — sem ele o servidor nao teria como saber onde KEYS acaba. }
function MontaEvalArgs(const APrimeiro: TRedisArg;
  const AKeys, AArgs: array of TRedisArg): TRedisArgs;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, 2 + Length(AKeys) + Length(AArgs));
  Result[0] := APrimeiro;
  Result[1] := Length(AKeys);
  for I := 0 to High(AKeys) do
    Result[2 + I] := AKeys[I];
  for I := 0 to High(AArgs) do
    Result[2 + Length(AKeys) + I] := AArgs[I];
end;

{ TRedisScriptingCommands }

constructor TRedisScriptingCommands.Create(AExecutor: TRedisCommandExecutor);
begin
  inherited Create(AExecutor);
  FLock := TCriticalSection.Create;
  FConhecidos := TStringList.Create;
  FConhecidos.Sorted := True;
  FConhecidos.Duplicates := dupIgnore;
end;

destructor TRedisScriptingCommands.Destroy;
begin
  FConhecidos.Free;
  FLock.Free;
  inherited Destroy;
end;

function TRedisScriptingCommands.JaConhecido(const ASha: string): Boolean;
var
  LIndice: Integer;
begin
  FLock.Enter;
  try
    Result := FConhecidos.Find(ASha, LIndice);
  finally
    FLock.Leave;
  end;
end;

procedure TRedisScriptingCommands.Memoriza(const ASha: string);
begin
  FLock.Enter;
  try
    FConhecidos.Add(ASha);
  finally
    FLock.Leave;
  end;
end;

procedure TRedisScriptingCommands.ClearCache;
begin
  FLock.Enter;
  try
    FConhecidos.Clear;
  finally
    FLock.Leave;
  end;
end;

function TRedisScriptingCommands.CachedCount: Integer;
begin
  FLock.Enter;
  try
    Result := FConhecidos.Count;
  finally
    FLock.Leave;
  end;
end;

function TRedisScriptingCommands.Eval(const AScript: string;
  const AKeys, AArgs: array of TRedisArg): IRedisReply;
begin
  Result := Cmd('EVAL', MontaEvalArgs(AScript, AKeys, AArgs));
end;

function TRedisScriptingCommands.EvalSha(const ASha: string;
  const AKeys, AArgs: array of TRedisArg): IRedisReply;
begin
  Result := Cmd('EVALSHA', MontaEvalArgs(ASha, AKeys, AArgs));
end;

function TRedisScriptingCommands.Run(const AScript: string;
  const AKeys, AArgs: array of TRedisArg): IRedisReply;
var
  LSha: string;
begin
  LSha := RedisScriptSha(AScript);

  if JaConhecido(LSha) then
  begin
    // CmdRaw, e nao Cmd: NOSCRIPT aqui nao e' erro para quem chamou, e' um
    // recado de que o servidor esqueceu o script.
    Result := CmdRaw('EVALSHA', MontaEvalArgs(LSha, AKeys, AArgs));
    if not (Result.IsError and (Result.ErrorCode = 'NOSCRIPT')) then
    begin
      // Qualquer outro erro (inclusive erro do proprio Lua) sobe normalmente.
      Result.RaiseIfError;
      Exit;
    end;
    // Cai para o EVAL abaixo, que reensina o script ao servidor. O SHA
    // continua no cache: ele nao mudou, quem esqueceu foi o outro lado.
  end;

  Result := Cmd('EVAL', MontaEvalArgs(AScript, AKeys, AArgs));
  // So' memoriza DEPOIS do EVAL bem-sucedido: script com erro de sintaxe nao
  // fica no cache do servidor, e memorizar aqui condenaria toda chamada
  // seguinte a um EVALSHA inutil.
  Memoriza(LSha);
end;

function TRedisScriptingCommands.ScriptLoad(const AScript: string): string;
begin
  Result := LowerCase(Cmd('SCRIPT', ['LOAD', AScript]).AsString);
  Memoriza(Result);
end;

function TRedisScriptingCommands.ScriptExists(const ASha: string): Boolean;
var
  LReply: IRedisReply;
begin
  // SCRIPT EXISTS aceita varios SHAs e responde um array de 0/1; aqui vai um
  // so', entao a resposta e' um array de um elemento.
  LReply := Cmd('SCRIPT', ['EXISTS', ASha]);
  Result := LReply.IsAggregate and (LReply.Count = 1) and LReply[0].AsBoolean;
end;

procedure TRedisScriptingCommands.ScriptFlush;
begin
  CmdVoid('SCRIPT', ['FLUSH']);
  ClearCache;
end;

end.
