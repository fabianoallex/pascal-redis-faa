unit Redis.Transaction;

{ MULTI/EXEC/WATCH: o bloco atomico do Redis.

  O que uma transacao do Redis E': a garantia de que os comandos do bloco rodam
  em sequencia, sem nenhum outro cliente intercalando comando no meio.

  O que ela NAO e', e vale ler duas vezes porque o nome engana:

  - **Nao ha' rollback.** Se o terceiro de cinco comandos falhar (um WRONGTYPE,
    por exemplo), os outros quatro rodam assim mesmo e ficam gravados. O erro
    vem no item 3 do array de respostas e mais nada acontece. Quem quiser
    "tudo ou nada" de verdade precisa de um script Lua, que o servidor executa
    como uma unidade e onde um erro interrompe o resto.
  - **Nao ha' leitura dentro do bloco.** Entre MULTI e EXEC os comandos so' sao
    ENFILEIRADOS; nenhum devolve valor. Nao da' para ler um saldo e decidir o
    que gravar dentro da transacao. E' justamente esse buraco que o WATCH
    preenche, por fora do bloco.

  O padrao completo (o "check-and-set" do Redis) e' este:

      LTx := LClient.BeginTransaction;
      try
        LTx.Watch(['saldo']);                                   // vigia
        LSaldo := LTx.Connection.Execute('GET', ['saldo']).AsInteger;  // le
        if LSaldo < 100 then
          Exit;                                                 // decide
        LTx.Queue('SET', ['saldo', LSaldo - 100]);              // enfileira
        if not LTx.TryCommit(LRespostas) then
          ; // outra conexao mexeu em 'saldo': recomece o ciclo
      finally
        LTx.Free;
      end;

  A leitura acontece FORA do bloco, entre o WATCH e o MULTI, e o EXEC so'
  sucede se ninguem tiver tocado nas chaves vigiadas nesse intervalo. Devolver
  False no TryCommit e' o caso NORMAL sob concorrencia, nao um erro — quem
  chama recomeca o ciclo.

  Implementacao: os comandos sao acumulados LOCALMENTE e vao ao servidor de uma
  vez so', num unico pipeline `MULTI cmd1 ... cmdN EXEC`. Uma transacao de dez
  comandos custa UMA ida e volta em vez de doze. Duas consequencias:

  - O `DISCARD` quase nunca precisa ir ao fio: se o commit nao aconteceu, o
    MULTI tambem nao foi enviado, e nao ha' o que descartar no servidor. O que
    o Discard PRECISA mandar e' o `UNWATCH`.
  - Enfileirar nao valida nada. Um comando inexistente ou com aridade errada so'
    e' recusado quando o lote chega ao servidor, e ai' o EXEC inteiro aborta
    (EXECABORT). O TryCommit levanta apontando QUAL comando o servidor recusou.

  WATCH e' estado DA CONEXAO, e essa e' a razao de a transacao segurar uma
  conexao inteira para si. Tambem e' a razao de o destrutor mandar UNWATCH: uma
  conexao devolvida ao pool com WATCH pendente faria o EXEC do PROXIMO usuario
  abortar sem motivo aparente. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Resp,
  Redis.Connection;

type
  /// A transacao nao pode continuar: o servidor recusou o MULTI, recusou um
  /// comando na fila (e por isso abortou o EXEC) ou a transacao ja' foi
  /// encerrada. Indica bug de quem chama, nao concorrencia.
  ERedisTransactionError = class(ERedisException);

  /// O EXEC devolveu nulo: alguma chave vigiada por WATCH mudou entre o WATCH
  /// e o EXEC. **Nao e' falha** — e' o mecanismo de trabalho do check-and-set,
  /// e a resposta certa quase sempre e' recomecar o ciclo. Levantada apenas
  /// pelo Commit; o TryCommit devolve False.
  ERedisTransactionAborted = class(ERedisTransactionError);

  /// Como a transacao devolve a conexao a quem a emprestou. Metodo em vez de
  /// interface para nao arrastar refcount para dentro do pool, e para nao criar
  /// dependencia circular com a Redis.Client.
  TRedisReleaseConnection = procedure(AConnection: TRedisConnection) of object;

  /// Em que ponto do ciclo a transacao esta'. Depois de tsCommitted ou
  /// tsDiscarded ela nao aceita mais nada: crie outra.
  TRedisTransactionState = (tsBuilding, tsCommitted, tsDiscarded);

  /// Um bloco MULTI/EXEC sobre uma conexao dedicada.
  ///
  /// NAO e' thread-safe e nem deveria ser: a transacao existe justamente para
  /// que uma sequencia de comandos saia por uma conexao so', sem ninguem
  /// intercalando.
  TRedisTransaction = class
  private
    FConnection: TRedisConnection;
    FOnRelease: TRedisReleaseConnection;
    FLote: TRedisPipeline;
    FState: TRedisTransactionState;
    FWatching: Boolean;
    function GetCount: Integer;
    procedure EnsureBuilding;
    procedure ResetLote;
    /// UNWATCH engolindo falha. So' para o caminho de destruicao: ali a
    /// conexao pode ja' estar morta, e levantar dentro de um destrutor
    /// mascararia a excecao que estava subindo.
    procedure UnwatchBestEffort;
  public
    /// Adota uma conexao ABERTA. AOnRelease, quando informado, e' chamado no
    /// destrutor para devolve-la a quem a emprestou (o pool, via
    /// TRedisClient.Release). Nil = a conexao e' de outra pessoa e a transacao
    /// nao mexe no ciclo de vida dela.
    constructor Create(AConnection: TRedisConnection;
      AOnRelease: TRedisReleaseConnection = nil);
    destructor Destroy; override;

    /// WATCH: passa a vigiar as chaves. Se qualquer uma delas for modificada
    /// por outra conexao antes do EXEC, o commit aborta.
    ///
    /// Vai ao servidor NA HORA (diferente do Queue), porque so' depois dele e'
    /// que faz sentido ler os valores para decidir o que enfileirar.
    ///
    /// ATENCAO a ordem: vigiar DEPOIS de ler nao protege nada — uma alteracao
    /// entre a leitura e o WATCH passa despercebida. A sequencia correta e'
    /// sempre vigiar, ler, decidir, enfileirar, commitar. A lib nao consegue
    /// verificar isso (nao sabe quando voce leu), entao fica registrado aqui.
    procedure Watch(const AKeys: array of TRedisArg);

    /// UNWATCH: para de vigiar tudo. O EXEC e o commit abortado ja' fazem isso
    /// sozinhos no servidor; isto serve para desistir sem commitar.
    procedure Unwatch;

    /// Enfileira um comando LOCALMENTE. Nao vai ao servidor e nao devolve
    /// valor: o resultado so' existe depois do commit, na posicao
    /// correspondente do array de respostas.
    procedure Queue(const AName: string; const AArgs: array of TRedisArg); overload;
    procedure Queue(const AName: string); overload;
    /// Enfileira com o nome do comando dentro do array — util para comandos de
    /// duas palavras montados dinamicamente.
    procedure QueueArgs(const AArgs: array of TRedisArg);

    /// Envia `MULTI + comandos + EXEC` numa tacada e devolve as respostas dos
    /// comandos, na ordem em que foram enfileirados.
    ///
    /// Devolve **False** quando o EXEC abortou porque uma chave vigiada mudou —
    /// o caso normal sob concorrencia. AReplies vem vazio nesse caso.
    ///
    /// Levanta ERedisTransactionError se o servidor recusou algum comando na
    /// fila (comando inexistente, aridade errada), citando a posicao.
    ///
    /// NAO levanta por erro de execucao de um comando: um WRONGTYPE no meio do
    /// bloco vem como item rkError no array, e os demais comandos rodaram assim
    /// mesmo — nao ha' rollback. Teste item a item com IsError/RaiseIfError.
    function TryCommit(out AReplies: TRedisReplyArray): Boolean;

    /// Como TryCommit, mas levanta ERedisTransactionAborted em vez de devolver
    /// False. Use quando nao houver WATCH (ai' o aborto e' de fato
    /// excepcional); sob check-and-set, prefira TryCommit.
    function Commit: TRedisReplyArray;

    /// Desiste: esvazia a fila local e manda UNWATCH se estiver vigiando. Nao
    /// precisa mandar DISCARD — o MULTI nunca chegou a sair daqui.
    procedure Discard;

    /// Quantos comandos estao enfileirados (e, portanto, quantas respostas o
    /// commit vai devolver).
    property Count: Integer read GetCount;
    property State: TRedisTransactionState read FState;
    property IsWatching: Boolean read FWatching;

    /// A conexao dedicada a esta transacao. E' por ela que se fazem as
    /// LEITURAS entre o Watch e o Queue — o unico jeito de decidir o que
    /// enfileirar, ja' que dentro do bloco nao ha' leitura.
    property Connection: TRedisConnection read FConnection;
  end;

implementation

{ TRedisTransaction }

constructor TRedisTransaction.Create(AConnection: TRedisConnection;
  AOnRelease: TRedisReleaseConnection);
begin
  inherited Create;
  if AConnection = nil then
    raise ERedisTransactionError.Create('transacao sem conexao');
  FConnection := AConnection;
  FOnRelease := AOnRelease;
  FLote := TRedisPipeline.Create;
  FState := tsBuilding;
  ResetLote;
end;

destructor TRedisTransaction.Destroy;
begin
  // Conexao com WATCH pendente NAO pode voltar ao pool: o EXEC do proximo
  // usuario abortaria sem motivo aparente, e ele nao teria como descobrir por
  // que. E' a mesma familia de bug da conexao suja do M2.
  if FWatching and (FConnection <> nil) then
    UnwatchBestEffort;
  FLote.Free;
  if Assigned(FOnRelease) and (FConnection <> nil) then
    FOnRelease(FConnection);
  inherited Destroy;
end;

procedure TRedisTransaction.ResetLote;
begin
  // O MULTI ja' entra no lote na primeira posicao: assim o Queue so' acrescenta
  // e o commit so' fecha com o EXEC, sem ninguem precisar remontar o buffer.
  FLote.Clear;
  FLote.Queue('MULTI');
end;

function TRedisTransaction.GetCount: Integer;
begin
  Result := FLote.Count - 1;  // desconta o MULTI
  if Result < 0 then
    Result := 0;
end;

procedure TRedisTransaction.EnsureBuilding;
begin
  if FState <> tsBuilding then
    raise ERedisTransactionError.Create(
      'transacao ja encerrada (commit ou discard); crie outra');
end;

procedure TRedisTransaction.UnwatchBestEffort;
begin
  try
    FConnection.ExecuteRaw('UNWATCH', []);
  except
    // Conexao morta ja' perdeu o WATCH junto com o socket, e o pool vai
    // destrui-la de qualquer forma.
  end;
  FWatching := False;
end;

procedure TRedisTransaction.Watch(const AKeys: array of TRedisArg);
begin
  EnsureBuilding;
  if Length(AKeys) = 0 then
    raise ERedisTransactionError.Create('WATCH sem chave');
  FConnection.Execute('WATCH', AKeys);
  FWatching := True;
end;

procedure TRedisTransaction.Unwatch;
begin
  EnsureBuilding;
  FConnection.Execute('UNWATCH', []);
  FWatching := False;
end;

procedure TRedisTransaction.Queue(const AName: string;
  const AArgs: array of TRedisArg);
begin
  EnsureBuilding;
  FLote.Queue(AName, AArgs);
end;

procedure TRedisTransaction.Queue(const AName: string);
begin
  Queue(AName, []);
end;

procedure TRedisTransaction.QueueArgs(const AArgs: array of TRedisArg);
begin
  EnsureBuilding;
  FLote.QueueArgs(AArgs);
end;

function TRedisTransaction.TryCommit(out AReplies: TRedisReplyArray): Boolean;
var
  LRespostas: TRedisReplyArray;
  LExec: IRedisReply;
  LQuantos, I: Integer;
begin
  AReplies := nil;
  EnsureBuilding;
  LQuantos := GetCount;

  // Ao contrario do pipeline vazio (que nao vai ao servidor), um bloco vazio
  // VAI: sob WATCH, `MULTI EXEC` sem comando nenhum e' uma pergunta legitima —
  // "alguem mexeu nas chaves que eu vigiava?" — e a resposta e' nulo ou array
  // vazio.
  FLote.Queue('EXEC');
  LRespostas := FConnection.ExecutePipeline(FLote);
  FState := tsCommitted;
  // O servidor limpa o WATCH no EXEC, tenha ele rodado ou abortado.
  FWatching := False;

  if Length(LRespostas) <> LQuantos + 2 then
    raise ERedisTransactionError.CreateFmt(
      'MULTI/EXEC devolveu %d respostas, esperava %d',
      [Length(LRespostas), LQuantos + 2]);

  if LRespostas[0].IsError then
    raise ERedisTransactionError.CreateFmt('MULTI recusado: %s',
      [LRespostas[0].ErrorMessage]);

  // Comando que o servidor se recusou a enfileirar (inexistente, aridade
  // errada). Ele marca a transacao e o EXEC aborta com EXECABORT — mas o erro
  // util e' este aqui, que diz QUAL comando estava torto.
  for I := 1 to LQuantos do
    if LRespostas[I].IsError then
      raise ERedisTransactionError.CreateFmt(
        'o servidor recusou o comando %d da transacao: %s',
        [I - 1, LRespostas[I].ErrorMessage]);

  LExec := LRespostas[LQuantos + 1];
  if LExec.IsError then
    raise ERedisTransactionError.CreateFmt('EXEC falhou: %s',
      [LExec.ErrorMessage]);

  // Nulo = uma chave vigiada mudou. Nao e' erro: e' o check-and-set dizendo
  // "recomece".
  if LExec.IsNull then
    Exit(False);

  if not LExec.IsAggregate then
    raise ERedisTransactionError.CreateFmt(
      'EXEC devolveu %s, esperava lista de respostas',
      [RedisReplyKindName(LExec.Kind)]);
  if LExec.Count <> LQuantos then
    raise ERedisTransactionError.CreateFmt(
      'EXEC devolveu %d respostas para %d comandos',
      [LExec.Count, LQuantos]);

  SetLength(AReplies, LQuantos);
  for I := 0 to LQuantos - 1 do
    AReplies[I] := LExec[I];
  Result := True;
end;

function TRedisTransaction.Commit: TRedisReplyArray;
begin
  if not TryCommit(Result) then
    raise ERedisTransactionAborted.Create(
      'EXEC abortado: uma chave vigiada mudou antes do commit');
end;

procedure TRedisTransaction.Discard;
begin
  EnsureBuilding;
  // Nada de DISCARD no fio: o MULTI ficou aqui dentro e nunca saiu. O que
  // PRECISA sair e' o UNWATCH — senao a conexao volta ao pool vigiando.
  if FWatching then
    Unwatch;
  ResetLote;
  FState := tsDiscarded;
end;

end.
