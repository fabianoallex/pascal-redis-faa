program SmokeTest;

{ Smoke test da pascal-redis-faa contra um Redis real (docker/docker-compose.yml).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src SmokeTest.dpr

  ESTADO (M5): conexao, pool, timeouts, fachadas por familia e TLS. Este
  programa exercita tudo de ponta a ponta — handshake, Execute generico,
  pipeline, erro de servidor, binario, RESP2 x RESP3, read timeout, pool,
  invalidacao de conexao e os comandos de chaves, strings, hashes, listas,
  conjuntos e sorted sets, inclusive um bloqueante. O que ainda nao existe:
  MULTI/EXEC e EVAL (M6), pub/sub (M7), streams (M8).

  COM --tls, o programa INTEIRO roda contra o listener cifrado (6380) em vez do
  de texto claro, e ganha uma secao dedicada ao TLS. Nao e' um modo separado com
  meia duzia de passos: e' a mesma bateria por cima da criptografia, que e' onde
  um envelope TLS mal-feito aparece (bulk grande atravessando varios registros
  TLS, pipeline numa escrita so', timeout de socket no meio de um registro).
  Precisa dos certs e do override — ver docker/docker-compose.tls.yml:

    docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
    SmokeTest.exe --tls

  As chaves criadas usam o prefixo 'pascal-redis-faa:smoke:' e sao apagadas no
  fim: nada de FLUSHDB, que apagaria dados de quem estiver usando o mesmo
  servidor.

  Sai com exit code 0 se tudo passou; 1 se algo falhou. }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ELSE}
  {$APPTYPE CONSOLE}
{$ENDIF}

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  {$ENDIF}
  SysUtils,
  Redis.Threading,
  Redis.Transport,
  Redis.Types,
  Redis.Resp,
  Redis.Connection,
  Redis.Pool,
  Redis.Commands,
  Redis.Commands.Keys,
  Redis.Commands.Strings,
  Redis.Commands.Hashes,
  Redis.Commands.Lists,
  Redis.Commands.Sets,
  Redis.Commands.ZSets,
  Redis.Client;

const
  HOST = 'localhost';
  PORT = REDIS_DEFAULT_PORT;
  PORT_TLS = REDIS_DEFAULT_TLS_PORT;
  PREFIXO = 'pascal-redis-faa:smoke:';

var
  GFalhas: Integer = 0;
  GPassos: Integer = 0;
  /// Ligado por --tls: manda TODAS as secoes pelo listener cifrado.
  GUsaTls: Boolean = False;

{ --tls manda o programa inteiro pelo listener cifrado. Qualquer outro
  argumento e recusado alto: um '--tsl' digitado errado rodaria em texto claro
  com cara de sucesso, e o smoke test perderia justamente o que se queria
  provar. }
function LeArgumentoTls: Boolean;
var
  I: Integer;
  LArg: string;
begin
  Result := False;
  for I := 1 to ParamCount do
  begin
    LArg := ParamStr(I);
    if LArg = '--tls' then
      Result := True
    else
    begin
      WriteLn('argumento desconhecido: ', LArg);
      WriteLn('uso: SmokeTest [--tls]');
      Halt(2);
    end;
  end;
end;

procedure Secao(const ATitulo: string);
begin
  WriteLn;
  WriteLn('== ', ATitulo);
end;

procedure Passo(const ANome: string; AOk: Boolean; const ADetalhe: string = '');
begin
  Inc(GPassos);
  if AOk then
    WriteLn('  [PASS] ', ANome, ADetalhe)
  else
  begin
    WriteLn('  [FAIL] ', ANome, ADetalhe);
    Inc(GFalhas);
  end;
end;

function Chave(const ASufixo: string): string;
begin
  Result := PREFIXO + ASufixo;
end;

function BytesIguais(const A, B: TBytes): Boolean;
var
  I: Integer;
begin
  if Length(A) <> Length(B) then
    Exit(False);
  for I := 0 to High(A) do
    if A[I] <> B[I] then
      Exit(False);
  Result := True;
end;

function MontaBytes(const AValores: array of Byte): TBytes;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, Length(AValores));
  for I := 0 to High(AValores) do
    Result[I] := AValores[I];
end;

function Params: TRedisParams;
begin
  if GUsaTls then
  begin
    Result := RedisDefaultTlsParams;   // ja vem com a porta 6380 e UseTls
    // O cert de docker/certs e self-signed, entao a validacao de cadeia
    // recusaria. A lib NAO tem atalho para isso de proposito: baixar a
    // verificacao e uma decisao de seguranca e tem de aparecer numa linha
    // explicita de quem a toma — esta aqui.
    Result.TlsVerifyPeer := False;
  end
  else
    Result := RedisDefaultParams;
  Result.Host := HOST;
  // Aparece no CLIENT LIST do servidor — vale ouro quando varias apps dividem
  // o mesmo Redis e uma delas esta segurando conexao.
  Result.ClientName := 'pascal-redis-faa-smoke';
end;

{ ---------------------------------------------------------------------------
  Comandos basicos: o kernel generico ja alcanca qualquer comando do Redis.
  --------------------------------------------------------------------------- }
procedure ExercitaBasico(AConn: TRedisConnection);
var
  LReply: IRedisReply;
  LChave: string;
begin
  Secao('comandos basicos');
  LChave := Chave('str');

  Passo('PING responde PONG', AConn.Ping);

  LReply := AConn.Execute('ECHO', ['pascal-redis-faa']);
  Passo('ECHO devolve o mesmo texto',
    LReply.AsString = 'pascal-redis-faa', ' -> ' + LReply.AsString);

  LReply := AConn.Execute('SET', [LChave, 'valor']);
  Passo('SET responde +OK', LReply.AsString = 'OK', ' -> ' + LReply.AsString);

  LReply := AConn.Execute('GET', [LChave]);
  Passo('GET devolve o valor gravado',
    (LReply.Kind = rkBulkString) and (LReply.AsString = 'valor'),
    ' -> ' + LReply.AsString);

  // Nulo nao e' string vazia: e' a diferenca entre "a chave nao existe" e "a
  // chave existe e vale ''". A lib guarda essa diferenca (decisao do M1).
  LReply := AConn.Execute('GET', [Chave('nao-existe')]);
  Passo('GET de chave ausente devolve nulo', LReply.IsNull);
  Passo('nulo nao e string vazia disfarcada',
    LReply.IsNull and (LReply.AsString = ''));

  AConn.Execute('SET', [Chave('vazia'), '']);
  LReply := AConn.Execute('GET', [Chave('vazia')]);
  Passo('chave com valor vazio nao e nulo',
    (not LReply.IsNull) and (LReply.AsString = ''));

  LReply := AConn.Execute('EXISTS', [LChave]);
  Passo('EXISTS devolve 1', LReply.AsInteger = 1);

  AConn.Execute('DEL', [Chave('contador')]);
  AConn.Execute('INCR', [Chave('contador')]);
  LReply := AConn.Execute('INCRBY', [Chave('contador'), 41]);
  Passo('INCR + INCRBY chegam a 42', LReply.AsInteger = 42,
    ' -> ' + IntToStr(LReply.AsInteger));

  AConn.Execute('SET', [Chave('expira'), 'x', 'EX', 60]);
  LReply := AConn.Execute('TTL', [Chave('expira')]);
  Passo('SET ... EX 60 deixa TTL positivo',
    (LReply.AsInteger > 0) and (LReply.AsInteger <= 60),
    ' -> ' + IntToStr(LReply.AsInteger) + 's');

  LReply := AConn.Execute('DEL', [LChave]);
  Passo('DEL remove a chave', LReply.AsInteger = 1);
  LReply := AConn.Execute('EXISTS', [LChave]);
  Passo('EXISTS depois do DEL devolve 0', LReply.AsInteger = 0);
end;

{ ---------------------------------------------------------------------------
  Binario-seguro por contrato: bytes arbitrarios (CRLF, zero, 0xFF) e texto
  acentuado tem que voltar identicos. E' o teste que pega a conversao de
  codepage errada no FPC — o modo de falha silencioso da API de string.
  --------------------------------------------------------------------------- }
procedure ExercitaBinario(AConn: TRedisConnection);
var
  LReply: IRedisReply;
  LOriginal, LVolta: TBytes;
  LTexto: string;
begin
  Secao('binario e UTF-8');

  // CRLF no meio do valor: se a lib emitisse comando inline, isto quebraria o
  // comando em dois. E' por isso que so' o unified request protocol e' usado.
  LOriginal := MontaBytes([65, 13, 10, 66, 0, 255, 200, 66]);
  AConn.Execute('SET', [Chave('bin'), LOriginal]);
  LReply := AConn.Execute('GET', [Chave('bin')]);
  LVolta := LReply.AsBytes;
  Passo('valor binario com CRLF e zero volta identico',
    BytesIguais(LOriginal, LVolta),
    ' -> ' + IntToStr(Length(LVolta)) + ' bytes');

  LTexto := 'ação, coração e cedilha';
  AConn.Execute('SET', [Chave('utf8'), LTexto]);
  LReply := AConn.Execute('GET', [Chave('utf8')]);
  Passo('texto acentuado sobrevive ao round-trip',
    LReply.AsString = LTexto,
    ' -> ' + IntToStr(Length(LReply.AsBytes)) + ' bytes UTF-8');

  // Um valor grande exercita a leitura em varias passadas do socket: a
  // resposta nao cabe num recv so'.
  LTexto := StringOfChar('x', 200000);
  AConn.Execute('SET', [Chave('grande'), LTexto]);
  LReply := AConn.Execute('GET', [Chave('grande')]);
  Passo('bulk string de 200 KB volta inteira',
    Length(LReply.AsBytes) = 200000,
    ' -> ' + IntToStr(Length(LReply.AsBytes)) + ' bytes');
end;

{ ---------------------------------------------------------------------------
  Pipeline: N comandos numa escrita, N respostas na ordem.
  --------------------------------------------------------------------------- }
procedure ExercitaPipeline(AConn: TRedisConnection);
var
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
  LLista: IRedisReply;
begin
  Secao('pipeline');
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('DEL', [Chave('lista')]);
    LPipe.Queue('RPUSH', [Chave('lista'), 'a']);
    LPipe.Queue('RPUSH', [Chave('lista'), 'b']);
    LPipe.Queue('RPUSH', [Chave('lista'), 'c']);
    LPipe.Queue('LRANGE', [Chave('lista'), 0, -1]);
    LPipe.Queue('LLEN', [Chave('lista')]);

    LReplies := AConn.ExecutePipeline(LPipe);
    Passo('6 comandos, 6 respostas', Length(LReplies) = 6,
      ' -> ' + IntToStr(Length(LReplies)));
    if Length(LReplies) = 6 then
    begin
      Passo('as respostas vem na ordem enfileirada',
        (LReplies[1].AsInteger = 1) and (LReplies[3].AsInteger = 3));
      LLista := LReplies[4];
      Passo('LRANGE devolve os 3 itens',
        (LLista.Kind = rkArray) and (LLista.Count = 3) and
        (LLista[0].AsString = 'a') and (LLista[2].AsString = 'c'));
      Passo('LLEN fecha o lote', LReplies[5].AsInteger = 3);
    end;

    // Erro no meio do lote NAO aborta o lote nem levanta excecao: o servidor
    // executou todos, e o interessante e' saber qual falhou.
    LPipe.Clear;
    LPipe.Queue('SET', [Chave('str2'), 'texto']);
    LPipe.Queue('LPUSH', [Chave('str2'), 'x']);   // WRONGTYPE
    LPipe.Queue('GET', [Chave('str2')]);
    LReplies := AConn.ExecutePipeline(LPipe);
    Passo('erro no meio do lote nao levanta excecao', Length(LReplies) = 3);
    if Length(LReplies) = 3 then
    begin
      Passo('o item que falhou vem como erro',
        LReplies[1].IsError and (LReplies[1].ErrorCode = 'WRONGTYPE'),
        ' -> ' + LReplies[1].ErrorMessage);
      Passo('os comandos seguintes rodaram assim mesmo',
        LReplies[2].AsString = 'texto');
    end;

    LPipe.Clear;
    Passo('lote vazio nao vai ao servidor',
      Length(AConn.ExecutePipeline(LPipe)) = 0);
  finally
    LPipe.Free;
  end;
  Passo('a conexao segue sa depois do pipeline', AConn.IsUsable and AConn.Ping);
end;

{ ---------------------------------------------------------------------------
  Erro de servidor e' resposta valida: levanta ERedisReplyError, mas a conexao
  continua utilizavel. Erro de conexao e' outra coisa (ver ExercitaInvalidacao).
  --------------------------------------------------------------------------- }
procedure ExercitaErros(AConn: TRedisConnection);
var
  LReply: IRedisReply;
  LCodigo: string;
  LPegou: Boolean;
begin
  Secao('erros do servidor');
  AConn.Execute('SET', [Chave('str3'), 'texto']);

  LPegou := False;
  LCodigo := '';
  try
    AConn.Execute('LPUSH', [Chave('str3'), 'x']);
  except
    on E: ERedisReplyError do
    begin
      LPegou := True;
      LCodigo := E.Code;
    end;
  end;
  Passo('LPUSH em string levanta ERedisReplyError', LPegou);
  Passo('o codigo do erro e WRONGTYPE', LCodigo = 'WRONGTYPE',
    ' -> ' + LCodigo);
  Passo('a conexao continua utilizavel depois do erro',
    AConn.IsUsable and AConn.Ping);

  LReply := AConn.ExecuteRaw('LPUSH', [Chave('str3'), 'x']);
  Passo('ExecuteRaw devolve o erro em vez de levantar',
    LReply.IsError and (LReply.ErrorCode = 'WRONGTYPE'));

  LPegou := False;
  try
    AConn.Execute('COMANDOQUENAOEXISTE', ['a']);
  except
    on E: ERedisReplyError do
      LPegou := True;
  end;
  Passo('comando desconhecido levanta erro do servidor', LPegou);
end;

{ ---------------------------------------------------------------------------
  Comandos de servidor + o banco (SELECT), que vive na conexao e nao no
  servidor — e por isso a reconexao do M3 tera de replaya-lo.
  --------------------------------------------------------------------------- }
procedure ExercitaServidor(AConn: TRedisConnection);
var
  LReply: IRedisReply;
  LInfo: string;
begin
  Secao('servidor e banco');

  LReply := AConn.Execute('INFO', ['server']);
  LInfo := LReply.AsString;
  Passo('INFO server traz redis_version',
    Pos('redis_version', LInfo) > 0,
    ' -> ' + IntToStr(Length(LInfo)) + ' bytes');

  LReply := AConn.Execute('DBSIZE');
  Passo('DBSIZE responde um inteiro', LReply.Kind = rkInteger,
    ' -> ' + IntToStr(LReply.AsInteger) + ' chaves');

  LReply := AConn.Execute('CLIENT', ['GETNAME']);
  Passo('CLIENT SETNAME do handshake pegou',
    LReply.AsString = AConn.Params.ClientName, ' -> ' + LReply.AsString);

  AConn.Select(1);
  Passo('SELECT 1 atualiza Database', AConn.Database = 1);
  AConn.Execute('SET', [Chave('db1'), 'so no banco 1']);
  AConn.Select(0);
  Passo('SELECT 0 volta ao banco padrao', AConn.Database = 0);
  LReply := AConn.Execute('GET', [Chave('db1')]);
  Passo('a chave do banco 1 nao aparece no banco 0', LReply.IsNull);
  AConn.Select(1);
  AConn.Execute('DEL', [Chave('db1')]);
  AConn.Select(0);
end;

{ ---------------------------------------------------------------------------
  RESP3 (HELLO 3) numa conexao propria. O ponto do teste nao e' so' o
  handshake: e' que o mapa achatado do M1 faz HGETALL ter a MESMA forma nos
  dois protocolos, entao codigo de aplicacao nao ramifica por versao de RESP.
  --------------------------------------------------------------------------- }
procedure ExercitaResp3(AConn2: TRedisConnection);
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LResp2, LResp3: IRedisReply;
begin
  Secao('RESP3 (HELLO 3)');
  AConn2.Execute('DEL', [Chave('hash')]);
  AConn2.Execute('HSET', [Chave('hash'), 'campo1', 'v1', 'campo2', 'v2']);
  LResp2 := AConn2.Execute('HGETALL', [Chave('hash')]);

  LParams := Params;
  LParams.Protocol := rpRESP3;
  LParams.ClientName := 'pascal-redis-faa-smoke-resp3';
  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;
    Passo('HELLO 3 negociou RESP3',
      LConn.NegotiatedProtocol = rpRESP3);
    Passo('o servidor informou a versao no HELLO',
      LConn.ServerVersion <> '', ' -> ' + LConn.ServerVersion);
    Passo('o servidor informou o id da conexao', LConn.ServerId > 0,
      ' -> id ' + IntToStr(LConn.ServerId));

    LResp3 := LConn.Execute('HGETALL', [Chave('hash')]);
    Passo('HGETALL em RESP3 volta como mapa', LResp3.Kind = rkMap);
    Passo('RESP2 (array) e RESP3 (mapa) tem a mesma contagem achatada',
      LResp2.Count = LResp3.Count,
      ' -> ' + IntToStr(LResp2.Count) + ' x ' + IntToStr(LResp3.Count));
    Passo('ValueByKey funciona igual nos dois',
      (LResp2.ValueByKey('campo1') <> nil) and
      (LResp3.ValueByKey('campo1') <> nil) and
      (LResp2.ValueByKey('campo1').AsString = 'v1') and
      (LResp3.ValueByKey('campo1').AsString = 'v1'));

    // Double nativo do RESP3: em RESP2 o mesmo score chega como bulk string, e
    // AsDouble entrega o mesmo numero nos dois casos.
    LConn.Execute('DEL', [Chave('zset')]);
    LConn.Execute('ZADD', [Chave('zset'), 1.5, 'membro']);
    LResp3 := LConn.Execute('ZSCORE', [Chave('zset'), 'membro']);
    Passo('ZSCORE em RESP3 vem como double',
      (LResp3.Kind = rkDouble) and (Abs(LResp3.AsDouble - 1.5) < 1E-9),
      ' -> ' + RedisFormatDouble(LResp3.AsDouble));
    LConn.Execute('DEL', [Chave('zset')]);
    LConn.Close;
  finally
    LConn.Free;
  end;
end;

{ ---------------------------------------------------------------------------
  Read timeout: sem ele, um comando pendurado prende a conexao E a thread do
  chamador para sempre — o Redis nao tem heartbeat para detectar isso.
  --------------------------------------------------------------------------- }
procedure ExercitaTimeout;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LInicio: UInt64;
  LDecorrido: UInt64;
  LPegou: Boolean;
begin
  Secao('read timeout');
  LParams := Params;
  LParams.ReceiveTimeoutMs := 300;
  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;
    // BLPOP numa fila vazia espera ate' o timeout DELE (2 s); o do socket
    // (300 ms) estoura antes.
    LInicio := RedisTickMs;
    LPegou := False;
    try
      LConn.Execute('BLPOP', [Chave('fila-vazia'), 2]);
    except
      on E: ERedisTimeout do
        LPegou := True;
    end;
    LDecorrido := RedisTickMs - LInicio;
    Passo('BLPOP alem do timeout levanta ERedisTimeout', LPegou,
      ' -> ' + IntToStr(LDecorrido) + ' ms');
    Passo('e desiste antes dos 2 s do comando', LDecorrido < 1500);
    Passo('a conexao com resposta a caminho e invalidada',
      LConn.IsBroken and (not LConn.IsUsable));
  finally
    LConn.Free;
  end;
end;

{ ---------------------------------------------------------------------------
  Pool: empresta, reusa e descarta. E' o que a aplicacao segura de verdade —
  nao ha' canal no Redis, a unidade de concorrencia e' a conexao.
  --------------------------------------------------------------------------- }
procedure ExercitaPool;
var
  LPool: TRedisPool;
  LPoolParams: TRedisPoolParams;
  LPrimeira, LSegunda: TRedisConnection;
  LPegou: Boolean;
begin
  Secao('pool de conexoes');
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 2;
  LPoolParams.AcquireTimeoutMs := 200;
  LPool := TRedisPool.Create(Params, LPoolParams);
  try
    LPrimeira := LPool.Acquire;
    Passo('Acquire empresta uma conexao aberta', LPrimeira.IsOpen);
    LPrimeira.Execute('SET', [Chave('pool'), 'ok']);
    Passo('e o comando roda por ela',
      LPrimeira.Execute('GET', [Chave('pool')]).AsString = 'ok');

    LSegunda := LPool.Acquire;
    Passo('o pool abre a segunda quando as duas sao usadas ao mesmo tempo',
      (LSegunda <> nil) and (LPool.TotalCount = 2));

    LPegou := False;
    try
      LPool.Acquire;  // no teto e sem ninguem para devolver
    except
      on E: ERedisPoolExhausted do
        LPegou := True;
    end;
    Passo('no teto, Acquire desiste em vez de abrir socket sem limite', LPegou);

    LPool.Release(LPrimeira);
    Passo('Release devolve para o ocioso', LPool.IdleCount = 1);

    LPrimeira := LPool.Acquire;
    Passo('e a proxima ida reusa a ociosa, sem abrir outra',
      LPool.CreatedCount = 2, ' -> ' + IntToStr(LPool.CreatedCount) + ' abertas');

    LPrimeira.Execute('DEL', [Chave('pool')]);
    LPool.Release(LPrimeira);
    LPool.Release(LSegunda);
    Passo('devolvidas as duas, nenhuma fica emprestada',
      (LPool.InUseCount = 0) and (LPool.IdleCount = 2));
  finally
    LPool.Free;
  end;
end;

{ ---------------------------------------------------------------------------
  Invalidacao: conexao que perdeu o socket nao se conserta, nao reexecuta o
  comando em voo e recusa os proximos. E' a regra que o pool do M3 usa para
  decidir destruir em vez de devolver.
  --------------------------------------------------------------------------- }
procedure ExercitaInvalidacao;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LPegou: Boolean;
begin
  Secao('invalidacao de conexao');
  LConn := TRedisConnection.Create(Params);
  try
    LConn.Open;
    Passo('conexao nova nasce utilizavel', LConn.IsUsable);

    // Abort e' o que uma thread de timeout (M3) fara: derruba o socket por
    // fora, sem pegar o lock.
    LConn.Abort;
    Passo('depois do Abort a conexao esta invalidada',
      LConn.IsBroken and (not LConn.IsUsable));

    LPegou := False;
    try
      LConn.Execute('PING');
    except
      on E: ERedisConnectionLost do
        LPegou := True;
    end;
    Passo('comando em conexao invalidada levanta ERedisConnectionLost', LPegou);

    LPegou := False;
    try
      LConn.Open;
    except
      on E: ERedisConnectionLost do
        LPegou := True;
    end;
    Passo('conexao invalidada nao reabre (crie outra)', LPegou);
  finally
    LConn.Free;
  end;

  // Porta sem ninguem escutando: a falha vira ERedisConnectionLost com o
  // endereco na mensagem, nao uma excecao crua de socket que o chamador teria
  // de conhecer.
  LParams := Params;
  LParams.Port := 6399;
  LConn := TRedisConnection.Create(LParams);
  try
    LPegou := False;
    try
      LConn.Open;
    except
      on E: ERedisConnectionLost do
        LPegou := True;
    end;
    Passo('conectar em porta morta levanta ERedisConnectionLost', LPegou);
    Passo('e a conexao nao ficou aberta pela metade', not LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

{ ---------------------------------------------------------------------------
  Familias tipadas (M4): o mesmo kernel, agora com nome de comando, conversao
  de retorno e a ordem dos modificadores conferida pela lib em vez de pelo
  chamador.
  --------------------------------------------------------------------------- }
procedure ExercitaFamilias;
var
  LClient: TRedisClient;
  LOpc: TRedisSetOptions;
  LReply: IRedisReply;
  LMapa: IRedisReply;
  LItens: TRedisStringArray;
  LPares: TRedisScoreMemberArray;
  LPertence: TRedisBooleanArray;
  LChaveBloq, LValorBloq, LValor: string;
  LTtl, LRank: Int64;
  LScore: Double;
  LInicio: UInt64;
  LCursor: Int64;
  LVistas, I: Integer;
begin
  Secao('familias tipadas e TRedisClient');
  LClient := TRedisClient.Create(Params);
  try
    { strings }
    LClient.Strings.SetValue(Chave('f:str'), 'valor');
    Passo('Strings.SetValue/GetString',
      LClient.Strings.GetString(Chave('f:str')) = 'valor');
    Passo('chave ausente e nula, nao vazia',
      LClient.Strings.Get(Chave('f:naoexiste')).IsNull);
    Passo('TryGet separa ausente de vazio',
      not LClient.Strings.TryGet(Chave('f:naoexiste'), LValor));

    LOpc := RedisDefaultSetOptions;
    LOpc.Condition := scNotExists;
    LOpc.Expiry := seSeconds;
    LOpc.ExpiryValue := 60;
    LReply := LClient.Strings.SetWithOptions(Chave('f:lock'), 'token', LOpc);
    Passo('SET NX EX grava na primeira vez', not LReply.IsNull);
    LReply := LClient.Strings.SetWithOptions(Chave('f:lock'), 'outro', LOpc);
    Passo('e o NX barra a segunda (nulo, nao erro)', LReply.IsNull);

    Passo('Strings.Incr conta a partir do zero',
      LClient.Strings.Incr(Chave('f:cont')) = 1);
    Passo('IncrByFloat manda ponto decimal',
      Abs(LClient.Strings.IncrByFloat(Chave('f:saldo'), 1.5) - 1.5) < 0.0001);

    { keys }
    LTtl := LClient.Keys.Ttl(Chave('f:lock'));
    Passo('Keys.Ttl devolve o prazo do SET NX EX', (LTtl > 0) and (LTtl <= 60),
      ' -> ' + IntToStr(LTtl) + ' s');
    Passo('Keys.Ttl de chave ausente e -2',
      LClient.Keys.Ttl(Chave('f:naoexiste')) = REDIS_TTL_NO_KEY);
    Passo('Keys.KeyType', LClient.Keys.KeyType(Chave('f:str')) = 'string');
    Passo('Keys.Exists', LClient.Keys.Exists(Chave('f:str')));

    { hashes }
    LClient.Hashes.HSetMany(Chave('f:hash'), ['ip', '10.0.0.1', 'user', 'ana']);
    LMapa := LClient.Hashes.HGetAll(Chave('f:hash'));
    Passo('Hashes.HGetAll achatado, com acesso por campo',
      (LMapa.Count = 4) and (LMapa.ValueByKey('user').AsString = 'ana'));
    Passo('Hashes.HIncrBy',
      LClient.Hashes.HIncrBy(Chave('f:hash'), 'hits', 2) = 2);
    Passo('Hashes.HDel de campo ausente devolve False',
      not LClient.Hashes.HDel(Chave('f:hash'), 'nada'));

    { listas }
    LClient.Lists.RPushMany(Chave('f:lista'), ['a', 'b', 'c']);
    LItens := LClient.Lists.LRange(Chave('f:lista'), 0, -1);
    Passo('Lists.RPushMany mantem a ordem do array',
      (Length(LItens) = 3) and (LItens[0] = 'a') and (LItens[2] = 'c'));
    Passo('Lists.LMove move entre listas, atomico',
      LClient.Lists.LMove(Chave('f:lista'), Chave('f:proc'),
        leRight, leLeft).AsString = 'c');

    { bloqueante: sai por conexao FORA do pool comum, com o prazo do socket
      esticado para alem do prazo do comando }
    LInicio := RedisTickMs;
    Passo('Lists.BLPop acha o que ja esta na fila',
      LClient.Lists.BLPop([Chave('f:proc')], 5, LChaveBloq, LValorBloq) and
      (LValorBloq = 'c'));
    Passo('BLPop com fila vazia devolve False no prazo, sem erro',
      not LClient.Lists.BLPop([Chave('f:vazia')], 1, LChaveBloq, LValorBloq));
    Passo('e o bloqueante nao segurou o pool comum',
      (LClient.Pool.InUseCount = 0) and (RedisTickMs - LInicio < 5000));

    { conjuntos }
    LClient.Sets.SAddMany(Chave('f:set:a'), ['redis', 'pascal']);
    LClient.Sets.SAddMany(Chave('f:set:b'), ['redis', 'lazarus']);
    Passo('Sets.SAdd de membro repetido devolve False',
      not LClient.Sets.SAdd(Chave('f:set:a'), 'redis'));
    LPertence := LClient.Sets.SMIsMember(Chave('f:set:a'), ['redis', 'lazarus']);
    Passo('Sets.SMIsMember mapeia 0/1 na ordem pedida',
      (Length(LPertence) = 2) and LPertence[0] and (not LPertence[1]));
    LItens := LClient.Sets.SInter([Chave('f:set:a'), Chave('f:set:b')]);
    Passo('Sets.SInter resolve no servidor',
      (Length(LItens) = 1) and (LItens[0] = 'redis'));

    { sorted sets }
    LClient.ZSets.ZAddMany(Chave('f:zset'), [100, 'ana', 300, 'bob', 200, 'cida']);
    LItens := LClient.ZSets.ZRevRange(Chave('f:zset'), 0, 1);
    Passo('ZSets.ZRevRange devolve o topo do ranking',
      (Length(LItens) = 2) and (LItens[0] = 'bob') and (LItens[1] = 'cida'));
    LPares := LClient.ZSets.ZRangeWithScores(Chave('f:zset'), 0, -1);
    Passo('ZSets.ZRangeWithScores devolve membro e score',
      (Length(LPares) = 3) and (LPares[0].Member = 'ana') and
      (Abs(LPares[0].Score - 100) < 0.0001));
    LItens := LClient.ZSets.ZRangeByScore(Chave('f:zset'),
      RedisScoreBound(100, True), REDIS_SCORE_MAX);
    Passo('ZRangeByScore com extremo aberto exclui o limite',
      Length(LItens) = 2);
    Passo('ZSets.ZTryScore acha o membro',
      LClient.ZSets.ZTryScore(Chave('f:zset'), 'ana', LScore) and
      (Abs(LScore - 100) < 0.0001));
    Passo('e devolve False para quem nao esta no conjunto',
      not LClient.ZSets.ZTryScore(Chave('f:zset'), 'ninguem', LScore));
    Passo('ZSets.ZTryRank da a posicao 0-based crescente',
      LClient.ZSets.ZTryRank(Chave('f:zset'), 'ana', LRank) and (LRank = 0));

    { SCAN: cursor opaco, laco ate' voltar a zero }
    for I := 1 to 20 do
      LClient.Strings.SetValue(Chave('f:scan:' + IntToStr(I)), 'v');
    LVistas := 0;
    LCursor := 0;
    repeat
      LItens := LClient.Keys.Scan(LCursor, PREFIXO + 'f:scan:*', 5);
      Inc(LVistas, Length(LItens));
    until LCursor = 0;
    Passo('Keys.Scan varre o prefixo inteiro em varios passos',
      LVistas >= 20, ' -> ' + IntToStr(LVistas) + ' chaves');

    { faxina pela propria fachada }
    for I := 1 to 20 do
      LClient.Keys.Del(Chave('f:scan:' + IntToStr(I)));
    LClient.Keys.DelMany([Chave('f:str'), Chave('f:lock'), Chave('f:cont'),
      Chave('f:saldo'), Chave('f:hash'), Chave('f:lista'), Chave('f:proc'),
      Chave('f:set:a'), Chave('f:set:b'), Chave('f:zset')]);
    Passo('e a faxina pela fachada nao deixa chave para tras',
      not LClient.Keys.Exists(Chave('f:zset')));
  finally
    LClient.Free;
  end;
end;

{ ---------------------------------------------------------------------------
  TLS (M5): so roda com --tls. As secoes anteriores ja passaram inteiras por
  cima da criptografia; o que sobra aqui e o que SO da para verificar mexendo
  nos parametros — cert recusado, porta trocada e carga grande o bastante para
  atravessar varios registros TLS.
  --------------------------------------------------------------------------- }
procedure ExercitaTls;
var
  LParams: TRedisParams;
  LConn: TRedisConnection;
  LPool: TRedisPool;
  LPoolParams: TRedisPoolParams;
  LPrimeira, LSegunda: TRedisConnection;
  LGrande, LVolta: TBytes;
  LPegou: Boolean;
  LMensagem: string;
  LInicio: UInt64;
  I: Integer;
begin
  Secao('TLS');

  { Qual motor cifrou de fato. O nome sai da compilacao; o DETALHE so existe no
    OpenSSL, que publica versao e o caminho da biblioteca que carregou de
    verdade no primeiro handshake — a informacao que resolve o dia em que a
    maquina tem tres OpenSSL instalados e o errado venceu. O SChannel nao tem
    equivalente porque nao ha o que escolher: e o proprio Windows. }
  Passo('o backend TLS que cifrou se identifica',
    Pos(RedisTlsBackendName, RedisTlsBackendInfo) > 0,
    ' -> ' + RedisTlsBackendInfo);

  { Certificado self-signed com a verificacao LIGADA tem de ser recusado. Se
    este passo falhar, TlsVerifyPeer nao esta chegando ao backend — e o modo
    seguro estaria aceitando qualquer certificado em silencio, que e a pior
    falha possivel nesta camada. }
  LParams := Params;
  LParams.TlsVerifyPeer := True;
  LConn := TRedisConnection.Create(LParams);
  try
    LPegou := False;
    LMensagem := '';
    try
      LConn.Open;
    except
      on E: ERedisTls do
      begin
        LPegou := True;
        LMensagem := E.Message;
      end;
    end;
    Passo('cert self-signed com TlsVerifyPeer=True e recusado', LPegou,
      ' -> ' + LMensagem);
    Passo('e a conexao nao ficou aberta pela metade', not LConn.IsOpen);
  finally
    LConn.Free;
  end;

  { Handshake TLS contra o listener de TEXTO CLARO. O servidor le o ClientHello
    como comando inline e nunca responde um ServerHello, entao quem desata o no
    e o read timeout — sem ele a thread ficaria pendurada para sempre. O prazo
    vai curto aqui so para o teste nao custar os 5 s do padrao. }
  LParams := Params;
  LParams.Port := PORT;
  LParams.ReceiveTimeoutMs := 700;
  LConn := TRedisConnection.Create(LParams);
  try
    LInicio := RedisTickMs;
    LPegou := False;
    LMensagem := '';
    try
      LConn.Open;
    except
      on E: ERedisTimeout do
      begin
        LPegou := True;
        LMensagem := E.Message;
      end;
    end;
    Passo('TLS contra porta plain desiste por timeout, nao trava', LPegou,
      ' -> ' + IntToStr(RedisTickMs - LInicio) + ' ms');
    // A mensagem tem de apontar a porta, senao o usuario vai procurar defeito
    // no certificado quando o que errou foi o numero da porta.
    Passo('e a mensagem aponta a porta como causa provavel',
      Pos('texto claro', LMensagem) > 0);
  finally
    LConn.Free;
  end;

  { O caso simetrico: conexao em texto claro contra o listener TLS. Aqui o Open
    PASSA — em RESP2 sem senha, sem nome e no banco 0 o handshake nao emite
    byte nenhum, entao nao ha o que dar errado ainda. A falha aparece no
    primeiro comando, quando o servidor descarta a conexao por lixo. Nao da
    para fazer melhor sem gastar um round-trip em TODA conexao do pool, e esse
    preco nao vale a pena. }
  LParams := RedisDefaultParams;
  LParams.Host := HOST;
  LParams.Port := PORT_TLS;
  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;
    Passo('plain na porta TLS: o Open passa (o handshake RESP2 nao fala)',
      LConn.IsOpen);
    LPegou := False;
    try
      LConn.Execute('PING');
    except
      on E: ERedisConnectionLost do
        LPegou := True;
    end;
    Passo('e o primeiro comando cai com ERedisConnectionLost', LPegou);
  finally
    LConn.Free;
  end;

  { Carga maior que um registro TLS (~16 KB). E o teste que quebra envelope
    mal-feito: o valor volta truncado ou embaralhado quando a remontagem de
    registros esta errada, e nao ha erro nenhum pelo caminho. }
  LConn := TRedisConnection.Create(Params);
  try
    LConn.Open;
    SetLength(LGrande, 512 * 1024);
    for I := 0 to High(LGrande) do
      LGrande[I] := Byte(I and $FF);   // padrao que denuncia bloco fora de ordem
    LConn.Execute('SET', [Chave('tls-grande'), LGrande]);
    LVolta := LConn.Execute('GET', [Chave('tls-grande')]).AsBytes;
    Passo('bulk de 512 KB atravessa varios registros TLS intacto',
      BytesIguais(LGrande, LVolta),
      ' -> ' + IntToStr(Length(LVolta)) + ' bytes');
    LConn.Execute('DEL', [Chave('tls-grande')]);
  finally
    LConn.Free;
  end;

  { Duas conexoes cifradas vivas ao mesmo tempo: prova que o handshake nao
    depende de estado global de processo (credencial do SChannel, contexto do
    OpenSSL) e que o pool funciona igual por cima do TLS. }
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 2;
  LPool := TRedisPool.Create(Params, LPoolParams);
  try
    LPrimeira := LPool.Acquire;
    LSegunda := LPool.Acquire;
    Passo('duas conexoes TLS simultaneas pelo pool',
      LPrimeira.Ping and LSegunda.Ping and (LPool.TotalCount = 2));
    LPool.Release(LPrimeira);
    LPool.Release(LSegunda);
  finally
    LPool.Free;
  end;
end;

procedure Limpa(AConn: TRedisConnection);
var
  LPipe: TRedisPipeline;
begin
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('DEL', [Chave('str'), Chave('str2'), Chave('str3'),
      Chave('vazia'), Chave('contador'), Chave('expira'), Chave('bin'),
      Chave('utf8'), Chave('grande'), Chave('lista'), Chave('hash')]);
    AConn.ExecutePipeline(LPipe);
  finally
    LPipe.Free;
  end;
end;

procedure Executa;
var
  LConn: TRedisConnection;
  LTextoModo, LTextoTls: string;
begin
  if GUsaTls then
  begin
    LTextoModo := ' (TLS)';
    LTextoTls := ' (exercitado: tudo abaixo passa cifrado)';
  end
  else
  begin
    LTextoModo := ' (texto claro)';
    LTextoTls := ' (nao exercitado; rode com --tls)';
  end;

  WriteLn('pascal-redis-faa :: smoke test M5');
  WriteLn('  alvo ......... ', HOST, ':', Params.Port, LTextoModo);
  WriteLn('  compilador ... ', {$IFDEF FPC} 'FPC ' + {$I %FPCVERSION%} {$ELSE} 'Delphi' {$ENDIF});
  WriteLn('  backend TLS .. ', RedisTlsBackendName, LTextoTls);

  LConn := TRedisConnection.Create(Params);
  try
    LConn.Open;
    Passo('conecta e faz o handshake', LConn.IsOpen);
    try
      ExercitaBasico(LConn);
      ExercitaBinario(LConn);
      ExercitaPipeline(LConn);
      ExercitaErros(LConn);
      ExercitaServidor(LConn);
      ExercitaResp3(LConn);
    finally
      Limpa(LConn);
    end;
    LConn.Close;
    Passo('encerra a conexao', not LConn.IsOpen);
  finally
    LConn.Free;
  end;

  if GUsaTls then
    ExercitaTls;
  ExercitaTimeout;
  ExercitaPool;
  ExercitaFamilias;
  ExercitaInvalidacao;
end;

begin
  {$IFDEF FPC}
  // Console FPC puro: sem isto os literais acentuados (o teste de UTF-8)
  // seriam transcodificados errado e falhariam por motivo alheio ao Redis.
  SetMultiByteConversionCodePage(CP_UTF8);
  {$ELSE}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  GUsaTls := LeArgumentoTls;
  try
    Executa;
  except
    on E: Exception do
    begin
      WriteLn('  [FAIL] excecao ', E.ClassName, ': ', E.Message);
      Inc(GFalhas);
    end;
  end;
  WriteLn;
  if GFalhas = 0 then
    WriteLn('RESULTADO: PASS (', GPassos, ' passos)')
  else
    WriteLn('RESULTADO: FAIL (', GFalhas, ' de ', GPassos, ' passos)');
  ExitCode := Ord(GFalhas <> 0);
end.
