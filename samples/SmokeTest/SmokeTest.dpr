program SmokeTest;

{ Smoke test da pascal-redis-faa contra um Redis real (docker/docker-compose.yml).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src SmokeTest.dpr

  ESTADO (M4): alem da conexao, do pool e dos timeouts, a lib ja tem o
  TRedisClient e as fachadas tipadas por familia. Este programa exercita tudo
  de ponta a ponta — handshake, Execute generico, pipeline, erro de servidor,
  binario, RESP2 x RESP3, read timeout, pool, invalidacao de conexao e os
  comandos de chaves, strings, hashes, listas, conjuntos e sorted sets,
  inclusive um bloqueante. O que ainda nao existe: TLS e o argumento --tls
  (M5), MULTI/EXEC e EVAL (M6), pub/sub (M7), streams (M8).

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
  PREFIXO = 'pascal-redis-faa:smoke:';

var
  GFalhas: Integer = 0;
  GPassos: Integer = 0;

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
  Result := RedisDefaultParams;
  Result.Host := HOST;
  Result.Port := PORT;
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
begin
  WriteLn('pascal-redis-faa :: smoke test M4');
  WriteLn('  alvo ......... ', HOST, ':', PORT);
  WriteLn('  compilador ... ', {$IFDEF FPC} 'FPC ' + {$I %FPCVERSION%} {$ELSE} 'Delphi' {$ENDIF});
  WriteLn('  backend TLS .. ', RedisTlsBackendName, ' (nao exercitado ate o M5)');

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
