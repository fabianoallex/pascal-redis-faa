# pascal-redis-faa

Cliente **Redis** (protocolo RESP2/RESP3) para **Free Pascal/Lazarus e Delphi**, numa
única codebase, escrito do zero. Licença MIT.

> **Status: v1 completo (M8 — streams).** Dá para conectar, autenticar, executar
> **qualquer** comando do Redis, usar pipeline, trabalhar com um pool de conexões com
> timeout de verdade, chamar os comandos de Keys, Strings, Hashes, Lists, Sets, ZSets e
> Streams pela fachada tipada — inclusive os bloqueantes — em RESP2 ou RESP3, cifrar tudo
> isso com TLS, usar `MULTI`/`EXEC`/`WATCH` e scripts Lua com cache de SHA, publicar e
> assinar canais, e montar fila de trabalho com consumer groups. O que falta são os
> demais samples GUI (M9) e a validação em Linux (M10). O roadmap completo está em `CLAUDE.md`.

Projeto irmão da [`pascal-amqp-faa`](../pascal-amqp-faa) (cliente AMQP 0-9-1) e da
`pascal-pipes-faa` (IPC), com as mesmas regras: codebase dual FPC 3.2.2 + Delphi 12,
sem dependências externas, TLS nativo em Windows (SChannel) e OpenSSL opt-in em
qualquer plataforma.

## O que o v1 entrega

| Área | Conteúdo |
|---|---|
| Núcleo | codec RESP2/RESP3, conexão, pool de conexões, timeouts, reconexão |
| Comandos | Keys, Strings, Hashes, Lists, Sets, ZSets, Streams |
| Avançado | pipelining, `MULTI`/`EXEC`/`WATCH`, scripting (`EVAL`/`EVALSHA` com cache de SHA) |
| Mensageria | Pub/Sub (conexão dedicada) e Streams com consumer groups |
| Segurança | TLS via SChannel (Windows) ou OpenSSL (`-dREDIS_OPENSSL`, qualquer plataforma) |

Comando de servidor (`PING`, `INFO`, `CONFIG`, `DBSIZE`) ainda não tem fachada própria —
sai pelo `Execute` genérico, que alcança qualquer comando do Redis.

**Fora do v1:** Redis Cluster (redirects `MOVED`/`ASK`), Sentinel e client-side
caching (`CLIENT TRACKING`).

## Compatibilidade de servidor

A lib fala RESP, não depende da implementação: funciona com **Redis**, **Valkey**,
**KeyDB** e **Dragonfly**. O `docker/docker-compose.yml` usa `redis:7.2-alpine` por
padrão.

Este projeto não é afiliado à Redis Ltd.; "Redis" é marca de seus respectivos
detentores e aparece aqui apenas para identificar o protocolo com que a lib fala.

## Servidor de desenvolvimento

```
cd docker
docker compose up -d                    # Redis em localhost:6379
docker compose exec redis redis-cli ping
```

Para o listener TLS (6380), gere os certs conforme `docker/certs/README.md` e suba
com o override:

```
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
```

## Uso

```pascal
uses
  SysUtils, Redis.Types, Redis.Connection;

var
  LParams: TRedisParams;
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LRespostas: TRedisReplyArray;
begin
  LParams := RedisDefaultParams;      // localhost:6379, banco 0, RESP2
  LParams.ClientName := 'minha-app';  // aparece no CLIENT LIST do servidor

  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;

    LConn.Execute('SET', ['usuario:1:nome', 'Fabiano', 'EX', 3600]);
    WriteLn(LConn.Execute('GET', ['usuario:1:nome']).AsString);

    // Chave ausente devolve nulo — que NÃO é o mesmo que string vazia.
    if LConn.Execute('GET', ['usuario:2:nome']).IsNull then
      WriteLn('não existe');

    // Pipeline: N comandos numa ida e volta só.
    LPipe := TRedisPipeline.Create;
    try
      LPipe.Queue('INCR', ['visitas']);
      LPipe.Queue('LPUSH', ['eventos', 'login']);
      LPipe.Queue('LLEN', ['eventos']);
      LRespostas := LConn.ExecutePipeline(LPipe);
      WriteLn('visitas: ', LRespostas[0].AsInteger);
    finally
      LPipe.Free;
    end;
  finally
    LConn.Free;   // o Close também fecha; o destrutor cobre o caminho de exceção
  end;
end.
```

### Fachadas tipadas por família

`TRedisClient` é o objeto que a aplicação segura: pool por dentro, comandos tipados por
fora. Não há `Connect` — a conexão abre no primeiro comando.

```pascal
uses
  SysUtils, Redis.Types, Redis.Client, Redis.Commands, Redis.Commands.Strings;

var
  LClient: TRedisClient;
  LOpcoes: TRedisSetOptions;
  LSessao: IRedisReply;
  LTopo: TRedisStringArray;
  LJson, LToken, LValor: string;
begin
  LClient := TRedisClient.Create(RedisDefaultParams);
  try
    LClient.Strings.SetEx('cache:usuario:1', 3600, LJson);

    // Ausente e vazio são coisas diferentes: TryGet devolve a presença.
    if LClient.Strings.TryGet('cache:usuario:1', LValor) then
      WriteLn(LValor);

    // Lock distribuído: SET NX PX devolve nulo quando outro já tem a posse.
    LOpcoes := RedisDefaultSetOptions;
    LOpcoes.Condition := scNotExists;      // NX
    LOpcoes.Expiry := seMilliseconds;      // PX
    LOpcoes.ExpiryValue := 30000;
    if not LClient.Strings.SetWithOptions('lock:pedido:7', LToken, LOpcoes).IsNull then
      WriteLn('a posse é minha');

    LClient.Hashes.HSetMany('sessao:42', ['ip', '10.0.0.1', 'user', 'ana']);
    LSessao := LClient.Hashes.HGetAll('sessao:42');
    WriteLn(LSessao.ValueByKey('user').AsString);

    LClient.ZSets.ZAdd('ranking', 1500, 'fabiano');
    LTopo := LClient.ZSets.ZRevRange('ranking', 0, 9);   // top 10

    LClient.Keys.Expire('sessao:42', 900);
  finally
    LClient.Free;
  end;
end.
```

Cada comando pega uma conexão do pool e a devolve antes de retornar, o que torna o mesmo
`TRedisClient` seguro para várias threads. Quando a sequência **precisa** da mesma conexão
(`SELECT`, e mais adiante `MULTI`/`WATCH`), amarre um cliente a uma conexão emprestada:

```pascal
LConn := LClient.Acquire;
try
  LDedicado := TRedisClient.CreateOnConnection(LConn);
  try
    ...            // tudo pela MESMA conexão
  finally
    LDedicado.Free;
  end;
finally
  LClient.Release(LConn);
end;
```

Convenção de retorno, válida em todas as famílias: escalar vira tipo nativo (`Boolean`,
`Int64`, `Double`, `string`); resposta que **pode ser nula** vira `IRedisReply` ou tem um
par `TryXxx` com `out`, porque "chave ausente" e "chave que vale zero" não podem virar o
mesmo valor; lista vira `TRedisStringArray`. Chaves, campos e valores entram como
`TRedisArg`, que aceita `string` e `TBytes` na **mesma** assinatura — a API é
binário-segura sem sobrecarga duplicada.

### Comandos bloqueantes

`BLPOP`, `BRPOP` e `BLMOVE` não saem do pool comum: um worker esperando 30 s por uma
tarefa seguraria uma conexão de que as outras threads precisam e, pior, morreria de
timeout de socket antes de o comando terminar — deixando a resposta a caminho para
contaminar a próxima conexão. A fachada os manda por um pool separado, com o read timeout
esticado para além do prazo do comando.

```pascal
// False não é erro: é o worker ocioso. Com várias chaves, LChave diz de qual veio.
while not GParar do
  if LClient.Lists.BLPop(['fila:alta', 'fila:baixa'], 5, LChave, LTarefa) then
    Processa(LChave, LTarefa);
```

Prazo zero significa esperar para sempre — só faz sentido em thread dedicada, porque a
única forma de cancelar é derrubar a conexão.

### Pool de conexões

O Redis não tem canal: uma conexão processa um comando por vez. A unidade de concorrência é
a conexão, e o que a aplicação segura é o **pool**.

```pascal
uses
  Redis.Types, Redis.Connection, Redis.Pool;

var
  LPool: TRedisPool;
  LConn: TRedisConnection;
begin
  LPool := TRedisPool.Create(RedisDefaultParams);   // teto de 10 conexões
  try
    LConn := LPool.Acquire;
    try
      LConn.Execute('INCR', ['visitas']);
    finally
      LPool.Release(LConn);   // devolver SEMPRE, inclusive em exceção
    end;
  finally
    LPool.Free;
  end;
end;
```

O pool descarta (em vez de reciclar) toda conexão que voltar invalidada, **suja** — sobrou
byte no buffer, o que contaminaria o próximo comando — ou com o banco trocado por um
`SELECT`. Conexão parada há mais que `HealthCheckAfterIdleMs` leva um `PING` antes de ser
emprestada, porque quem derruba conexão ociosa é o servidor e o cliente não é avisado. No
teto, `Acquire` espera uma devolução e depois levanta `ERedisPoolExhausted`, em vez de abrir
socket sem limite até o servidor recusar todo mundo.

Não existe "retomar" uma conexão morta: o que o pool faz é abrir outra, e o handshake dela
replaya `HELLO`/`AUTH`, `CLIENT SETNAME` e `SELECT`. O comando que estava em voo **não** é
repetido — `INCR` e `LPUSH` não são idempotentes.

### Timeouts

`ReceiveTimeoutMs` e `SendTimeoutMs` (5 s por padrão) viram `SO_RCVTIMEO`/`SO_SNDTIMEO` no
socket. Estourado o prazo, o comando levanta `ERedisTimeout` **e a conexão é invalidada** —
a resposta atrasada ainda pode chegar, e reciclar essa conexão entregaria essa resposta ao
comando seguinte. Sem esse timeout, um servidor que emudece prende a conexão e a thread que
chamou para sempre: o Redis não tem heartbeat.

Comandos bloqueantes precisam de um timeout maior que o do próprio comando. A fachada já
cuida disso (seção "Comandos bloqueantes"); quem for direto na conexão usa
`TRedisConnection.SetReceiveTimeout` numa conexão fora do pool.

`Execute` alcança qualquer comando, presente ou futuro. As fachadas tipadas por família são
uma camada de conveniência por cima — nunca um pré-requisito, e nunca um limite: um comando
que a lib ainda não modelou continua a uma linha de distância.

**Binário por contrato.** Argumento `TBytes` vai byte a byte para o fio e `AsBytes`
devolve o valor cru; as sobrecargas `string` passam por UTF-8. Um valor com CRLF, zero ou
0xFF no meio sobrevive ao round-trip.

**Erros.** Erro do servidor (`WRONGTYPE`, `NOSCRIPT`, `NOAUTH`…) levanta
`ERedisReplyError`, que traz o `Code` pronto para testar, e a conexão continua utilizável.
Erro de I/O levanta `ERedisConnectionLost` e **invalida** a conexão: o comando em voo não é
reexecutado, porque `INCR`, `LPUSH` e `SETNX` não são idempotentes — a decisão de repetir é
de quem chamou. `ExecuteRaw` devolve o erro como resposta, sem levantar, e o pipeline nunca
levanta por erro de servidor (cada item pode ser um erro).

**RESP3** é opt-in: `LParams.Protocol := rpRESP3` faz o handshake com `HELLO 3`. O código da
aplicação não muda — um `HGETALL` tem a mesma forma nos dois protocolos, porque o mapa do
RESP3 é guardado achatado.

### TLS

O backend é escolhido em **compilação**, nunca em runtime — um cliente que tenta um motor
e cai para outro acaba usando, na máquina do usuário, um backend diferente do que foi
testado:

| Build | Backend | Onde |
|---|---|---|
| padrão | SChannel (SSPI) | só Windows, sem DLL para distribuir |
| `-dREDIS_OPENSSL` | OpenSSL (libssl/libcrypto) | qualquer plataforma, carregado dinamicamente |
| nenhum dos dois | — | `UseTls` levanta `ERedisTls` explicando como habilitar |

`RedisTlsBackendName` diz qual motor este build tem; `RedisTlsBackendInfo` acrescenta o
detalhe de runtime que só o OpenSSL fornece — versão e o caminho da biblioteca que
realmente carregou, que é o que resolve o dia em que a máquina tem três OpenSSL
instalados e o errado venceu.

```pascal
uses
  Redis.Types, Redis.Client;

var
  LParams: TRedisParams;
begin
  LParams := RedisDefaultTlsParams;   // porta 6380 E UseTls, de uma vez
  LParams.TlsVerifyPeer := False;     // só em desenvolvimento (cert self-signed)

  LClient := TRedisClient.Create(LParams);
```

**TLS no Redis não é upgrade em banda.** Não existe `STARTTLS`: o servidor abre um
listener separado (`--tls-port`), e a conexão nasce cifrada ou não nasce. Por isso
`RedisDefaultTlsParams` troca a porta *junto* com o `UseTls` — ligar um sem o outro manda
um ClientHello para o listener de texto claro, que o lê como comando inline e nunca
responde. Nesse caso o que salva é o read timeout, e a lib levanta `ERedisTimeout`
**nomeando a troca de porta** como causa provável: a pista natural seria procurar defeito
no certificado, e o erro está no número da porta.

**`TlsVerifyPeer := False` aceita qualquer certificado** — mantém a cifragem e joga fora a
defesa contra man-in-the-middle. A lib não oferece nenhum atalho que já venha com a
validação desligada: `RedisDefaultTlsParams` mantém `TlsVerifyPeer := True`, e quem
precisa baixá-la escreve a linha acima. É proposital — assim a decisão aparece no diff de
quem a tomou e num `grep` de quem for auditar, em vez de atravessar para produção
escondida num nome de função amigável.

Para exercitar contra o servidor de desenvolvimento, gere os certs (ver
`docker/certs/README.md`), suba o override e rode o smoke test cifrado:

```
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d
SmokeTest.exe --tls
```

`--tls` não é um modo com meia dúzia de passos extras: a bateria **inteira** passa a rodar
por cima da criptografia, que é onde um envelope TLS mal-feito aparece — bulk de 512 KB
atravessando vários registros TLS, pipeline numa escrita só, timeout de socket no meio de
um registro.

### Transações (`MULTI`/`EXEC`/`WATCH`)

A palavra engana: uma transação do Redis garante que **ninguém intercala comando no meio
do bloco** — e só isso. Ela não desfaz nada quando um comando falha, e não deixa ler nada
lá dentro. É esse segundo buraco que o `WATCH` preenche, por fora do bloco:

```pascal
LTx := LClient.BeginTransaction;      // empresta uma conexão do pool
try
  LTx.Watch(['saldo']);                                            // vigia
  LSaldo := LTx.Connection.Execute('GET', ['saldo']).AsInteger;    // lê (fora do bloco)
  if LSaldo < 100 then
    Exit;                                                          // decide
  LTx.Queue('SET', ['saldo', LSaldo - 100]);                       // enfileira
  if not LTx.TryCommit(LRespostas) then
    ; // alguém mexeu em 'saldo' entre o WATCH e o EXEC: recomece o ciclo
finally
  LTx.Free;                            // devolve a conexão ao pool
end;
```

`TryCommit` devolvendo **False não é erro** — é o funcionamento normal do check-and-set sob
concorrência, e a resposta certa quase sempre é repetir o ciclo. (`Commit` existe para
quando não há `WATCH`: ele levanta `ERedisTransactionAborted` no lugar.)

**Não há rollback.** Se um comando do bloco falhar com `WRONGTYPE`, os outros rodaram e
ficaram gravados; o erro vem como item `rkError` no array de respostas e `Commit` **não**
levanta por isso. Levantar faria você concluir que nada foi gravado, quando quase tudo foi.
Para tudo-ou-nada de verdade, use um script Lua.

O bloco inteiro sai numa **única ida e volta** (`MULTI` + comandos + `EXEC` num pipeline),
e não N+2. Como o `MULTI` só sai no commit, `Discard` nem precisa falar com o servidor — o
que ele manda é o `UNWATCH`. E o `try/finally` não é decoração: enquanto a transação viver,
aquela conexão está fora de circulação, porque `WATCH` é estado **da conexão**.

### Scripting (`EVAL`/`EVALSHA`)

Um script Lua é a única atomicidade real do Redis: ele lê, decide e escreve numa passagem
só, e nada mais roda no servidor enquanto isso — com a contrapartida óbvia de que **script
demorado trava o servidor inteiro**.

O exemplo que não existe sem script é o release de lock distribuído:

```pascal
const
  LIBERA =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then' + sLineBreak +
    '  return redis.call("DEL", KEYS[1])' + sLineBreak +
    'else' + sLineBreak +
    '  return 0' + sLineBreak +
    'end';

// 1 quando o lock era seu e foi liberado; 0 quando já era de outra pessoa.
LClient.Scripting.Run(LIBERA, ['lock:pedido:7'], [LMeuToken]).AsInteger;
```

Fazer isso com `GET` seguido de `DEL` é a corrida clássica: entre os dois, o lock pode
expirar e ser tomado por outro — e o `DEL` apagaria o lock **dele**.

`Run` cuida do cache de SHA sozinha: calcula o SHA-1 localmente (sem round-trip), manda
`EVAL` na estreia — o servidor executa **e** guarda — e `EVALSHA` da segunda em diante,
trocando kilobytes de Lua por 40 bytes. Se o servidor tiver esquecido o script
(`SCRIPT FLUSH`, restart, failover para uma réplica), ele responde `NOSCRIPT` e a lib
reenvia o `EVAL` sozinha: o cache é uma otimização que **se autocorrige**, nunca uma
suposição sobre o estado do servidor. `Eval`, `EvalSha`, `ScriptLoad`, `ScriptExists` e
`ScriptFlush` continuam disponíveis para quem quiser controlar o ciclo à mão.

### Pub/Sub

Publicar é um comando comum — sai por qualquer conexão do pool:

```pascal
uses
  Redis.Types, Redis.Client, Redis.Commands.PubSub;

// Devolve quantos assinantes receberam NAQUELE instante. Zero quer dizer que a
// mensagem evaporou: pub/sub não guarda nada para quem chegar depois.
LClient.PubSub.Publish('noticias', 'saiu a edição de hoje');
```

Assinar é outra história: em RESP2 o `SUBSCRIBE` sequestra a conexão, então o assinante
tem conexão **dedicada**, fora do pool, e uma thread lendo o que o servidor manda.

```pascal
uses
  SysUtils, Redis.Types, Redis.Client, Redis.PubSub;

type
  TOuvinte = class
    procedure Chegou(ASender: TObject; const AMensagem: TRedisPubSubMessage);
  end;

procedure TOuvinte.Chegou(ASender: TObject; const AMensagem: TRedisPubSubMessage);
begin
  // Roda na THREAD DE LEITURA, uma mensagem por vez, na ordem em que chegaram.
  // Numa aplicação GUI, marshale para a main thread (TThread.Queue).
  WriteLn(AMensagem.Channel, ': ', AMensagem.Text);
end;

var
  LSub: TRedisSubscriber;
begin
  LSub := LClient.CreateSubscriber;      // mesmos parâmetros de conexão do cliente
  try
    LSub.OnMessage := LOuvinte.Chegou;
    LSub.Start;                          // abre a conexão e sobe a thread

    // Só volta depois que o servidor confirmou: publicar na linha seguinte não
    // é corrida.
    LSub.Subscribe(['noticias', 'alertas']);
    LSub.PSubscribe(['noticias.*']);     // glob, casado pelo servidor

    ...                                  // a aplicação segue a vida
  finally
    LSub.Free;                           // para a thread e fecha a conexão
  end;
end;
```

O que a lib garante e o que não garante:

- **Ordem, sim.** O `OnMessage` roda na thread de leitura, uma mensagem por vez. É por
  isso que **callback lento segura o socket**: trabalho pesado deve ir para uma fila da
  aplicação. Exceção que escape do callback vai para o `OnError` e não derruba a conexão.
- **Entrega, não.** Mensagem publicada enquanto o assinante estava fora do ar está
  perdida — sem fila, sem replay. Quem precisa de entrega garantida usa Streams com
  consumer group (seção seguinte).
- **Reconexão, sim** (`AutoReconnect`, ligado por padrão): a conexão volta e as
  assinaturas são reenviadas, com `OnDisconnected`/`OnReconnected` avisando. O que se
  perdeu no intervalo continua perdido.

Em **RESP2**, com assinatura ativa, a conexão só aceita comandos de assinatura, `PING`,
`RESET` e `QUIT` — e a lib recusa os outros **antes** de irem ao fio, com uma mensagem que
diz o que fazer. Em **RESP3** (`LParams.Protocol := rpRESP3`) as mensagens chegam por um
tipo próprio (push) e a mesma conexão continua servindo comando comum:

```pascal
LSub.Subscribe(['noticias']);
LSub.Execute('SET', ['ultima-leitura', '2026-08-23']);   // só vale em RESP3
```

`PUBSUB CHANNELS`, `NUMSUB` e `NUMPAT` respondem pelo lado de quem publica
(`LClient.PubSub.ActiveChannels`, `CountSubscribers`, `NumPatterns`) — úteis para
diagnóstico, não para lógica de aplicação: a resposta envelhece no caminho de volta.

### Streams e consumer groups

Stream é o único tipo do Redis com **entrega confiável**. A diferença para o pub/sub cabe
numa frase: ali, mensagem publicada sem assinante no ar evapora; aqui, ela fica gravada, e
o consumer group ainda registra quem a recebeu e se ela foi confirmada.

Gravar e ler é um log append-only com id crescente:

```pascal
uses
  Redis.Types, Redis.Client, Redis.Commands.Streams;

var
  LId: string;
  LEntradas: TRedisStreamEntryArray;
begin
  // '*' pede o próximo id ao servidor: <ms>-<sequência>, sempre crescente.
  LId := LClient.Streams.XAdd('eventos', ['tipo', 'venda', 'valor', '199.90']);

  // MAXLEN ~ 1000 mantém "mil e pouco" entradas — muito mais barato que o
  // corte exato, e é o que se usa em produção.
  LClient.Streams.XAddMaxLen('eventos', 1000, True, ['tipo', 'login']);

  LEntradas := LClient.Streams.XRange('eventos',
    REDIS_STREAM_MIN_ID, REDIS_STREAM_MAX_ID);
  WriteLn(LEntradas[0].FieldValue('tipo'));    // campos por nome
end;
```

A fila de trabalho é o consumer group. Cada entrada vai para **um** consumidor do grupo, e
fica pendente (na PEL, a *pending entries list*) até o `XACK`:

```pascal
var
  LDados: TRedisStreamDataArray;
  I: Integer;
begin
  // MKSTREAM cria a chave se ela ainda não existe — o caso de todo consumidor
  // que sobe antes do produtor. TryCreate devolve False se o grupo já existia,
  // então todo worker pode chamar isto na subida sem try/except.
  LClient.Streams.XGroupTryCreate('eventos', 'processadores', '0', True);

  // '>' é o que nunca foi entregue a ninguém do grupo. BLOCK é em
  // MILISSEGUNDOS (o timeout do BLPOP é em segundos — não confunda), e sai por
  // uma conexão fora do pool comum.
  LDados := LClient.Streams.XReadGroupBlocking('processadores', 'worker-1',
    ['eventos'], [REDIS_STREAM_NEW], 5000, 10);

  if Length(LDados) > 0 then
    for I := 0 to High(LDados[0].Entries) do
    begin
      Processa(LDados[0].Entries[I]);
      // Sem XACK a entrada fica pendente para sempre: é o vazamento clássico
      // de consumer group.
      LClient.Streams.XAck('eventos', 'processadores',
        [LDados[0].Entries[I].Id]);
    end;
end;
```

Worker que morre no meio deixa a entrada na PEL, e é isso que torna a entrega confiável.
Recolher o trabalho abandonado é um comando só:

```pascal
var
  LProximo: string;
  LEntradas: TRedisStreamEntryArray;
begin
  // Reivindica o que está parado há mais de 60 s. O mínimo de ociosidade é a
  // proteção contra dois workers processando a mesma entrada: quem está
  // trabalhando há 200 ms não é roubado.
  LEntradas := LClient.Streams.XAutoClaim('eventos', 'processadores',
    'worker-2', 60000, '0-0', 100, LProximo);
  // LProximo é cursor, com a mecânica do SCAN: repita até voltar '0-0'.
end;
```

Três detalhes que a lib absorve e que costumam morder:

- **`XREAD`/`XREADGROUP` mudam de forma entre RESP2 e RESP3** (lista de pares contra
  mapa). A fachada devolve sempre `TRedisStreamDataArray`, então a aplicação não ramifica
  por protocolo. Chave sem novidade **não aparece** na resposta — use
  `RedisFindStreamData` para achar pelo nome, nunca pela posição da chamada.
- **Entrada pode vir sem campos.** `XDEL` tira do stream mas não da PEL: reler a PEL
  alcança ids que já não existem, e aí `Fields` é `nil` (`IsDeleted`). Não é erro — é o
  estado normal de uma pendência sobre entrada apagada.
- **`'>'` contra `'0'`.** `XREADGROUP` com `REDIS_STREAM_NEW` pede o que nunca foi
  entregue e cria pendência; com `REDIS_STREAM_PENDING` relê a PEL **deste** consumidor,
  que é como um worker retoma o próprio trabalho depois de reiniciar. Trocar um pelo outro
  é o erro clássico.

`XPendingSummary`/`XPendingRange` respondem quem está devendo o quê (com ociosidade e
contador de entregas — contador alto denuncia mensagem venenosa, que derruba todo worker
que a pega), e `XInfoStream`/`XInfoGroups`/`XInfoConsumers` entregam a introspecção do
servidor como mapa achatado.

## Build

**FPC (linha de comando):**

```
fpc -Fusrc -Fisrc -FEbuild -FUbuild samples\SmokeTest\SmokeTest.dpr
```

**Lazarus:** `lazbuild packages\pascal_redis_faa.lpk` e depois os projetos; o
build mode `openssl` (`lazbuild -B --build-mode=openssl <proj>.lpi`) troca o
backend TLS.

**Delphi:** abrir `Redis.groupproj` no IDE (a Community Edition não compila por
linha de comando).

## Testes unitários

Não precisam de servidor. O codec RESP é exercitado sobre uma fonte de bytes em
memória, que entrega a resposta em pedaços de tamanho controlado para reproduzir
leituras parciais de rede; a conexão inteira (handshake, `Execute`, pipeline,
invalidação) sobe sobre um servidor falso, também em memória. A lógica do pool
(teto, reuso, descarte, poda por ociosidade, health check reprovando) roda sobre
esse mesmo servidor falso. As fachadas por família são verificadas nas duas metades que
importam: os bytes que foram para o fio (a ordem dos modificadores de `SET`, `ZADD` e
`ZRANGEBYSCORE` não é livre) e a conversão da resposta (nulo que não pode virar `''`,
`WITHSCORES` mudando de forma entre RESP2 e RESP3). As transações e o scripting entram
aqui também: o `EXEC` nulo, o comando recusado na fila e o `NOSCRIPT` são estados que o
servidor real quase nunca produz sob demanda, mas que o servidor falso entrega de graça.
O pub/sub tem um servidor falso que **responde** — ele interpreta o `SUBSCRIBE` e devolve
a confirmação —, porque sem diálogo não há como testar confirmação de assinatura, ordem
das mensagens, callback que levanta nem queda de conexão. Os streams voltam ao roteiro
fixo: o que interessa ali é a montagem do comando (`MAXLEN ~` antes do id, `STREAMS` como
último modificador, `BLOCK` em milissegundos) e a leitura de respostas que o servidor real
raramente produz sob demanda — entrada apagada que sobrou na PEL, `XAUTOCLAIM` sem a
lista de apagados do Redis 6.2.

**FPC/Lazarus (FPCUnit):**

```
lazbuild -B tests\Unit\fpc\RedisUnitTestsFpc.lpi
tests\Unit\fpc\RedisUnitTestsFpc.exe --all --format=plain
```

**Delphi (DUnitX):** abrir `Redis.groupproj` e compilar `Redis.UnitTests`.

## Testes de integração

Precisam do container de pé (seção "Servidor de desenvolvimento"). Cobrem o que não dá para
verificar em memória: read timeout de socket de verdade, conexão derrubada pelo servidor
(`CLIENT KILL`), conexão contaminada por resposta atrasada, várias threads dividindo o
mesmo pool e uma bateria por família de comandos contra o servidor real — incluindo um
`BLPOP` com prazo maior que o read timeout e a prova de que `HGETALL` e `ZRANGE
WITHSCORES` devolvem o mesmo resultado em RESP2 e em RESP3. O pub/sub entra com a
difusão para vários assinantes e com a reconexão: a conexão do assinante é derrubada por
`CLIENT KILL` e quem confirma que as assinaturas voltaram é o **servidor**, pelo
`PUBSUB NUMSUB`, não a contabilidade do cliente. Os streams entram com o ciclo inteiro de
uma fila de trabalho: dois consumidores do mesmo grupo repartindo as entradas, a pendência
que sobrevive ao worker que morreu e volta por `XAUTOCLAIM`, o `XCLAIM` que **não** rouba
trabalho em andamento e o `XREAD BLOCK` com prazo maior que o read timeout do socket.

**TLS fica de fora daqui de propósito:** esta suíte tem de valer com só o
`docker-compose.yml` de pé, sem certificado nenhum. Quem exercita TLS é o smoke
test, com `--tls`.

```
lazbuild -B tests\Integration\fpc\RedisIntegrationTestsFpc.lpi
tests\Integration\fpc\RedisIntegrationTestsFpc.exe --all --format=plain
```

No Delphi, abrir `Redis.groupproj` e compilar `Redis.IntegrationSuite`.

Sem parâmetros os executáveis abrem a interface gráfica com a árvore de testes.

As duas suítes têm a mesma cobertura e o corpo dos testes é idêntico — o
`tests\Unit\Redis.DUnitXCompat.pas` existe para isso. Toda mudança em uma vai
para a outra na mesma sessão.

## Smoke test

Precisa do container de pé (seção anterior).

```
cd samples\SmokeTest
lazbuild SmokeTest.lpi
SmokeTest.exe          # 151 passos, texto claro (6379)
SmokeTest.exe --tls    # 160 passos, tudo cifrado (6380) + a seção de TLS
```

Sai com código 0 se todos os passos passarem, e 2 se receber um argumento
desconhecido — um `--tsl` digitado errado rodaria em texto claro com cara de
sucesso, que é justo o contrário do que se queria provar.

O `--tls` precisa do override e dos certs (seção "TLS"). Vale rodá-lo nos dois
backends: o build normal usa SChannel, e `lazbuild -B --build-mode=openssl
SmokeTest.lpi` troca para OpenSSL.

## Samples GUI (VCL/LCL)

Cada sample é um app único que compila nos dois mundos a partir do **mesmo
fonte** — `lazbuild <Nome>.lpi` no FPC/Lazarus, ou abrir o `.dproj` no Delphi.
Todos trazem host/porta/senha/banco e um checkbox TLS (marcá-lo troca a porta
junto, porque no Redis TLS não é upgrade em banda: é outra porta).

O princípio dos samples é um mecanismo por app, com as variações do padrão como
controles dentro da própria tela — e cada um traz a armadilha do padrão num
botão, para dar para reproduzi-la em vez de só ler sobre ela.

### `CacheAsideVcl`

O ciclo completo: miss → busca na fonte lenta → grava com TTL → hit → expira.
A fonte lenta é simulada por um `Sleep` de tempo editável, porque o que importa
é o ciclo, não de onde o dado vem.

```
cd samples\CacheAsideVcl
lazbuild CacheAsideVcl.lpi
CacheAsideVcl.exe
```

Três coisas para experimentar na tela:

- **A armadilha do `SET`.** O botão "Regravar SEM KEEPTTL" grava o valor com
  `Strings.SetValue` e o rótulo "TTL restante" pula para **SEM PRAZO**: um `SET`
  simples apaga o prazo da chave, e um cache regravado assim vira vazamento
  permanente — o tipo de bug que só aparece semanas depois, como memória que não
  para de crescer. O botão ao lado faz a mesma gravação com `Expiry := seKeepTtl`
  e o prazo continua correndo.
- **A debandada.** "Consultar 5x ao mesmo tempo" dispara cinco consultas
  concorrentes na mesma chave fria: as cinco erram o cache e as cinco vão à
  fonte. O cache-aside sozinho não protege contra isso, e o log mostra os cinco
  misses no mesmo milissegundo.
- **Expiração em massa.** Aquecer 20 chaves com TTL fixo faz a barra despencar
  de 20 para 0 de uma vez (todas expiram no mesmo segundo, e o lote inteiro de
  misses cai na fonte junto); com jitter — o mesmo TTL ±20% sorteado por chave —
  a barra desce aos poucos.

O sample também estabelece o padrão de threads dos samples GUI deste projeto:
como o Redis é request/response, quem chama `Strings.Get` bloqueia a própria
thread, então **nada de rede acontece na thread da UI**. Toda operação é um
`TRedisWorkItem` no `RedisPool`, e o resultado volta para a tela por um marshal
descartável + `TThread.Queue`.

### `LockDistribuidoVcl`

Lock distribuído com `SET NX PX` e um token de posse. **Aviso na própria
tela**: isto é um lock de **instância única** (um Redis só, sem coordenação
entre vários) — não é Redlock. Se o processo dono morrer ou pausar por mais
tempo que o TTL, a garantia cai junto.

```
cd samples\LockDistribuidoVcl
lazbuild LockDistribuidoVcl.lpi
LockDistribuidoVcl.exe
```

Três coisas para experimentar na tela:

- **A armadilha do `DEL`.** Adquira o lock com um TTL curto, ligue "Simular
  concorrente tentando o lock" (que fica tentando `SET NX PX` a cada 1s) e
  espere o seu TTL vencer — o concorrente assume o lock com o próprio token.
  Clicando "Liberar com DEL direto (ARMADILHA)" achando que ainda é seu, o log
  mostra o lock do concorrente sendo apagado por engano. O botão ao lado,
  "Liberar com script (compare-and-delete)", faz a mesma liberação com um Lua
  que só apaga se o token no servidor ainda for o seu — é a única forma segura.
- **A variante sem token.** Desmarcar "Gerar token de posse" grava a chave com
  um valor fixo, e o botão "Liberar com script" fica desabilitado: sem token
  não há o que comparar, e a única saída é o `DEL` às cegas.
- **Renovação de posse.** "Renovar automaticamente" roda a cada 1s um script
  que só estende o TTL (`PEXPIRE`) se o token no servidor ainda for o seu —
  mantém a posse sem deixar o prazo estourar. Se alguém já tomou o lock, a
  renovação falha sozinha e a tela desmarca o checkbox.

## Licença

MIT — Copyright (c) 2026 Fabiano Arndt. Ver `LICENSE`.
