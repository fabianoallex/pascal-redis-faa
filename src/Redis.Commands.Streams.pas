unit Redis.Commands.Streams;

{ Comandos de stream: um log append-only com id crescente, leitura por faixa e
  — o que nenhum outro tipo do Redis oferece — CONSUMER GROUPS.

  E' o unico tipo com entrega confiavel. A diferenca para o pub/sub do M7 cabe
  numa frase: no pub/sub, mensagem publicada sem assinante no ar evapora; num
  stream, ela fica gravada, e o consumer group ainda registra quem a recebeu e
  se ela foi confirmada. O preco e' que alguem precisa confirmar (XACK) e
  alguem precisa cuidar do que ficou pendente (XPENDING/XCLAIM/XAUTOCLAIM) —
  trabalho que o pub/sub nao tem porque nao promete nada.

  Vocabulario minimo, porque os nomes do Redis nao ajudam:

  - **Entrada**: um id ('1700000000000-0') mais um dicionario campo/valor
    ACHATADO. O id e' <ms>-<sequencia>, e a sequencia desempata dentro do mesmo
    milissegundo. '*' pede ao servidor que gere o proximo.
  - **PEL** (pending entries list): por grupo, a lista do que foi ENTREGUE e
    ainda nao foi confirmado. E' o que faz a entrega confiavel: worker que
    morre no meio deixa a entrada na PEL, e outro a reivindica com XCLAIM ou
    XAUTOCLAIM.
  - **'>' contra um id**: XREADGROUP com '>' pede o que NUNCA foi entregue a
    ninguem do grupo (e cria pendencia); com '0' pede a PEL DESTE consumidor,
    que e' como um worker retoma o proprio trabalho depois de reiniciar. Sao os
    dois modos, e trocar um pelo outro e' o erro classico.

  Duas armadilhas de protocolo que esta unit absorve:

  1. **XREAD/XREADGROUP mudam de forma entre RESP2 e RESP3.** Em RESP2 vem uma
     lista de pares [chave, entradas]; em RESP3, um MAPA chave -> entradas, que
     o leitor guarda achatado. Os metodos devolvem sempre
     TRedisStreamDataArray, entao a aplicacao nao ramifica por protocolo — a
     mesma solucao do WITHSCORES no M4.

  2. **Entrada pode vir SEM campos.** Ler a PEL alcanca ids que ja' foram
     apagados do stream (XDEL ou trim), e ai' o servidor devolve os campos como
     nulo. Nesses casos TRedisStreamEntry.Fields e' nil — nao um dicionario
     vazio —, porque "apagada do stream" e "gravada sem campo" sao coisas
     diferentes, e a segunda o Redis nem permite.

  Os bloqueantes (XREAD/XREADGROUP com BLOCK) seguem a mesma regra do M4: saem
  por TRedisCommandExecutor.ExecuteBlocking, numa conexao fora do pool comum,
  com o read timeout esticado alem do BLOCK. ATENCAO a unidade: o BLOCK do
  Redis e' em MILISSEGUNDOS, ao contrario do timeout de BLPOP, que e' em
  segundos. Os metodos daqui recebem milissegundos, como o comando. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

const
  /// Id que pede ao servidor para gerar o proximo (o normal num XADD).
  REDIS_STREAM_AUTO_ID = '*';

  /// Extremos de faixa do XRANGE/XREVRANGE/XPENDING: o menor e o maior id
  /// possiveis. XRange(k, REDIS_STREAM_MIN_ID, REDIS_STREAM_MAX_ID) e' o
  /// stream inteiro.
  REDIS_STREAM_MIN_ID = '-';
  REDIS_STREAM_MAX_ID = '+';

  /// XREAD: "so' o que chegar DEPOIS desta chamada". Sem isto (passando um id
  /// concreto) a leitura comeca do historico.
  REDIS_STREAM_LAST = '$';

  /// XREADGROUP: entradas que nunca foram entregues a ninguem do grupo. Cria
  /// pendencia na PEL do consumidor.
  REDIS_STREAM_NEW = '>';

  /// XREADGROUP: a PEL DESTE consumidor, do inicio. E' como um worker retoma
  /// o que ja' havia recebido e nao confirmou antes de morrer. Nao cria
  /// pendencia nova.
  REDIS_STREAM_PENDING = '0';

type
  /// Uma entrada do stream: id mais campos.
  TRedisStreamEntry = record
    /// '<ms>-<sequencia>', sempre crescente dentro da chave.
    Id: string;

    /// Campos ACHATADOS — campo em 0, valor em 1, campo em 2... — igual ao que
    /// o HGETALL devolve. Use FieldValue para acesso por nome.
    ///
    /// **nil quando a entrada foi apagada do stream** e sobrou apenas na PEL
    /// do grupo (XDEL ou trim depois da entrega). Nao e' erro: e' o que
    /// XREADGROUP com '0', XCLAIM e XAUTOCLAIM devolvem nesse caso.
    Fields: IRedisReply;

    /// Valor de um campo pelo nome; '' quando o campo nao existe. Se a
    /// diferenca entre ausente e vazio importa, use TryFieldValue.
    function FieldValue(const AName: string): string;
    /// Valor sem interpretar codepage — para campo que carrega binario.
    function FieldBytes(const AName: string): TBytes;
    /// False quando o campo nao existe (ou a entrada nao tem campos).
    function TryFieldValue(const AName: string; out AValue: string): Boolean;
    /// Quantos PARES campo/valor a entrada tem (nao o Count achatado).
    function FieldCount: Integer;
    /// True quando a entrada sobrou so' na PEL — ver Fields.
    function IsDeleted: Boolean;
  end;

  TRedisStreamEntryArray = array of TRedisStreamEntry;

  /// O que um XREAD/XREADGROUP devolveu para UMA das chaves pedidas. Chave sem
  /// novidade simplesmente nao aparece no resultado — por isso o array pode
  /// ser menor que a lista de chaves da chamada, e por isso o nome vem junto.
  TRedisStreamData = record
    Key: string;
    Entries: TRedisStreamEntryArray;
  end;

  TRedisStreamDataArray = array of TRedisStreamData;

  /// Um consumidor no resumo do XPENDING.
  TRedisPendingConsumer = record
    Name: string;
    Count: Int64;
  end;

  TRedisPendingConsumerArray = array of TRedisPendingConsumer;

  /// Resumo do XPENDING (a forma curta, sem faixa): quantas entradas o grupo
  /// tem pendentes, entre que ids, e como se distribuem pelos consumidores.
  ///
  /// Com Count = 0, MinId e MaxId vem vazios e Consumers vazio — o servidor
  /// responde nulo nesses campos, e nulo aqui e' "nao ha' pendencia".
  TRedisPendingSummary = record
    Count: Int64;
    MinId: string;
    MaxId: string;
    Consumers: TRedisPendingConsumerArray;
  end;

  /// Uma pendencia na forma longa do XPENDING (com faixa).
  TRedisPendingEntry = record
    Id: string;
    /// Quem recebeu a entrada e ainda nao confirmou.
    Consumer: string;
    /// Ha' quanto tempo, em milissegundos, ela foi entregue pela ultima vez.
    /// E' o criterio do XCLAIM: so' reivindica quem passou do minimo.
    IdleMs: Int64;
    /// Quantas vezes ja' foi entregue. Contador que so' sobe — numero alto
    /// denuncia mensagem venenosa, que derruba todo worker que a pega.
    DeliveryCount: Int64;
  end;

  TRedisPendingEntryArray = array of TRedisPendingEntry;

  /// Comandos de stream (XADD, XRANGE, XREADGROUP, XACK...).
  TRedisStreamsCommands = class(TRedisCommandFamily)
  public
    { --- Escrita --- }

    /// XADD com id gerado pelo servidor ('*'). Devolve o id gravado.
    function XAdd(const AKey: TRedisArg;
      const AFieldValues: array of TRedisArg): string;

    /// XADD com id escolhido por quem chama. O id tem de ser MAIOR que o
    /// ultimo da chave — o servidor recusa id menor ou igual, e e' isso que
    /// garante a ordem total do log.
    function XAddId(const AKey, AId: TRedisArg;
      const AFieldValues: array of TRedisArg): string;

    /// XADD com trim por tamanho no mesmo comando: grava e mantem no maximo
    /// AMaxLen entradas.
    ///
    /// AApproximate (o '~') deixa o servidor parar no limite do no' interno em
    /// vez de contar exato. E' MUITO mais barato e e' o que se usa em
    /// producao: 'MAXLEN ~ 1000' quer dizer "mil e pouco", nunca menos que
    /// mil. Exato ('=') num stream grande pode custar caro num comando so'.
    function XAddMaxLen(const AKey: TRedisArg; AMaxLen: Int64;
      AApproximate: Boolean; const AFieldValues: array of TRedisArg): string;

    /// XADD com trim por id: descarta o que for menor que AMinId. E' o corte
    /// por TEMPO, ja' que o id comeca com o milissegundo.
    function XAddMinId(const AKey, AMinId: TRedisArg; AApproximate: Boolean;
      const AFieldValues: array of TRedisArg): string;

    /// XLEN: quantas entradas a chave tem. 0 se a chave nao existe.
    function XLen(const AKey: TRedisArg): Int64;

    /// XDEL. Devolve quantos ids existiam de fato.
    ///
    /// Apagar do STREAM nao tira da PEL de grupo nenhum: a pendencia continua
    /// la', e quem a reivindicar recebe a entrada sem campos (ver
    /// TRedisStreamEntry.Fields). Quem tira da PEL e' o XACK.
    function XDel(const AKey: TRedisArg; const AIds: array of TRedisArg): Int64;

    /// XTRIM MAXLEN. Devolve quantas entradas sairam. Ver XAddMaxLen sobre o
    /// '~'.
    function XTrimMaxLen(const AKey: TRedisArg; AMaxLen: Int64;
      AApproximate: Boolean = True): Int64;
    /// XTRIM MINID: descarta tudo abaixo do id.
    function XTrimMinId(const AKey, AMinId: TRedisArg;
      AApproximate: Boolean = True): Int64;

    { --- Leitura por faixa --- }

    /// XRANGE, com os dois extremos INCLUIDOS. ACount < 0 nao limita.
    ///
    /// Os extremos aceitam id completo ('1700000000000-0'), so' o
    /// milissegundo ('1700000000000', que vale a entrada inteira daquele ms),
    /// os coringas REDIS_STREAM_MIN_ID/MAX_ID e o prefixo '(' para extremo
    /// ABERTO (Redis 6.2+) — que e' como se pagina sem repetir a ultima
    /// entrada. Use RedisStreamIdExclusive para montar.
    function XRange(const AKey, AStart, AStop: TRedisArg;
      ACount: Int64 = -1): TRedisStreamEntryArray;

    /// XREVRANGE: do mais novo para o mais velho. ATENCAO: aqui os argumentos
    /// sao (fim, inicio) — a ordem inversa da do XRange, como no comando.
    function XRevRange(const AKey, AStop, AStart: TRedisArg;
      ACount: Int64 = -1): TRedisStreamEntryArray;

    { --- Leitura sequencial (sem grupo) --- }

    /// XREAD sem bloquear: o que houver DEPOIS de cada id, agora.
    ///
    /// AKeys e AIds andam juntos, um id por chave, na mesma ordem — o comando
    /// os separa em dois blocos ('STREAMS k1 k2 id1 id2') e trocar a
    /// quantidade rende um erro obscuro do servidor; esta unit confere antes.
    ///
    /// Chave sem novidade nao aparece no resultado: o array devolvido pode ser
    /// menor que AKeys, ou vazio.
    function XRead(const AKeys, AIds: array of TRedisArg;
      ACount: Int64 = -1): TRedisStreamDataArray;

    /// XREAD com BLOCK, em MILISSEGUNDOS (nao segundos — ver o cabecalho da
    /// unit). ABlockMs = 0 espera para sempre, o que so' faz sentido em thread
    /// dedicada.
    ///
    /// Sai por conexao fora do pool comum. Array vazio significa que o prazo
    /// venceu sem novidade — o caso NORMAL de um leitor ocioso, nao um erro.
    function XReadBlocking(const AKeys, AIds: array of TRedisArg;
      ABlockMs: Int64; ACount: Int64 = -1): TRedisStreamDataArray;

    { --- Consumer groups --- }

    /// XGROUP CREATE. AId e' de onde o grupo comeca a enxergar: '$' so' o que
    /// chegar depois, '0' o stream inteiro desde o comeco.
    ///
    /// AMkStream cria a chave se ela nao existir; sem isto, criar grupo em
    /// stream que ainda nao recebeu nada falha — o que acontece toda vez que o
    /// consumidor sobe antes do produtor.
    ///
    /// Levanta ERedisReplyError (BUSYGROUP) se o grupo ja' existir. Para o uso
    /// idempotente — todo worker chamando na subida —, use XGroupTryCreate.
    procedure XGroupCreate(const AKey, AGroup, AId: TRedisArg;
      AMkStream: Boolean = False);

    /// XGROUP CREATE que trata BUSYGROUP como resposta, e nao como falha:
    /// devolve False quando o grupo ja' existia. Qualquer outro erro sobe.
    ///
    /// E' a forma de chamar na subida de cada worker sem que N-1 deles
    /// precisem de try/except para ignorar o normal.
    function XGroupTryCreate(const AKey, AGroup, AId: TRedisArg;
      AMkStream: Boolean = False): Boolean;

    /// XGROUP DESTROY. True se o grupo existia. Descarta a PEL junto — o que
    /// estava pendente deixa de estar, sem ter sido processado.
    function XGroupDestroy(const AKey, AGroup: TRedisArg): Boolean;

    /// XGROUP SETID: move o ponto de leitura do grupo. Nao mexe na PEL.
    procedure XGroupSetId(const AKey, AGroup, AId: TRedisArg);

    /// XGROUP CREATECONSUMER. True se o consumidor foi criado agora.
    /// Raramente necessario: o consumidor nasce sozinho no primeiro
    /// XREADGROUP.
    function XGroupCreateConsumer(const AKey, AGroup,
      AConsumer: TRedisArg): Boolean;

    /// XGROUP DELCONSUMER. Devolve quantas pendencias o consumidor levava
    /// consigo — que voltam a ser de ninguem, e portanto SOMEM da PEL sem
    /// terem sido processadas. Reivindique antes (XAutoClaim) se elas
    /// importam.
    function XGroupDelConsumer(const AKey, AGroup,
      AConsumer: TRedisArg): Int64;

    /// XREADGROUP sem bloquear.
    ///
    /// AIds segue a regra dos dois modos: REDIS_STREAM_NEW ('>') pede o que
    /// nunca foi entregue ao grupo e CRIA pendencia; REDIS_STREAM_PENDING
    /// ('0') rele' a PEL DESTE consumidor, sem criar pendencia nova.
    ///
    /// ANoAck manda o NOACK, que entrega sem registrar pendencia — mais rapido
    /// e sem rede de seguranca: e' abrir mao justamente do que o grupo
    /// oferece.
    function XReadGroup(const AGroup, AConsumer: TRedisArg;
      const AKeys, AIds: array of TRedisArg; ACount: Int64 = -1;
      ANoAck: Boolean = False): TRedisStreamDataArray;

    /// XREADGROUP com BLOCK, em MILISSEGUNDOS. 0 espera para sempre.
    /// Sai por conexao fora do pool comum, como o XReadBlocking.
    ///
    /// Bloquear so' funciona com '>': com '0' o comando le' a PEL e responde
    /// na hora, mesmo vazia.
    function XReadGroupBlocking(const AGroup, AConsumer: TRedisArg;
      const AKeys, AIds: array of TRedisArg; ABlockMs: Int64;
      ACount: Int64 = -1; ANoAck: Boolean = False): TRedisStreamDataArray;

    /// XACK: confirma o processamento e TIRA da PEL. Devolve quantos ids
    /// estavam mesmo pendentes para este grupo.
    ///
    /// Sem XACK a entrada fica pendente para sempre e reaparece em todo
    /// XAUTOCLAIM — e' o vazamento classico de consumer group.
    function XAck(const AKey, AGroup: TRedisArg;
      const AIds: array of TRedisArg): Int64;

    { --- Pendencias --- }

    /// XPENDING na forma curta: o resumo do grupo.
    function XPendingSummary(const AKey,
      AGroup: TRedisArg): TRedisPendingSummary;

    /// XPENDING na forma longa: as pendencias da faixa, uma a uma.
    /// AConsumer vazio nao filtra por consumidor.
    function XPendingRange(const AKey, AGroup, AStart, AStop: TRedisArg;
      ACount: Int64; const AConsumer: string = ''): TRedisPendingEntryArray;

    /// XPENDING com IDLE: so' o que esta' parado ha' pelo menos AMinIdleMs.
    /// E' a pergunta que interessa antes de reivindicar — pendencia recente
    /// pode estar sendo processada agora mesmo.
    function XPendingIdle(const AKey, AGroup: TRedisArg; AMinIdleMs: Int64;
      const AStart, AStop: TRedisArg; ACount: Int64;
      const AConsumer: string = ''): TRedisPendingEntryArray;

    /// XCLAIM: transfere ids pendentes para AConsumer, desde que parados ha'
    /// pelo menos AMinIdleMs. Devolve as entradas transferidas — e id que nao
    /// estava pendente (ou nao passou do minimo) simplesmente nao vem.
    ///
    /// O minimo de ociosidade e' a protecao contra dois workers processando a
    /// mesma entrada: quem esta' trabalhando ha' 200 ms nao e' roubado por um
    /// XCLAIM de 60 s.
    function XClaim(const AKey, AGroup, AConsumer: TRedisArg;
      AMinIdleMs: Int64;
      const AIds: array of TRedisArg): TRedisStreamEntryArray;

    /// XCLAIM JUSTID: transfere e devolve so' os ids, sem trazer os campos.
    /// Mais barato, e nao incrementa o contador de entregas.
    function XClaimJustId(const AKey, AGroup, AConsumer: TRedisArg;
      AMinIdleMs: Int64; const AIds: array of TRedisArg): TRedisStringArray;

    /// XAUTOCLAIM (Redis 6.2+): varre a PEL a partir de AStartId e reivindica
    /// de uma vez o que estiver parado ha' AMinIdleMs. E' o XPENDING + XCLAIM
    /// num comando so', e e' o que um worker roda periodicamente para recolher
    /// o trabalho de quem morreu.
    ///
    /// ANextId e' o cursor do proximo passo, com a mesma mecanica do SCAN:
    /// repita ate' ele voltar '0-0'. ACount < 0 usa o padrao do servidor.
    function XAutoClaim(const AKey, AGroup, AConsumer: TRedisArg;
      AMinIdleMs: Int64; const AStartId: TRedisArg; ACount: Int64;
      out ANextId: string): TRedisStreamEntryArray; overload;

    /// XAUTOCLAIM que tambem entrega a terceira parte da resposta (Redis 7+):
    /// os ids que estavam na PEL mas ja' nao existem no stream. O servidor os
    /// remove da PEL sozinho; a lista existe para quem quiser registrar.
    function XAutoClaim(const AKey, AGroup, AConsumer: TRedisArg;
      AMinIdleMs: Int64; const AStartId: TRedisArg; ACount: Int64;
      out ANextId: string;
      out ADeletedIds: TRedisStringArray): TRedisStreamEntryArray; overload;

    { --- Introspeccao --- }

    /// XINFO STREAM: mapa ACHATADO com length, last-generated-id,
    /// first-entry... Use ValueByKey. Mesma forma em RESP2 e RESP3.
    function XInfoStream(const AKey: TRedisArg): IRedisReply;
    /// XINFO GROUPS: uma entrada por grupo, cada uma um mapa achatado com
    /// name, consumers, pending, last-delivered-id, lag.
    function XInfoGroups(const AKey: TRedisArg): IRedisReply;
    /// XINFO CONSUMERS: uma entrada por consumidor do grupo, com name,
    /// pending, idle e (Redis 7+) inactive.
    function XInfoConsumers(const AKey, AGroup: TRedisArg): IRedisReply;
  end;

/// Monta '<ms>-<sequencia>'.
function RedisStreamId(AMilliseconds, ASequence: Int64): string;

/// Quebra um id em milissegundo e sequencia. False se o texto nao tiver a
/// forma <numero>-<numero>. Aceita o '(' de extremo aberto, ignorando-o.
function RedisTryParseStreamId(const AId: string;
  out AMilliseconds, ASequence: Int64): Boolean;

/// Prefixa o '(' que torna o extremo ABERTO num XRANGE/XREVRANGE/XPENDING
/// (Redis 6.2+): RedisStreamIdExclusive('5-1') = '(5-1', que significa
/// "depois de 5-1". E' o jeito de paginar sem repetir a ultima entrada lida.
function RedisStreamIdExclusive(const AId: string): string;

/// Converte a resposta de um XRANGE/XCLAIM (lista de [id, campos]) em
/// entradas. Nulo vira lista vazia.
function RedisReplyToStreamEntries(
  const AReply: IRedisReply): TRedisStreamEntryArray;

/// Converte a resposta de um XREAD/XREADGROUP, aceitando tanto a lista de
/// pares do RESP2 quanto o mapa achatado do RESP3.
function RedisReplyToStreamData(
  const AReply: IRedisReply): TRedisStreamDataArray;

/// Indice da chave no resultado de um XREAD, ou -1. Existe porque chave sem
/// novidade nao aparece: procurar pelo nome e' o certo, indexar pela posicao
/// da chamada e' o errado.
function RedisFindStreamData(const AData: TRedisStreamDataArray;
  const AKey: string): Integer;

implementation

{ Helpers de unit }

function RedisStreamId(AMilliseconds, ASequence: Int64): string;
begin
  Result := IntToStr(AMilliseconds) + '-' + IntToStr(ASequence);
end;

function RedisTryParseStreamId(const AId: string;
  out AMilliseconds, ASequence: Int64): Boolean;
var
  LText: string;
  LPos: Integer;
begin
  AMilliseconds := 0;
  ASequence := 0;
  LText := AId;
  if (LText <> '') and (LText[1] = '(') then
    LText := Copy(LText, 2, Length(LText) - 1);
  LPos := Pos('-', LText);
  if LPos <= 0 then
    Exit(False);
  Result := RedisTryParseInt64(Copy(LText, 1, LPos - 1), AMilliseconds) and
    RedisTryParseInt64(Copy(LText, LPos + 1, Length(LText) - LPos), ASequence);
end;

function RedisStreamIdExclusive(const AId: string): string;
begin
  if (AId <> '') and (AId[1] = '(') then
    Result := AId
  else
    Result := '(' + AId;
end;

function RedisReplyToStreamEntries(
  const AReply: IRedisReply): TRedisStreamEntryArray;
var
  I: Integer;
  LItem: IRedisReply;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.CreateFmt('esperava lista de entradas, veio %s',
      [RedisReplyKindName(AReply.Kind)]);
  SetLength(Result, AReply.Count);
  for I := 0 to AReply.Count - 1 do
  begin
    LItem := AReply[I];
    if (not LItem.IsAggregate) or (LItem.Count <> 2) then
      raise ERedisTypeError.Create('esperava entrada [id, campos]');
    Result[I].Id := LItem[0].AsString;
    // Campos nulos: a entrada saiu do stream e sobrou so' na PEL. Guardar nil
    // (e nao o no' nulo) deixa um teste so' — Fields = nil — para esse caso.
    if LItem[1].IsNull then
      Result[I].Fields := nil
    else
      Result[I].Fields := LItem[1];
  end;
end;

function RedisReplyToStreamData(
  const AReply: IRedisReply): TRedisStreamDataArray;
var
  I, LCount: Integer;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.CreateFmt('esperava resposta de XREAD, veio %s',
      [RedisReplyKindName(AReply.Kind)]);
  if AReply.Count = 0 then
    Exit;

  // RESP2 devolve uma lista de pares [chave, entradas]; RESP3, um mapa que o
  // leitor guarda achatado (chave, entradas, chave, entradas...). O primeiro
  // item decide: num par ele e' agregado; no mapa achatado e' o nome da chave.
  if AReply[0].IsAggregate then
  begin
    SetLength(Result, AReply.Count);
    for I := 0 to AReply.Count - 1 do
    begin
      if AReply[I].Count <> 2 then
        raise ERedisTypeError.Create('esperava par [chave, entradas]');
      Result[I].Key := AReply[I][0].AsString;
      Result[I].Entries := RedisReplyToStreamEntries(AReply[I][1]);
    end;
    Exit;
  end;

  if Odd(AReply.Count) then
    raise ERedisTypeError.Create('mapa de streams com tamanho impar');
  LCount := AReply.Count div 2;
  SetLength(Result, LCount);
  for I := 0 to LCount - 1 do
  begin
    Result[I].Key := AReply[I * 2].AsString;
    Result[I].Entries := RedisReplyToStreamEntries(AReply[I * 2 + 1]);
  end;
end;

function RedisFindStreamData(const AData: TRedisStreamDataArray;
  const AKey: string): Integer;
var
  I: Integer;
begin
  for I := 0 to High(AData) do
    if AData[I].Key = AKey then
      Exit(I);
  Result := -1;
end;

function RedisReplyToPendingConsumers(
  const AReply: IRedisReply): TRedisPendingConsumerArray;
var
  I: Integer;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.Create('esperava lista de consumidores');
  SetLength(Result, AReply.Count);
  for I := 0 to AReply.Count - 1 do
  begin
    if (not AReply[I].IsAggregate) or (AReply[I].Count <> 2) then
      raise ERedisTypeError.Create('esperava par [consumidor, pendencias]');
    Result[I].Name := AReply[I][0].AsString;
    // A contagem vem como bulk string, e nao como inteiro; AsInteger cobre os
    // dois.
    Result[I].Count := AReply[I][1].AsInteger;
  end;
end;

function RedisReplyToPendingEntries(
  const AReply: IRedisReply): TRedisPendingEntryArray;
var
  I: Integer;
  LItem: IRedisReply;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.Create('esperava lista de pendencias');
  SetLength(Result, AReply.Count);
  for I := 0 to AReply.Count - 1 do
  begin
    LItem := AReply[I];
    if (not LItem.IsAggregate) or (LItem.Count <> 4) then
      raise ERedisTypeError.Create(
        'esperava pendencia [id, consumidor, ocioso, entregas]');
    Result[I].Id := LItem[0].AsString;
    Result[I].Consumer := LItem[1].AsString;
    Result[I].IdleMs := LItem[2].AsInteger;
    Result[I].DeliveryCount := LItem[3].AsInteger;
  end;
end;

// Acrescenta a lista inteira ao fim dos argumentos.
procedure AddAll(var AArgs: TRedisArgs; const AItems: array of TRedisArg);
var
  I: Integer;
begin
  for I := 0 to High(AItems) do
    RedisAddArg(AArgs, AItems[I]);
end;

// Monta 'STREAMS k1 k2 ... id1 id2 ...' conferindo antes que as duas listas
// tenham o mesmo tamanho. Trocar a quantidade rende do servidor um
// "Unbalanced XREAD list of streams", que nao diz onde esta' o engano.
procedure AddStreamsBlock(var AArgs: TRedisArgs; const AName: string;
  const AKeys, AIds: array of TRedisArg);
begin
  if Length(AKeys) = 0 then
    raise ERedisException.CreateFmt('%s sem chave', [AName]);
  if Length(AKeys) <> Length(AIds) then
    raise ERedisException.CreateFmt(
      '%s: %d chaves para %d ids - precisa de um id por chave',
      [AName, Length(AKeys), Length(AIds)]);
  RedisAddArg(AArgs, 'STREAMS');
  AddAll(AArgs, AKeys);
  AddAll(AArgs, AIds);
end;

// Modificador de trim comum a XADD e XTRIM: MAXLEN/MINID, '~' ou '='.
procedure AddTrim(var AArgs: TRedisArgs; const AStrategy: string;
  const AThreshold: TRedisArg; AApproximate: Boolean);
begin
  RedisAddArg(AArgs, AStrategy);
  if AApproximate then
    RedisAddArg(AArgs, '~')
  else
    RedisAddArg(AArgs, '=');
  RedisAddArg(AArgs, AThreshold);
end;

procedure CheckFieldValues(const AName: string;
  const AFieldValues: array of TRedisArg);
begin
  if Length(AFieldValues) = 0 then
    raise ERedisException.CreateFmt('%s sem par campo/valor', [AName]);
  if Odd(Length(AFieldValues)) then
    raise ERedisException.CreateFmt(
      '%s espera campo, valor, campo, valor...', [AName]);
end;

// Monta os argumentos comuns aos dois XREADGROUP (com e sem BLOCK).
function BuildReadGroupArgs(const AGroup, AConsumer: TRedisArg;
  const AKeys, AIds: array of TRedisArg; ACount: Int64; ANoAck: Boolean;
  ABlockMs: Int64; AWithBlock: Boolean): TRedisArgs;
begin
  Result := RedisArgs(['GROUP', AGroup, AConsumer], []);
  if ACount >= 0 then
  begin
    RedisAddArg(Result, 'COUNT');
    RedisAddArg(Result, ACount);
  end;
  if AWithBlock then
  begin
    if ABlockMs < 0 then
      raise ERedisException.Create('XREADGROUP: BLOCK negativo');
    RedisAddArg(Result, 'BLOCK');
    RedisAddArg(Result, ABlockMs);
  end;
  if ANoAck then
    RedisAddArg(Result, 'NOACK');
  AddStreamsBlock(Result, 'XREADGROUP', AKeys, AIds);
end;

// Le a resposta do XAUTOCLAIM: [cursor, entradas] no Redis 6.2 e
// [cursor, entradas, apagados] no 7+. Aceitar os dois tamanhos e' o que mantem
// a lib util contra servidor 6.2 sem ramificar por versao.
procedure ParseAutoClaim(const AReply: IRedisReply; out ANextId: string;
  out AEntries: TRedisStreamEntryArray; out ADeleted: TRedisStringArray);
begin
  ANextId := '';
  AEntries := nil;
  ADeleted := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if (not AReply.IsAggregate) or (AReply.Count < 2) then
    raise ERedisTypeError.Create('XAUTOCLAIM: esperava [cursor, entradas, ...]');
  ANextId := AReply[0].AsString;
  AEntries := RedisReplyToStreamEntries(AReply[1]);
  if AReply.Count >= 3 then
    ADeleted := RedisReplyToStrings(AReply[2]);
end;

{ TRedisStreamEntry }

function TRedisStreamEntry.FieldCount: Integer;
begin
  if Fields = nil then
    Result := 0
  else
    Result := Fields.Count div 2;
end;

function TRedisStreamEntry.IsDeleted: Boolean;
begin
  Result := Fields = nil;
end;

function TRedisStreamEntry.TryFieldValue(const AName: string;
  out AValue: string): Boolean;
var
  LValue: IRedisReply;
begin
  AValue := '';
  if Fields = nil then
    Exit(False);
  LValue := Fields.ValueByKey(AName);
  Result := LValue <> nil;
  if Result then
    AValue := LValue.AsString;
end;

function TRedisStreamEntry.FieldValue(const AName: string): string;
begin
  if not TryFieldValue(AName, Result) then
    Result := '';
end;

function TRedisStreamEntry.FieldBytes(const AName: string): TBytes;
var
  LValue: IRedisReply;
begin
  Result := nil;
  if Fields = nil then
    Exit;
  LValue := Fields.ValueByKey(AName);
  if LValue <> nil then
    Result := LValue.AsBytes;
end;

{ TRedisStreamsCommands }

function TRedisStreamsCommands.XAdd(const AKey: TRedisArg;
  const AFieldValues: array of TRedisArg): string;
begin
  Result := XAddId(AKey, REDIS_STREAM_AUTO_ID, AFieldValues);
end;

function TRedisStreamsCommands.XAddId(const AKey, AId: TRedisArg;
  const AFieldValues: array of TRedisArg): string;
begin
  CheckFieldValues('XADD', AFieldValues);
  Result := CmdString('XADD', RedisArgs([AKey, AId], AFieldValues));
end;

function TRedisStreamsCommands.XAddMaxLen(const AKey: TRedisArg;
  AMaxLen: Int64; AApproximate: Boolean;
  const AFieldValues: array of TRedisArg): string;
var
  LArgs: TRedisArgs;
begin
  CheckFieldValues('XADD', AFieldValues);
  LArgs := RedisArgs([AKey], []);
  AddTrim(LArgs, 'MAXLEN', AMaxLen, AApproximate);
  RedisAddArg(LArgs, REDIS_STREAM_AUTO_ID);
  AddAll(LArgs, AFieldValues);
  Result := CmdString('XADD', LArgs);
end;

function TRedisStreamsCommands.XAddMinId(const AKey, AMinId: TRedisArg;
  AApproximate: Boolean; const AFieldValues: array of TRedisArg): string;
var
  LArgs: TRedisArgs;
begin
  CheckFieldValues('XADD', AFieldValues);
  LArgs := RedisArgs([AKey], []);
  AddTrim(LArgs, 'MINID', AMinId, AApproximate);
  RedisAddArg(LArgs, REDIS_STREAM_AUTO_ID);
  AddAll(LArgs, AFieldValues);
  Result := CmdString('XADD', LArgs);
end;

function TRedisStreamsCommands.XLen(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('XLEN', [AKey]);
end;

function TRedisStreamsCommands.XDel(const AKey: TRedisArg;
  const AIds: array of TRedisArg): Int64;
begin
  if Length(AIds) = 0 then
    Exit(0);
  Result := CmdInt('XDEL', RedisArgs([AKey], AIds));
end;

function TRedisStreamsCommands.XTrimMaxLen(const AKey: TRedisArg;
  AMaxLen: Int64; AApproximate: Boolean): Int64;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey], []);
  AddTrim(LArgs, 'MAXLEN', AMaxLen, AApproximate);
  Result := CmdInt('XTRIM', LArgs);
end;

function TRedisStreamsCommands.XTrimMinId(const AKey, AMinId: TRedisArg;
  AApproximate: Boolean): Int64;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey], []);
  AddTrim(LArgs, 'MINID', AMinId, AApproximate);
  Result := CmdInt('XTRIM', LArgs);
end;

function TRedisStreamsCommands.XRange(const AKey, AStart, AStop: TRedisArg;
  ACount: Int64): TRedisStreamEntryArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AStart, AStop], []);
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'COUNT');
    RedisAddArg(LArgs, ACount);
  end;
  Result := RedisReplyToStreamEntries(Cmd('XRANGE', LArgs));
end;

function TRedisStreamsCommands.XRevRange(const AKey, AStop, AStart: TRedisArg;
  ACount: Int64): TRedisStreamEntryArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AStop, AStart], []);
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'COUNT');
    RedisAddArg(LArgs, ACount);
  end;
  Result := RedisReplyToStreamEntries(Cmd('XREVRANGE', LArgs));
end;

function TRedisStreamsCommands.XRead(const AKeys, AIds: array of TRedisArg;
  ACount: Int64): TRedisStreamDataArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := nil;
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'COUNT');
    RedisAddArg(LArgs, ACount);
  end;
  AddStreamsBlock(LArgs, 'XREAD', AKeys, AIds);
  Result := RedisReplyToStreamData(Cmd('XREAD', LArgs));
end;

function TRedisStreamsCommands.XReadBlocking(
  const AKeys, AIds: array of TRedisArg; ABlockMs: Int64;
  ACount: Int64): TRedisStreamDataArray;
var
  LArgs: TRedisArgs;
begin
  if ABlockMs < 0 then
    raise ERedisException.Create('XREAD: BLOCK negativo');
  LArgs := nil;
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'COUNT');
    RedisAddArg(LArgs, ACount);
  end;
  RedisAddArg(LArgs, 'BLOCK');
  RedisAddArg(LArgs, ABlockMs);
  AddStreamsBlock(LArgs, 'XREAD', AKeys, AIds);
  // ExecuteBlocking fala em SEGUNDOS (a unidade do BLPOP); o BLOCK do XREAD,
  // em milissegundos. A divisao acontece aqui, e nao no chamador, para que a
  // assinatura publica use a mesma unidade do comando.
  Result := RedisReplyToStreamData(
    Executor.ExecuteBlocking('XREAD', LArgs, ABlockMs / 1000));
end;

procedure TRedisStreamsCommands.XGroupCreate(const AKey, AGroup,
  AId: TRedisArg; AMkStream: Boolean);
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs(['CREATE', AKey, AGroup, AId], []);
  if AMkStream then
    RedisAddArg(LArgs, 'MKSTREAM');
  CmdVoid('XGROUP', LArgs);
end;

function TRedisStreamsCommands.XGroupTryCreate(const AKey, AGroup,
  AId: TRedisArg; AMkStream: Boolean): Boolean;
var
  LArgs: TRedisArgs;
  LReply: IRedisReply;
begin
  LArgs := RedisArgs(['CREATE', AKey, AGroup, AId], []);
  if AMkStream then
    RedisAddArg(LArgs, 'MKSTREAM');
  LReply := CmdRaw('XGROUP', LArgs);
  if LReply.IsError then
  begin
    // BUSYGROUP e' o unico erro que aqui vale como resposta: o grupo ja'
    // existe, que e' exatamente o estado que se queria. Qualquer outro (a
    // chave ausente sem MKSTREAM, WRONGTYPE) continua sendo falha.
    if LReply.ErrorCode = 'BUSYGROUP' then
      Exit(False);
    LReply.RaiseIfError;
  end;
  Result := True;
end;

function TRedisStreamsCommands.XGroupDestroy(const AKey,
  AGroup: TRedisArg): Boolean;
begin
  Result := CmdInt('XGROUP', ['DESTROY', AKey, AGroup]) > 0;
end;

procedure TRedisStreamsCommands.XGroupSetId(const AKey, AGroup,
  AId: TRedisArg);
begin
  CmdVoid('XGROUP', ['SETID', AKey, AGroup, AId]);
end;

function TRedisStreamsCommands.XGroupCreateConsumer(const AKey, AGroup,
  AConsumer: TRedisArg): Boolean;
begin
  Result := CmdInt('XGROUP', ['CREATECONSUMER', AKey, AGroup, AConsumer]) > 0;
end;

function TRedisStreamsCommands.XGroupDelConsumer(const AKey, AGroup,
  AConsumer: TRedisArg): Int64;
begin
  Result := CmdInt('XGROUP', ['DELCONSUMER', AKey, AGroup, AConsumer]);
end;

function TRedisStreamsCommands.XReadGroup(const AGroup, AConsumer: TRedisArg;
  const AKeys, AIds: array of TRedisArg; ACount: Int64;
  ANoAck: Boolean): TRedisStreamDataArray;
begin
  Result := RedisReplyToStreamData(Cmd('XREADGROUP',
    BuildReadGroupArgs(AGroup, AConsumer, AKeys, AIds, ACount, ANoAck,
      0, False)));
end;

function TRedisStreamsCommands.XReadGroupBlocking(const AGroup,
  AConsumer: TRedisArg; const AKeys, AIds: array of TRedisArg;
  ABlockMs: Int64; ACount: Int64; ANoAck: Boolean): TRedisStreamDataArray;
begin
  Result := RedisReplyToStreamData(Executor.ExecuteBlocking('XREADGROUP',
    BuildReadGroupArgs(AGroup, AConsumer, AKeys, AIds, ACount, ANoAck,
      ABlockMs, True), ABlockMs / 1000));
end;

function TRedisStreamsCommands.XAck(const AKey, AGroup: TRedisArg;
  const AIds: array of TRedisArg): Int64;
begin
  if Length(AIds) = 0 then
    Exit(0);
  Result := CmdInt('XACK', RedisArgs([AKey, AGroup], AIds));
end;

function TRedisStreamsCommands.XPendingSummary(const AKey,
  AGroup: TRedisArg): TRedisPendingSummary;
var
  LReply: IRedisReply;
begin
  Result.Count := 0;
  Result.MinId := '';
  Result.MaxId := '';
  Result.Consumers := nil;
  LReply := Cmd('XPENDING', [AKey, AGroup]);
  if LReply.IsNull then
    Exit;
  if (not LReply.IsAggregate) or (LReply.Count <> 4) then
    raise ERedisTypeError.Create(
      'XPENDING: esperava [total, min, max, consumidores]');
  Result.Count := LReply[0].AsInteger;
  // Sem pendencia, min e max vem NULOS — nao vazios. AsString ja' devolve ''
  // no nulo, e aqui os dois casos significam a mesma coisa.
  Result.MinId := LReply[1].AsString;
  Result.MaxId := LReply[2].AsString;
  Result.Consumers := RedisReplyToPendingConsumers(LReply[3]);
end;

function TRedisStreamsCommands.XPendingRange(const AKey, AGroup, AStart,
  AStop: TRedisArg; ACount: Int64;
  const AConsumer: string): TRedisPendingEntryArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AGroup, AStart, AStop, ACount], []);
  if AConsumer <> '' then
    RedisAddArg(LArgs, AConsumer);
  Result := RedisReplyToPendingEntries(Cmd('XPENDING', LArgs));
end;

function TRedisStreamsCommands.XPendingIdle(const AKey, AGroup: TRedisArg;
  AMinIdleMs: Int64; const AStart, AStop: TRedisArg; ACount: Int64;
  const AConsumer: string): TRedisPendingEntryArray;
var
  LArgs: TRedisArgs;
begin
  // IDLE vem ANTES da faixa; trocar a ordem e' erro de sintaxe no servidor.
  LArgs := RedisArgs([AKey, AGroup, 'IDLE', AMinIdleMs, AStart, AStop,
    ACount], []);
  if AConsumer <> '' then
    RedisAddArg(LArgs, AConsumer);
  Result := RedisReplyToPendingEntries(Cmd('XPENDING', LArgs));
end;

function TRedisStreamsCommands.XClaim(const AKey, AGroup, AConsumer: TRedisArg;
  AMinIdleMs: Int64; const AIds: array of TRedisArg): TRedisStreamEntryArray;
begin
  if Length(AIds) = 0 then
    raise ERedisException.Create('XCLAIM sem id');
  Result := RedisReplyToStreamEntries(Cmd('XCLAIM',
    RedisArgs([AKey, AGroup, AConsumer, AMinIdleMs], AIds)));
end;

function TRedisStreamsCommands.XClaimJustId(const AKey, AGroup,
  AConsumer: TRedisArg; AMinIdleMs: Int64;
  const AIds: array of TRedisArg): TRedisStringArray;
var
  LArgs: TRedisArgs;
begin
  if Length(AIds) = 0 then
    raise ERedisException.Create('XCLAIM sem id');
  LArgs := RedisArgs([AKey, AGroup, AConsumer, AMinIdleMs], AIds);
  RedisAddArg(LArgs, 'JUSTID');
  Result := CmdStrings('XCLAIM', LArgs);
end;

function TRedisStreamsCommands.XAutoClaim(const AKey, AGroup,
  AConsumer: TRedisArg; AMinIdleMs: Int64; const AStartId: TRedisArg;
  ACount: Int64; out ANextId: string): TRedisStreamEntryArray;
var
  LDeleted: TRedisStringArray;
begin
  Result := XAutoClaim(AKey, AGroup, AConsumer, AMinIdleMs, AStartId, ACount,
    ANextId, LDeleted);
end;

function TRedisStreamsCommands.XAutoClaim(const AKey, AGroup,
  AConsumer: TRedisArg; AMinIdleMs: Int64; const AStartId: TRedisArg;
  ACount: Int64; out ANextId: string;
  out ADeletedIds: TRedisStringArray): TRedisStreamEntryArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AGroup, AConsumer, AMinIdleMs, AStartId], []);
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'COUNT');
    RedisAddArg(LArgs, ACount);
  end;
  ParseAutoClaim(Cmd('XAUTOCLAIM', LArgs), ANextId, Result, ADeletedIds);
end;

function TRedisStreamsCommands.XInfoStream(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('XINFO', ['STREAM', AKey]);
end;

function TRedisStreamsCommands.XInfoGroups(const AKey: TRedisArg): IRedisReply;
begin
  Result := Cmd('XINFO', ['GROUPS', AKey]);
end;

function TRedisStreamsCommands.XInfoConsumers(const AKey,
  AGroup: TRedisArg): IRedisReply;
begin
  Result := Cmd('XINFO', ['CONSUMERS', AKey, AGroup]);
end;

end.
