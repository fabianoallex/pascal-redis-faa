unit Redis.Commands.PubSub;

{ Familia PUBLISH/PUBSUB: o lado de QUEM PUBLICA.

  Fica separada da Redis.PubSub de proposito. Publicar e' um comando comum como
  outro qualquer — vai e volta numa conexao do pool, nao precisa de thread nem
  de conexao dedicada, e o app que so' publica nao carrega nada de assinatura
  junto. Quem ASSINA e' que precisa de tudo aquilo.

  O retorno do PUBLISH e' a unica informacao que o Redis da' sobre entrega:
  quantos assinantes RECEBERAM a mensagem naquele instante. Zero significa que
  ninguem ouviu e a mensagem evaporou — nao ha' fila esperando alguem assinar.
  Vale a pena repetir porque e' a diferenca entre pub/sub e Streams (M8).

  PUBSUB CHANNELS/NUMSUB/NUMPAT sao introspeccao: uteis para diagnostico e para
  testes, mas nao para logica de aplicacao, ja' que a resposta envelhece no
  caminho de volta. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

type
  /// Contagem de assinantes por canal, como o PUBSUB NUMSUB devolve.
  TRedisChannelCount = record
    Channel: string;
    Subscribers: Int64;
  end;

  TRedisChannelCountArray = array of TRedisChannelCount;

  TRedisPubSubCommands = class(TRedisCommandFamily)
  public
    /// PUBLISH canal mensagem. Devolve quantos assinantes receberam — zero
    /// quer dizer que a mensagem se perdeu, e nao que ficou guardada.
    function Publish(const AChannel, AMessage: TRedisArg): Int64;

    /// SPUBLISH: publica num canal shardado (Redis 7+). Fora de cluster e'
    /// equivalente ao PUBLISH, so' que so' alcanca quem usou SSUBSCRIBE.
    function SPublish(const AChannel, AMessage: TRedisArg): Int64;

    /// PUBSUB CHANNELS [padrao]: canais com pelo menos um assinante por nome
    /// exato. NAO lista canais alcancados so' por PSUBSCRIBE — para esses
    /// existe o NumPatterns.
    function ActiveChannels: TRedisStringArray; overload;
    function ActiveChannels(const APattern: TRedisArg): TRedisStringArray; overload;

    /// PUBSUB NUMSUB canal...: quantos assinantes por nome exato tem cada
    /// canal. Canal sem assinante nenhum vem com zero, nao some da lista.
    function CountSubscribers(const AChannels: array of TRedisArg): TRedisChannelCountArray;

    /// PUBSUB NUMPAT: quantos PADROES distintos estao assinados no servidor
    /// inteiro (nao quantos assinantes).
    function NumPatterns: Int64;

    /// PUBSUB SHARDCHANNELS [padrao] (Redis 7+).
    function ActiveShardChannels: TRedisStringArray; overload;
    function ActiveShardChannels(const APattern: TRedisArg): TRedisStringArray; overload;

    /// PUBSUB SHARDNUMSUB canal... (Redis 7+).
    function CountShardSubscribers(const AChannels: array of TRedisArg): TRedisChannelCountArray;
  end;

/// Converte a resposta achatada do PUBSUB NUMSUB (canal, contagem, canal,
/// contagem...) na lista de pares. Em RESP3 a mesma resposta chega como mapa,
/// que a lib tambem guarda achatado — por isso um codigo so' atende os dois.
function RedisReplyToChannelCounts(const AReply: IRedisReply): TRedisChannelCountArray;

implementation

function RedisReplyToChannelCounts(
  const AReply: IRedisReply): TRedisChannelCountArray;
var
  I, LPairs: Integer;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.CreateFmt(
      'esperava lista de canal/contagem, veio %s',
      [RedisReplyKindName(AReply.Kind)]);
  LPairs := AReply.Count div 2;
  SetLength(Result, LPairs);
  for I := 0 to LPairs - 1 do
  begin
    Result[I].Channel := AReply[I * 2].AsString;
    Result[I].Subscribers := AReply[I * 2 + 1].AsInteger;
  end;
end;

{ TRedisPubSubCommands }

function TRedisPubSubCommands.Publish(const AChannel, AMessage: TRedisArg): Int64;
begin
  Result := CmdInt('PUBLISH', [AChannel, AMessage]);
end;

function TRedisPubSubCommands.SPublish(const AChannel, AMessage: TRedisArg): Int64;
begin
  Result := CmdInt('SPUBLISH', [AChannel, AMessage]);
end;

function TRedisPubSubCommands.ActiveChannels: TRedisStringArray;
begin
  Result := CmdStrings('PUBSUB', ['CHANNELS']);
end;

function TRedisPubSubCommands.ActiveChannels(
  const APattern: TRedisArg): TRedisStringArray;
begin
  Result := CmdStrings('PUBSUB', ['CHANNELS', APattern]);
end;

function TRedisPubSubCommands.CountSubscribers(
  const AChannels: array of TRedisArg): TRedisChannelCountArray;
begin
  Result := RedisReplyToChannelCounts(
    Cmd('PUBSUB', RedisArgs(['NUMSUB'], AChannels)));
end;

function TRedisPubSubCommands.NumPatterns: Int64;
begin
  Result := CmdInt('PUBSUB', ['NUMPAT']);
end;

function TRedisPubSubCommands.ActiveShardChannels: TRedisStringArray;
begin
  Result := CmdStrings('PUBSUB', ['SHARDCHANNELS']);
end;

function TRedisPubSubCommands.ActiveShardChannels(
  const APattern: TRedisArg): TRedisStringArray;
begin
  Result := CmdStrings('PUBSUB', ['SHARDCHANNELS', APattern]);
end;

function TRedisPubSubCommands.CountShardSubscribers(
  const AChannels: array of TRedisArg): TRedisChannelCountArray;
begin
  Result := RedisReplyToChannelCounts(
    Cmd('PUBSUB', RedisArgs(['SHARDNUMSUB'], AChannels)));
end;

end.
