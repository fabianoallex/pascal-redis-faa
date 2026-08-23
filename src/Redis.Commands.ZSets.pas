unit Redis.Commands.ZSets;

{ Comandos de sorted set: conjunto sem repeticao em que cada membro carrega um
  score em ponto flutuante, e a colecao fica permanentemente ordenada por ele.

  E' o tipo de ranking, fila de prioridade, indice por tempo e janela
  deslizante de rate limit. O empate de score desempata pela ordem lexicografica
  do membro, o que torna a ordem TOTAL e estavel.

  Duas armadilhas de protocolo que esta unit absorve:

  1. **WITHSCORES muda de forma entre RESP2 e RESP3.** Em RESP2 vem uma lista
     achatada (membro, score, membro, score...); em RESP3, uma lista de PARES
     [membro, score]. Os metodos ...WithScores aceitam as duas formas e
     devolvem sempre TRedisScoreMemberArray, entao o codigo da aplicacao nao
     ramifica por protocolo.

  2. **Score e' texto no RESP2 e double no RESP3.** IRedisReply.AsDouble ja'
     cobre os dois, e entende 'inf', '-inf' e 'nan'.

  Os limites de faixa (min/max) sao string de proposito, e nao Double: a
  sintaxe do Redis inclui '-inf', '+inf' e o prefixo '(' para extremo aberto,
  que nao cabem num numero. Use RedisScoreBound para montar sem errar. }

{$I redis.inc}

interface

uses
  SysUtils,
  Redis.Types,
  Redis.Commands;

const
  /// Limites infinitos de faixa, para ZCOUNT / ZRANGEBYSCORE / ZREMRANGEBYSCORE.
  REDIS_SCORE_MIN = '-inf';
  REDIS_SCORE_MAX = '+inf';

type
  /// Um membro com o seu score.
  TRedisScoreMember = record
    Member: string;
    Score: Double;
  end;

  TRedisScoreMemberArray = array of TRedisScoreMember;

  /// Condicao de existencia do ZADD.
  ///   zaAlways       — grava sempre (padrao)
  ///   zaNotExists    — NX: so' acrescenta membro novo, nunca atualiza score
  ///   zaExists       — XX: so' atualiza membro existente, nunca acrescenta
  ///   zaGreaterThan  — GT: so' atualiza se o score novo for MAIOR (Redis 6.2+)
  ///   zaLessThan     — LT: so' atualiza se for MENOR (Redis 6.2+)
  TRedisZAddCondition = (zaAlways, zaNotExists, zaExists, zaGreaterThan,
    zaLessThan);

  /// Comandos de sorted set (ZADD, ZRANGE, ZINCRBY...).
  TRedisZSetsCommands = class(TRedisCommandFamily)
  public
    /// ZADD de um membro. Devolve 1 se o membro foi ACRESCENTADO e 0 se apenas
    /// teve o score atualizado.
    function ZAdd(const AKey: TRedisArg; AScore: Double;
      const AMember: TRedisArg): Int64;

    /// ZADD com condicao. AChanged (o modificador CH) troca o significado do
    /// retorno: em vez de "quantos foram acrescentados", passa a ser "quantos
    /// mudaram", acrescentados mais atualizados. E' o que se quer quando o
    /// interesse e' saber se o ranking se mexeu.
    function ZAddOpt(const AKey: TRedisArg; AScore: Double;
      const AMember: TRedisArg; ACondition: TRedisZAddCondition;
      AChanged: Boolean = False): Int64;

    /// ZADD de varios: score, membro, score, membro... (nesta ordem, que e' a
    /// do comando).
    function ZAddMany(const AKey: TRedisArg;
      const AScoreMembers: array of TRedisArg): Int64;

    /// ZINCRBY: soma ao score e devolve o resultado. Cria o membro com o
    /// proprio delta se ele nao existia.
    function ZIncrBy(const AKey: TRedisArg; ADelta: Double;
      const AMember: TRedisArg): Double;

    function ZRem(const AKey, AMember: TRedisArg): Boolean;
    function ZRemMany(const AKey: TRedisArg;
      const AMembers: array of TRedisArg): Int64;

    /// ZSCORE cru: NULO quando o membro nao esta' no conjunto — que e'
    /// diferente de score zero.
    function ZScore(const AKey, AMember: TRedisArg): IRedisReply;
    /// ZSCORE com presenca explicita.
    function ZTryScore(const AKey, AMember: TRedisArg; out AScore: Double): Boolean;

    function ZCard(const AKey: TRedisArg): Int64;
    /// ZCOUNT numa faixa de score. Extremos incluidos; use RedisScoreBound
    /// para abrir um deles.
    function ZCount(const AKey: TRedisArg; const AMin, AMax: string): Int64;

    /// ZRANK / ZREVRANK: posicao 0-based na ordem crescente / decrescente.
    /// False quando o membro nao esta' no conjunto.
    function ZTryRank(const AKey, AMember: TRedisArg; out ARank: Int64): Boolean;
    function ZTryRevRank(const AKey, AMember: TRedisArg; out ARank: Int64): Boolean;

    /// ZRANGE por posicao, com os dois extremos INCLUIDOS e indice negativo
    /// contando do fim. ZRange(k, 0, -1) e' o conjunto inteiro em ordem
    /// crescente de score; ZRevRange(k, 0, 9) e' o top 10.
    function ZRange(const AKey: TRedisArg; AStart, AStop: Int64): TRedisStringArray;
    function ZRevRange(const AKey: TRedisArg; AStart, AStop: Int64): TRedisStringArray;
    function ZRangeWithScores(const AKey: TRedisArg;
      AStart, AStop: Int64): TRedisScoreMemberArray;
    function ZRevRangeWithScores(const AKey: TRedisArg;
      AStart, AStop: Int64): TRedisScoreMemberArray;

    /// ZRANGEBYSCORE. AOffset/ACount aplicam o LIMIT; passe ACount < 0 para
    /// nao limitar (o padrao).
    function ZRangeByScore(const AKey: TRedisArg; const AMin, AMax: string;
      AOffset: Int64 = 0; ACount: Int64 = -1): TRedisStringArray;
    function ZRangeByScoreWithScores(const AKey: TRedisArg;
      const AMin, AMax: string; AOffset: Int64 = 0;
      ACount: Int64 = -1): TRedisScoreMemberArray;
    /// ZREVRANGEBYSCORE. ATENCAO: aqui o MAIOR vem primeiro, entao os
    /// argumentos sao (max, min) — a ordem inversa da do ZRangeByScore.
    function ZRevRangeByScore(const AKey: TRedisArg; const AMax, AMin: string;
      AOffset: Int64 = 0; ACount: Int64 = -1): TRedisStringArray;

    function ZRemRangeByRank(const AKey: TRedisArg; AStart, AStop: Int64): Int64;
    function ZRemRangeByScore(const AKey: TRedisArg; const AMin, AMax: string): Int64;

    /// ZPOPMIN / ZPOPMAX: tira o membro de menor / maior score. False quando o
    /// conjunto esta' vazio ou nao existe.
    function ZPopMin(const AKey: TRedisArg; out AMember: string;
      out AScore: Double): Boolean;
    function ZPopMax(const AKey: TRedisArg; out AMember: string;
      out AScore: Double): Boolean;

    /// Um passo do ZSCAN, com a mesma mecanica de cursor do SCAN (ver
    /// TRedisKeysCommands.Scan). Os elementos vem ACHATADOS: membro, score,
    /// membro, score...
    function ZScan(const AKey: TRedisArg; var ACursor: Int64;
      const AMatch: string = ''; ACount: Integer = 0): TRedisStringArray;
  end;

/// Monta um limite de faixa a partir de um numero. AExclusive poe o '(' que o
/// Redis usa para extremo ABERTO: RedisScoreBound(10, True) = '(10', que
/// significa "maior que 10", enquanto '10' significa "10 ou mais".
function RedisScoreBound(const AValue: Double; AExclusive: Boolean = False): string;

/// Converte a resposta de um ...WITHSCORES em pares, aceitando tanto a lista
/// achatada do RESP2 quanto a lista de pares do RESP3.
function RedisReplyToScoreMembers(
  const AReply: IRedisReply): TRedisScoreMemberArray;

implementation

function RedisScoreBound(const AValue: Double; AExclusive: Boolean): string;
begin
  Result := RedisFormatDouble(AValue);
  if AExclusive then
    Result := '(' + Result;
end;

function RedisReplyToScoreMembers(
  const AReply: IRedisReply): TRedisScoreMemberArray;
var
  I, LCount: Integer;
begin
  Result := nil;
  if (AReply = nil) or AReply.IsNull then
    Exit;
  if not AReply.IsAggregate then
    raise ERedisTypeError.CreateFmt('esperava lista com scores, veio %s',
      [RedisReplyKindName(AReply.Kind)]);
  if AReply.Count = 0 then
    Exit;

  // RESP3 devolve uma lista de pares [membro, score]; RESP2, uma lista
  // achatada. O primeiro item decide qual das duas chegou — nao ha' ambiguidade
  // porque um membro e' sempre escalar.
  if AReply[0].IsAggregate then
  begin
    SetLength(Result, AReply.Count);
    for I := 0 to AReply.Count - 1 do
    begin
      if AReply[I].Count <> 2 then
        raise ERedisTypeError.Create('esperava par [membro, score]');
      Result[I].Member := AReply[I][0].AsString;
      Result[I].Score := AReply[I][1].AsDouble;
    end;
    Exit;
  end;

  if Odd(AReply.Count) then
    raise ERedisTypeError.Create(
      'lista de membro/score com tamanho impar');
  LCount := AReply.Count div 2;
  SetLength(Result, LCount);
  for I := 0 to LCount - 1 do
  begin
    Result[I].Member := AReply[I * 2].AsString;
    Result[I].Score := AReply[I * 2 + 1].AsDouble;
  end;
end;

{ Helpers de unit }

function ConditionName(AValue: TRedisZAddCondition): string;
begin
  case AValue of
    zaNotExists:   Result := 'NX';
    zaExists:      Result := 'XX';
    zaGreaterThan: Result := 'GT';
    zaLessThan:    Result := 'LT';
  else
    Result := '';
  end;
end;

{ TRedisZSetsCommands }

function TRedisZSetsCommands.ZAdd(const AKey: TRedisArg; AScore: Double;
  const AMember: TRedisArg): Int64;
begin
  Result := CmdInt('ZADD', [AKey, AScore, AMember]);
end;

function TRedisZSetsCommands.ZAddOpt(const AKey: TRedisArg; AScore: Double;
  const AMember: TRedisArg; ACondition: TRedisZAddCondition;
  AChanged: Boolean): Int64;
var
  LArgs: TRedisArgs;
  LCondition: string;
begin
  LArgs := RedisArgs([AKey], []);
  LCondition := ConditionName(ACondition);
  if LCondition <> '' then
    RedisAddArg(LArgs, LCondition);
  if AChanged then
    RedisAddArg(LArgs, 'CH');
  RedisAddArg(LArgs, AScore);
  RedisAddArg(LArgs, AMember);
  Result := CmdInt('ZADD', LArgs);
end;

function TRedisZSetsCommands.ZAddMany(const AKey: TRedisArg;
  const AScoreMembers: array of TRedisArg): Int64;
begin
  if Length(AScoreMembers) = 0 then
    raise ERedisException.Create('ZADD sem par score/membro');
  if Odd(Length(AScoreMembers)) then
    raise ERedisException.Create('ZADD espera score, membro, score, membro...');
  Result := CmdInt('ZADD', RedisArgs([AKey], AScoreMembers));
end;

function TRedisZSetsCommands.ZIncrBy(const AKey: TRedisArg; ADelta: Double;
  const AMember: TRedisArg): Double;
begin
  Result := CmdDouble('ZINCRBY', [AKey, ADelta, AMember]);
end;

function TRedisZSetsCommands.ZRem(const AKey, AMember: TRedisArg): Boolean;
begin
  Result := CmdInt('ZREM', [AKey, AMember]) > 0;
end;

function TRedisZSetsCommands.ZRemMany(const AKey: TRedisArg;
  const AMembers: array of TRedisArg): Int64;
begin
  if Length(AMembers) = 0 then
    Exit(0);
  Result := CmdInt('ZREM', RedisArgs([AKey], AMembers));
end;

function TRedisZSetsCommands.ZScore(const AKey, AMember: TRedisArg): IRedisReply;
begin
  Result := Cmd('ZSCORE', [AKey, AMember]);
end;

function TRedisZSetsCommands.ZTryScore(const AKey, AMember: TRedisArg;
  out AScore: Double): Boolean;
var
  LReply: IRedisReply;
begin
  LReply := Cmd('ZSCORE', [AKey, AMember]);
  Result := not LReply.IsNull;
  if Result then
    AScore := LReply.AsDouble
  else
    AScore := 0;
end;

function TRedisZSetsCommands.ZCard(const AKey: TRedisArg): Int64;
begin
  Result := CmdInt('ZCARD', [AKey]);
end;

function TRedisZSetsCommands.ZCount(const AKey: TRedisArg;
  const AMin, AMax: string): Int64;
begin
  Result := CmdInt('ZCOUNT', [AKey, AMin, AMax]);
end;

function TRedisZSetsCommands.ZTryRank(const AKey, AMember: TRedisArg;
  out ARank: Int64): Boolean;
var
  LReply: IRedisReply;
begin
  LReply := Cmd('ZRANK', [AKey, AMember]);
  Result := not LReply.IsNull;
  if Result then
    ARank := LReply.AsInteger
  else
    ARank := -1;
end;

function TRedisZSetsCommands.ZTryRevRank(const AKey, AMember: TRedisArg;
  out ARank: Int64): Boolean;
var
  LReply: IRedisReply;
begin
  LReply := Cmd('ZREVRANK', [AKey, AMember]);
  Result := not LReply.IsNull;
  if Result then
    ARank := LReply.AsInteger
  else
    ARank := -1;
end;

function TRedisZSetsCommands.ZRange(const AKey: TRedisArg;
  AStart, AStop: Int64): TRedisStringArray;
begin
  Result := CmdStrings('ZRANGE', [AKey, AStart, AStop]);
end;

function TRedisZSetsCommands.ZRevRange(const AKey: TRedisArg;
  AStart, AStop: Int64): TRedisStringArray;
begin
  Result := CmdStrings('ZREVRANGE', [AKey, AStart, AStop]);
end;

function TRedisZSetsCommands.ZRangeWithScores(const AKey: TRedisArg;
  AStart, AStop: Int64): TRedisScoreMemberArray;
begin
  Result := RedisReplyToScoreMembers(
    Cmd('ZRANGE', [AKey, AStart, AStop, 'WITHSCORES']));
end;

function TRedisZSetsCommands.ZRevRangeWithScores(const AKey: TRedisArg;
  AStart, AStop: Int64): TRedisScoreMemberArray;
begin
  Result := RedisReplyToScoreMembers(
    Cmd('ZREVRANGE', [AKey, AStart, AStop, 'WITHSCORES']));
end;

function TRedisZSetsCommands.ZRangeByScore(const AKey: TRedisArg;
  const AMin, AMax: string; AOffset, ACount: Int64): TRedisStringArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AMin, AMax], []);
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'LIMIT');
    RedisAddArg(LArgs, AOffset);
    RedisAddArg(LArgs, ACount);
  end;
  Result := CmdStrings('ZRANGEBYSCORE', LArgs);
end;

function TRedisZSetsCommands.ZRangeByScoreWithScores(const AKey: TRedisArg;
  const AMin, AMax: string; AOffset, ACount: Int64): TRedisScoreMemberArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AMin, AMax, 'WITHSCORES'], []);
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'LIMIT');
    RedisAddArg(LArgs, AOffset);
    RedisAddArg(LArgs, ACount);
  end;
  Result := RedisReplyToScoreMembers(Cmd('ZRANGEBYSCORE', LArgs));
end;

function TRedisZSetsCommands.ZRevRangeByScore(const AKey: TRedisArg;
  const AMax, AMin: string; AOffset, ACount: Int64): TRedisStringArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, AMax, AMin], []);
  if ACount >= 0 then
  begin
    RedisAddArg(LArgs, 'LIMIT');
    RedisAddArg(LArgs, AOffset);
    RedisAddArg(LArgs, ACount);
  end;
  Result := CmdStrings('ZREVRANGEBYSCORE', LArgs);
end;

function TRedisZSetsCommands.ZRemRangeByRank(const AKey: TRedisArg;
  AStart, AStop: Int64): Int64;
begin
  Result := CmdInt('ZREMRANGEBYRANK', [AKey, AStart, AStop]);
end;

function TRedisZSetsCommands.ZRemRangeByScore(const AKey: TRedisArg;
  const AMin, AMax: string): Int64;
begin
  Result := CmdInt('ZREMRANGEBYSCORE', [AKey, AMin, AMax]);
end;

function TRedisZSetsCommands.ZPopMin(const AKey: TRedisArg; out AMember: string;
  out AScore: Double): Boolean;
var
  LPares: TRedisScoreMemberArray;
begin
  LPares := RedisReplyToScoreMembers(Cmd('ZPOPMIN', [AKey]));
  Result := Length(LPares) > 0;
  if Result then
  begin
    AMember := LPares[0].Member;
    AScore := LPares[0].Score;
  end
  else
  begin
    AMember := '';
    AScore := 0;
  end;
end;

function TRedisZSetsCommands.ZPopMax(const AKey: TRedisArg; out AMember: string;
  out AScore: Double): Boolean;
var
  LPares: TRedisScoreMemberArray;
begin
  LPares := RedisReplyToScoreMembers(Cmd('ZPOPMAX', [AKey]));
  Result := Length(LPares) > 0;
  if Result then
  begin
    AMember := LPares[0].Member;
    AScore := LPares[0].Score;
  end
  else
  begin
    AMember := '';
    AScore := 0;
  end;
end;

function TRedisZSetsCommands.ZScan(const AKey: TRedisArg; var ACursor: Int64;
  const AMatch: string; ACount: Integer): TRedisStringArray;
var
  LArgs: TRedisArgs;
begin
  LArgs := RedisArgs([AKey, ACursor], []);
  if AMatch <> '' then
  begin
    RedisAddArg(LArgs, 'MATCH');
    RedisAddArg(LArgs, AMatch);
  end;
  if ACount > 0 then
  begin
    RedisAddArg(LArgs, 'COUNT');
    RedisAddArg(LArgs, ACount);
  end;
  Result := ExecScan('ZSCAN', LArgs, ACursor);
end;

end.
