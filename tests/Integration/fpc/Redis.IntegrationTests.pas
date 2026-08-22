unit Redis.IntegrationTests;

{ Testes de integracao (FPCUnit). Mesma cobertura do tests\Integration\Redis.IntegrationTests.pas
  (DUnitX/Delphi) — as duas suites sao mantidas linha a linha equivalentes, entao
  toda mudanca aqui vai para la' na mesma sessao.

  PRECISAM DE UM REDIS DE VERDADE em localhost:6379 (docker/docker-compose.yml).
  Sem ele, tudo aqui falha por ERedisConnectionLost — e' o esperado, nao ha'
  como testar contra o servidor real sem o servidor real.

  O que so' da' para verificar aqui, e nao nas suites unitarias:

  - **Read timeout de verdade.** Um BLPOP mais longo que o timeout da conexao
    prova que o SO_RCVTIMEO foi mesmo aplicado no socket (o servidor falso nao
    tem socket, logo nao tem como estourar timeout de socket).
  - **Conexao contaminada.** Depois do timeout, a resposta atrasada do BLPOP
    ainda esta' a caminho. Este e' o teste que o roadmap pede desde o M0: a
    conexao seguinte NAO pode receber essa resposta no lugar da dela.
  - **Servidor derrubando conexao ociosa** (CLIENT KILL), que e' o motivo de o
    pool conferir a saude antes de emprestar.
  - **Concorrencia real**: varias threads emprestando e devolvendo do mesmo
    pool, com sockets de verdade.

  As chaves criadas usam o prefixo 'pascal-redis-faa:it:' e sao apagadas ao
  fim de cada teste — nada de FLUSHDB, que apagaria dados de quem estiver
  usando o mesmo servidor. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes,
  Redis.Types, Redis.Threading, Redis.Connection, Redis.Pool;

type
  { Roda comandos pelo pool numa thread propria. Guarda o erro em vez de
    deixa-lo escapar: excecao em thread de teste some sem deixar rastro, e o
    que interessa e' o teste falhar com a mensagem certa. }
  TRedisPoolWorkerThread = class(TThread)
  private
    FPool: TRedisPool;
    FIndex: Integer;
    FRounds: Integer;
    FOk: Integer;
    FErro: string;
  protected
    procedure Execute; override;
  public
    constructor Create(APool: TRedisPool; AIndex, ARounds: Integer);
    property Ok: Integer read FOk;
    property Erro: string read FErro;
  end;

  TRedisConnectionIntegrationTests = class(TTestCase)
  published
    procedure Conecta_ExecutaEFecha;
    procedure Binario_EAcentuacao_SobrevivemAoRoundTrip;
    procedure Pipeline_ContraServidorReal;
    procedure Resp3_HandshakeCompleto;
    procedure BancoInexistente_FalhaNoHandshake;
    procedure ReadTimeout_LevantaERedisTimeoutEInvalida;
    procedure ServidorDerrubaAConexao_ViraConnectionLost;
  end;

  TRedisPoolIntegrationTests = class(TTestCase)
  published
    procedure Acquire_ReusaAMesmaConexao;
    procedure ConexaoQueSofreuTimeout_NaoContaminaAProxima;
    procedure ConexaoDerrubadaPeloServidor_ETrocadaNoHealthCheck;
    procedure VariasThreads_CompartilhamOPool;
  end;

implementation

const
  PREFIXO = 'pascal-redis-faa:it:';

{ Helpers compartilhados }

function ParamsDeTeste: TRedisParams;
begin
  Result := RedisDefaultParams;
  Result.Host := 'localhost';
  Result.Port := REDIS_DEFAULT_PORT;
  Result.ClientName := 'pascal-redis-faa-it';
end;

function Chave(const ASufixo: string): string;
begin
  Result := PREFIXO + ASufixo;
end;

function MakeBytes(const AValues: array of Byte): TBytes;
var
  I: Integer;
begin
  Result := nil;
  SetLength(Result, Length(AValues));
  for I := 0 to High(AValues) do
    Result[I] := AValues[I];
end;

function Hex(const ABytes: TBytes): string;
var
  I: Integer;
begin
  Result := '';
  for I := 0 to High(ABytes) do
    Result := Result + IntToHex(ABytes[I], 2);
end;

// Conexao aberta com os parametros de teste. Quem chama e' dono.
function AbreConexao: TRedisConnection;
begin
  Result := TRedisConnection.Create(ParamsDeTeste);
  try
    Result.Open;
  except
    Result.Free;
    raise;
  end;
end;

// Apaga as chaves do teste sem tocar no resto do banco.
procedure Limpa(AConn: TRedisConnection; const ASufixos: array of string);
var
  LArgs: TRedisArgs;
  I: Integer;
begin
  SetLength(LArgs, Length(ASufixos) + 1);
  LArgs[0] := 'DEL';
  for I := 0 to High(ASufixos) do
    LArgs[I + 1] := Chave(ASufixos[I]);
  AConn.ExecuteArgs(LArgs);
end;

{ TRedisPoolWorkerThread }

constructor TRedisPoolWorkerThread.Create(APool: TRedisPool;
  AIndex, ARounds: Integer);
begin
  inherited Create(True);
  FreeOnTerminate := False;
  FPool := APool;
  FIndex := AIndex;
  FRounds := ARounds;
end;

procedure TRedisPoolWorkerThread.Execute;
var
  I: Integer;
  LConn: TRedisConnection;
  LChave, LValor: string;
begin
  try
    for I := 1 to FRounds do
    begin
      LChave := Chave('thread' + IntToStr(FIndex));
      LValor := IntToStr(FIndex) + '-' + IntToStr(I);
      LConn := FPool.Acquire;
      try
        LConn.Execute('SET', [LChave, LValor]);
        if LConn.Execute('GET', [LChave]).AsString <> LValor then
        begin
          FErro := 'valor trocado na thread ' + IntToStr(FIndex);
          Exit;
        end;
        Inc(FOk);
      finally
        FPool.Release(LConn);
      end;
    end;
  except
    on E: Exception do
      FErro := E.ClassName + ': ' + E.Message;
  end;
end;

{ TRedisConnectionIntegrationTests }

procedure TRedisConnectionIntegrationTests.Conecta_ExecutaEFecha;
var
  LConn: TRedisConnection;
begin
  LConn := AbreConexao;
  try
    TAssert.AssertTrue('devia abrir', LConn.IsOpen);
    TAssert.AssertTrue('PING', LConn.Ping);

    LConn.Execute('SET', [Chave('str'), 'valor']);
    TAssert.AssertEquals('valor', LConn.Execute('GET', [Chave('str')]).AsString);
    TAssert.AssertEquals(Int64(1), LConn.Execute('DEL', [Chave('str')]).AsInteger);
    TAssert.AssertTrue('chave ausente e nulo',
      LConn.Execute('GET', [Chave('str')]).IsNull);

    // O nome anunciado no handshake tem de estar la' do lado do servidor.
    TAssert.AssertEquals('pascal-redis-faa-it',
      LConn.Execute('CLIENT', ['GETNAME']).AsString);

    LConn.Close;
    TAssert.AssertFalse('devia fechar', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.Binario_EAcentuacao_SobrevivemAoRoundTrip;
var
  LConn: TRedisConnection;
  LOriginal: TBytes;
  LTexto: string;
begin
  LConn := AbreConexao;
  try
    // CRLF e zero no meio do valor: o que quebraria um comando inline.
    LOriginal := MakeBytes([65, 13, 10, 66, 0, 255, 200]);
    LConn.Execute('SET', [Chave('bin'), LOriginal]);
    TAssert.AssertEquals(Hex(LOriginal),
      Hex(LConn.Execute('GET', [Chave('bin')]).AsBytes));

    LTexto := 'ação e coração';
    LConn.Execute('SET', [Chave('utf8'), LTexto]);
    TAssert.AssertEquals(LTexto, LConn.Execute('GET', [Chave('utf8')]).AsString);

    Limpa(LConn, ['bin', 'utf8']);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.Pipeline_ContraServidorReal;
var
  LConn: TRedisConnection;
  LPipe: TRedisPipeline;
  LReplies: TRedisReplyArray;
begin
  LConn := AbreConexao;
  LPipe := TRedisPipeline.Create;
  try
    LPipe.Queue('DEL', [Chave('lista')]);
    LPipe.Queue('RPUSH', [Chave('lista'), 'a']);
    LPipe.Queue('RPUSH', [Chave('lista'), 'b']);
    LPipe.Queue('LRANGE', [Chave('lista'), 0, -1]);
    LReplies := LConn.ExecutePipeline(LPipe);

    TAssert.AssertEquals(4, Length(LReplies));
    TAssert.AssertEquals(Int64(2), LReplies[2].AsInteger);
    TAssert.AssertEquals(2, LReplies[3].Count);
    TAssert.AssertEquals('a', LReplies[3][0].AsString);
    TAssert.AssertEquals('b', LReplies[3][1].AsString);
    // O lote nao pode deixar resto no buffer: e' a diferenca entre pipeline e
    // conexao suja.
    TAssert.AssertFalse('nao pode sujar a conexao', LConn.IsDirty);
    TAssert.AssertTrue('e ela continua util', LConn.IsUsable);

    Limpa(LConn, ['lista']);
  finally
    LPipe.Free;
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.Resp3_HandshakeCompleto;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LMapa: IRedisReply;
begin
  LParams := ParamsDeTeste;
  LParams.Protocol := rpRESP3;
  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;
    TAssert.AssertTrue('devia negociar RESP3',
      LConn.NegotiatedProtocol = rpRESP3);
    TAssert.AssertTrue('o HELLO traz a versao', LConn.ServerVersion <> '');
    TAssert.AssertTrue('e o id da conexao', LConn.ServerId > 0);

    // O mapa achatado do M1 e' o que faz HGETALL ter a MESMA forma nos dois
    // protocolos: em RESP3 o kind muda, a contagem e o ValueByKey nao.
    LConn.Execute('DEL', [Chave('hash')]);
    LConn.Execute('HSET', [Chave('hash'), 'c1', 'v1', 'c2', 'v2']);
    LMapa := LConn.Execute('HGETALL', [Chave('hash')]);
    TAssert.AssertTrue('RESP3 devolve mapa', LMapa.Kind = rkMap);
    TAssert.AssertEquals('contagem achatada', 4, LMapa.Count);
    TAssert.AssertEquals('v1', LMapa.ValueByKey('c1').AsString);

    Limpa(LConn, ['hash']);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.BancoInexistente_FalhaNoHandshake;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LClasse: string;
begin
  // O SELECT do handshake tem de ser conferido: sem isso a conexao subiria
  // "aberta" no banco 0 e a app gravaria no lugar errado, calada.
  LParams := ParamsDeTeste;
  LParams.Database := 99;  // o padrao do Redis e' 16 bancos (0..15)
  LConn := TRedisConnection.Create(LParams);
  try
    LClasse := '';
    try
      LConn.Open;
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisReplyError', LClasse);
    TAssert.AssertFalse('nao pode ficar aberta', LConn.IsOpen);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.ReadTimeout_LevantaERedisTimeoutEInvalida;
var
  LConn: TRedisConnection;
  LParams: TRedisParams;
  LClasse: string;
  LInicio: UInt64;
  LDecorridoMs: UInt64;
begin
  // BLPOP numa chave que nao existe fica esperando ate' o timeout DELE (2 s).
  // Como o timeout da CONEXAO e' menor (300 ms), quem estoura primeiro e' o
  // socket. Sem SO_RCVTIMEO no transporte, esta thread ficaria 2 s parada — e,
  // num servidor que emudecesse de vez, ficaria parada para sempre.
  LParams := ParamsDeTeste;
  LParams.ReceiveTimeoutMs := 300;
  LConn := TRedisConnection.Create(LParams);
  try
    LConn.Open;
    LInicio := RedisTickMs;
    LClasse := '';
    try
      LConn.Execute('BLPOP', [Chave('fila-que-nao-existe'), 2]);
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    LDecorridoMs := RedisTickMs - LInicio;

    TAssert.AssertEquals('ERedisTimeout', LClasse);
    TAssert.AssertTrue('devia desistir bem antes dos 2 s do BLPOP: ' +
      IntToStr(LDecorridoMs) + ' ms', LDecorridoMs < 1500);
    // Timeout invalida: pode haver resposta a caminho, e reciclar contaminaria
    // o proximo comando.
    TAssert.AssertTrue('devia estar invalidada', LConn.IsBroken);
    TAssert.AssertFalse('e fora de uso', LConn.IsUsable);
  finally
    LConn.Free;
  end;
end;

procedure TRedisConnectionIntegrationTests.ServidorDerrubaAConexao_ViraConnectionLost;
var
  LVitima, LCarrasco: TRedisConnection;
  LId: Int64;
  LClasse: string;
begin
  // CLIENT KILL e' o que acontece de verdade num failover, num restart ou
  // quando o operador limpa conexoes penduradas. O cliente nao e' avisado: so'
  // descobre no proximo comando.
  LVitima := AbreConexao;
  LCarrasco := nil;
  try
    LId := LVitima.Execute('CLIENT', ['ID']).AsInteger;
    LCarrasco := AbreConexao;
    LCarrasco.Execute('CLIENT', ['KILL', 'ID', LId]);

    LClasse := '';
    try
      LVitima.Execute('PING');
    except
      on E: Exception do
        LClasse := E.ClassName;
    end;
    TAssert.AssertEquals('ERedisConnectionLost', LClasse);
    TAssert.AssertTrue('devia estar invalidada', LVitima.IsBroken);
  finally
    LCarrasco.Free;
    LVitima.Free;
  end;
end;

{ TRedisPoolIntegrationTests }

procedure TRedisPoolIntegrationTests.Acquire_ReusaAMesmaConexao;
var
  LPool: TRedisPool;
  LConn: TRedisConnection;
  LPoolParams: TRedisPoolParams;
  I: Integer;
begin
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.HealthCheckAfterIdleMs := -1;  // sem PING: mede so' o reuso
  LPool := TRedisPool.Create(ParamsDeTeste, LPoolParams);
  try
    for I := 1 to 5 do
    begin
      LConn := LPool.Acquire;
      try
        TAssert.AssertTrue('PING na volta ' + IntToStr(I), LConn.Ping);
      finally
        LPool.Release(LConn);
      end;
    end;
    TAssert.AssertEquals('cinco idas, um socket', Int64(1), LPool.CreatedCount);
    TAssert.AssertEquals('nada descartado', Int64(0), LPool.DiscardedCount);
    TAssert.AssertEquals(1, LPool.IdleCount);
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolIntegrationTests.ConexaoQueSofreuTimeout_NaoContaminaAProxima;
var
  LPool: TRedisPool;
  LParams: TRedisParams;
  LPoolParams: TRedisPoolParams;
  LConn: TRedisConnection;
  LReply: IRedisReply;
  LEstourou: Boolean;
begin
  // ESTE e' o teste de conexao-suja que o projeto persegue desde o inicio.
  //
  // O BLPOP sofre timeout do lado do cliente, mas o servidor NAO sabe disso:
  // ele ainda vai responder (na hora em que o BLPOP dele vencer). Se o pool
  // reciclasse essa conexao, o GET seguinte leria a resposta atrasada do BLPOP
  // — e a aplicacao receberia, calada, o valor errado. Com MaxSize = 1 o pool
  // e' obrigado a reusar a unica conexao que tem, entao se ele nao a destruir
  // o teste pega o erro na hora.
  LParams := ParamsDeTeste;
  LParams.ReceiveTimeoutMs := 300;
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 1;
  LPoolParams.HealthCheckAfterIdleMs := -1;  // sem rede de seguranca: o
                                             // descarte tem de vir do timeout
  LPool := TRedisPool.Create(LParams, LPoolParams);
  try
    LConn := LPool.Acquire;
    LEstourou := False;
    try
      try
        LConn.Execute('BLPOP', [Chave('fila-que-nao-existe'), 2]);
      except
        on E: ERedisTimeout do
          LEstourou := True;
      end;
    finally
      LPool.Release(LConn);
    end;
    TAssert.AssertTrue('o BLPOP devia ter estourado o timeout', LEstourou);
    TAssert.AssertEquals('a conexao com resposta a caminho foi descartada',
      Int64(1), LPool.DiscardedCount);
    TAssert.AssertEquals('e nao ficou ociosa', 0, LPool.IdleCount);

    // Conexao nova, do zero: o valor tem de ser o que este comando gravou, e
    // nao a resposta atrasada do BLPOP anterior.
    LConn := LPool.Acquire;
    try
      LConn.Execute('SET', [Chave('depois'), 'valor-novo']);
      LReply := LConn.Execute('GET', [Chave('depois')]);
      TAssert.AssertTrue('a resposta tem de ser uma bulk string',
        LReply.Kind = rkBulkString);
      TAssert.AssertEquals('valor-novo', LReply.AsString);
      TAssert.AssertFalse('e a conexao nova esta limpa', LConn.IsDirty);
      Limpa(LConn, ['depois']);
    finally
      LPool.Release(LConn);
    end;
  finally
    LPool.Free;
  end;
end;

procedure TRedisPoolIntegrationTests.ConexaoDerrubadaPeloServidor_ETrocadaNoHealthCheck;
var
  LPool: TRedisPool;
  LPoolParams: TRedisPoolParams;
  LConn, LCarrasco: TRedisConnection;
  LId: Int64;
begin
  // Conexao ociosa derrubada pelo servidor e' a falha classica de pool: ela
  // parece boa (o cliente nao foi avisado) e so' quebra no comando do usuario.
  // Com HealthCheckAfterIdleMs = 0, o pool confere antes de emprestar e troca.
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.HealthCheckAfterIdleMs := 0;
  LPool := TRedisPool.Create(ParamsDeTeste, LPoolParams);
  LCarrasco := nil;
  try
    LConn := LPool.Acquire;
    LId := LConn.Execute('CLIENT', ['ID']).AsInteger;
    LPool.Release(LConn);

    LCarrasco := AbreConexao;
    LCarrasco.Execute('CLIENT', ['KILL', 'ID', LId]);

    // O usuario nao pode ver nada disso: o Acquire devolve uma conexao boa.
    LConn := LPool.Acquire;
    try
      TAssert.AssertTrue('a conexao emprestada tem de funcionar', LConn.Ping);
      TAssert.AssertEquals('a morta foi descartada', Int64(1),
        LPool.DiscardedCount);
      TAssert.AssertEquals('e outra foi aberta no lugar', Int64(2),
        LPool.CreatedCount);
      TAssert.AssertEquals('sem inflar o pool', 1, LPool.TotalCount);
    finally
      LPool.Release(LConn);
    end;
  finally
    LCarrasco.Free;
    LPool.Free;
  end;
end;

procedure TRedisPoolIntegrationTests.VariasThreads_CompartilhamOPool;
const
  THREADS = 4;
  RODADAS = 25;
var
  LPool: TRedisPool;
  LPoolParams: TRedisPoolParams;
  LThreads: array[0..THREADS - 1] of TRedisPoolWorkerThread;
  LConn: TRedisConnection;
  LSufixos: array[0..THREADS - 1] of string;
  I: Integer;
  LTotalOk: Integer;
begin
  // Duas coisas de uma vez: que o pool aguenta concorrencia de verdade (lock,
  // espera, devolucao) e que o teto e' teto — com MaxSize menor que o numero
  // de threads, algumas TEM de esperar pela devolucao das outras.
  LPoolParams := RedisDefaultPoolParams;
  LPoolParams.MaxSize := 2;
  LPoolParams.AcquireTimeoutMs := 5000;
  LPool := TRedisPool.Create(ParamsDeTeste, LPoolParams);
  try
    for I := 0 to THREADS - 1 do
      LThreads[I] := TRedisPoolWorkerThread.Create(LPool, I, RODADAS);
    try
      for I := 0 to THREADS - 1 do
        LThreads[I].Start;
      for I := 0 to THREADS - 1 do
        LThreads[I].WaitFor;

      LTotalOk := 0;
      for I := 0 to THREADS - 1 do
      begin
        TAssert.AssertEquals('thread ' + IntToStr(I), '', LThreads[I].Erro);
        Inc(LTotalOk, LThreads[I].Ok);
      end;
      TAssert.AssertEquals('todas as rodadas completaram',
        THREADS * RODADAS, LTotalOk);
      TAssert.AssertTrue('o teto foi respeitado: ' + IntToStr(LPool.TotalCount),
        LPool.TotalCount <= LPoolParams.MaxSize);
      TAssert.AssertEquals('ninguem ficou emprestada', 0, LPool.InUseCount);
    finally
      for I := 0 to THREADS - 1 do
        LThreads[I].Free;
    end;

    LConn := LPool.Acquire;
    try
      for I := 0 to THREADS - 1 do
        LSufixos[I] := 'thread' + IntToStr(I);
      Limpa(LConn, LSufixos);
    finally
      LPool.Release(LConn);
    end;
  finally
    LPool.Free;
  end;
end;

initialization
  RegisterTest(TRedisConnectionIntegrationTests);
  RegisterTest(TRedisPoolIntegrationTests);

end.
