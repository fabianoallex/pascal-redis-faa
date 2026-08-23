unit Redis.PubSubTests;

{ Testes de Redis.PubSub (FPCUnit). Mesma cobertura do tests\Unit\Redis.PubSubTests.pas
  (DUnitX/Delphi) — as duas suites sao mantidas linha a linha equivalentes, entao
  toda mudanca aqui vai para la' na mesma sessao.

  Duas metades bem diferentes:

  1. A classificacao (RedisPubSubVerbOf, RedisParsePubSubMessage,
     RedisAllowedWhileSubscribed) e' funcao pura sobre arvore de resposta: sem
     thread, sem servidor, e roda igual nas duas formas do protocolo — array
     em RESP2, push em RESP3.

  2. O assinante inteiro sobe sobre um TRedisFakePubSubStream, um servidor de
     mentira que RESPONDE: ele interpreta o SUBSCRIBE que o cliente escreveu e
     devolve a confirmacao, como o Redis faria. Sem isso nao daria para testar
     nada do que importa aqui — confirmacao, ordem das mensagens, callback que
     levanta, queda de conexao — porque tudo depende do dialogo, nao de uma
     resposta roteirizada.

  O fake tem uma trava de seguranca: leitura sem dados desiste depois de dez
  segundos e devolve EOF. E' o que impede um teste mal escrito de pendurar a
  suite inteira, ja' que a thread de leitura fica parada esperando o servidor
  falar. }

{$mode delphi}{$H+}

interface

uses
  fpcunit, testregistry, SysUtils, Classes, SyncObjs,
  Redis.Types, Redis.Resp, Redis.Threading, Redis.Connection, Redis.Commands,
  Redis.PubSub;

type
  { Servidor de mentira que fala pub/sub.

    Diferente do TRedisFakeServerStream das outras suites, este nao tem
    roteiro: ele INTERPRETA o que o cliente escreve e responde de acordo
    (SUBSCRIBE/UNSUBSCRIBE e familia viram confirmacao; PING vira +PONG). Alem
    disso o teste pode injetar mensagens a qualquer momento, que e' o que um
    servidor de verdade faz quando outro cliente publica.

    A leitura BLOQUEIA enquanto nao ha' nada — como um socket em silencio. E'
    o unico jeito de exercitar a thread de leitura de verdade. }
  TRedisFakePubSubStream = class(TStream)
  private
    FLock: TCriticalSection;
    FData: TEvent;
    FOut: TBytes;
    FOutLen: Integer;
    FIn: TBytes;
    FInLen: Integer;
    FParsePos: Integer;
    FClosed: Boolean;
    FChannels: TStringList;
    FPatterns: TStringList;
    FShards: TStringList;
    /// Enfileira bytes para o cliente ler. Chamar SOB o lock.
    procedure Emit(const AData: TBytes);
    procedure EmitText(const AText: string);
    /// Interpreta os comandos completos que ja' chegaram. Chamar SOB o lock.
    procedure ParsePending;
    procedure HandleCommand(AArgs: TStringList);
    function TotalSubscriptions: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;

    /// Mensagem publicada por outro cliente (SUBSCRIBE).
    procedure PublishMessage(const AChannel, APayload: string);
    /// Mensagem que casou um padrao (PSUBSCRIBE).
    procedure PublishPattern(const APattern, AChannel, APayload: string);
    /// Mensagem de canal shardado (SSUBSCRIBE).
    procedure PublishShard(const AChannel, APayload: string);
    /// Mensagem com payload binario.
    procedure PublishBinary(const AChannel: string; const APayload: TBytes);
    /// Bytes RESP crus, para os casos que nao tem construtor pronto.
    procedure PublishRaw(const AText: string);
    /// Simula a queda da conexao: a leitura pendente devolve fim de fluxo.
    procedure CloseStream;
    /// Tudo o que o cliente escreveu, como texto.
    function WrittenText: string;
    procedure ClearWritten;
  end;

  { Guarda o que os callbacks receberam. Os callbacks rodam na thread de
    leitura, entao tudo aqui e' protegido por lock — e a espera e' por
    condicao, nunca por Sleep fixo, que renderia teste lento e instavel. }
  TRedisPubSubColetor = class
  private
    FLock: TCriticalSection;
    FMensagens: TStringList;
    FPadroes: TStringList;
    FErros: TStringList;
    FAssinaturas: TStringList;
    FUltimoPayload: TBytes;
    FDesconexoes: Integer;
    FLevantarNoCallback: Boolean;
    FAssinante: TRedisSubscriber;
    FTentaExecute: Boolean;
    FAssinaNoCallback: string;
    FErroDeExecute: string;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Mensagem(ASender: TObject; const AMessage: TRedisPubSubMessage);
    procedure Assinou(ASender: TObject; AKind: TRedisPubSubKind;
      const AName: string; ACount: Integer);
    procedure Cancelou(ASender: TObject; AKind: TRedisPubSubKind;
      const AName: string; ACount: Integer);
    procedure Erro(ASender: TObject; AError: Exception);
    procedure Desconectou(ASender: TObject);
    /// Espera chegarem ACount mensagens; False se o prazo estourar.
    function EsperaMensagens(ACount, ATimeoutMs: Integer): Boolean;
    function EsperaErros(ACount, ATimeoutMs: Integer): Boolean;
    /// Espera chegarem ACount eventos de assinatura/cancelamento.
    function EsperaEventos(ACount, ATimeoutMs: Integer): Boolean;
    function EsperaDesconexoes(ACount, ATimeoutMs: Integer): Boolean;
    function Mensagens: string;
    function Padroes: string;
    function Assinaturas: string;
    function Erros: string;
    function UltimoPayload: TBytes;
    property LevantarNoCallback: Boolean read FLevantarNoCallback
      write FLevantarNoCallback;
    /// O assinante que o callback usa nas duas travessuras abaixo.
    property Assinante: TRedisSubscriber read FAssinante write FAssinante;
    /// Faz o callback tentar um Execute — que tem de ser recusado por vir de
    /// dentro da thread de leitura, em vez de travar ate' o timeout.
    property TentaExecute: Boolean read FTentaExecute write FTentaExecute;
    /// Faz o callback assinar outro canal. Nao pode travar: de dentro do
    /// callback a assinatura sai sem esperar confirmacao.
    property AssinaNoCallback: string read FAssinaNoCallback
      write FAssinaNoCallback;
    function ErroDeExecute: string;
  end;

  TRedisPubSubParseTests = class(TTestCase)
  published
    procedure Message_EmResp2_EReconhecida;
    procedure Message_EmResp3_ComoPush;
    procedure PMessage_TemQuatroItens;
    procedure SMessage_EReconhecida;
    procedure Confirmacoes_DasSeisFormas;
    procedure AridadeErrada_NaoEPubSub;
    procedure ContagemQueNaoEInteiro_NaoEConfirmacao;
    procedure RespostaComum_NaoEPubSub;
    procedure Nulo_NaoEPubSub;
    procedure ParseMessage_PreencheCanalEPayload;
    procedure ParseMessage_PadraoTrazOCanalReal;
    procedure ParseMessage_PayloadBinarioIntacto;
    procedure ParseMessage_NaoEMensagem_DevolveFalse;
    procedure Permitido_EmModoDeAssinatura;
  end;

  TRedisSubscriberTests = class(TTestCase)
  published
    procedure Subscribe_EscreveOComandoNoFio;
    procedure Subscribe_EsperaAConfirmacaoDoServidor;
    procedure Subscribe_DeDentroDoCallback_NaoEspera;
    procedure Unsubscribe_TiraDaListaConfirmada;
    procedure Unsubscribe_SemArgumentos_LimpaTudo;
    procedure PSubscribe_RegistraPadrao;
    procedure SSubscribe_RegistraShard;
    procedure Mensagem_ChegaNoCallback;
    procedure Mensagens_ChegamNaOrdem;
    procedure MensagemDePadrao_TrazPadraoECanal;
    procedure MensagemBinaria_ChegaIntacta;
    procedure CallbackQueLevanta_NaoDerrubaOAssinante;
    procedure Execute_DeDentroDoCallback_Levanta;
    procedure Resp2_ComandoComumComAssinatura_Levanta;
    procedure Resp2_ComandoComumSemAssinatura_Passa;
    procedure Ping_RespondePong;
    procedure NomeVazio_Levanta;
    procedure SemStart_Levanta;
    procedure Queda_ZeraAsAssinaturasEAvisa;
    procedure Adotado_NaoReconecta;
    procedure Stop_EIdempotente;
  end;

implementation

const
  CRLF = #13#10;

{ Helpers de montagem RESP (os mesmos das outras suites, so' o necessario). }

function Bulk(const AValue: string): string;
begin
  Result := '$' + IntToStr(Length(RedisUtf8Encode(AValue))) + CRLF + AValue + CRLF;
end;

function Wire(const AArgs: array of string): string;
var
  I: Integer;
begin
  Result := '*' + IntToStr(Length(AArgs)) + CRLF;
  for I := 0 to High(AArgs) do
    Result := Result + Bulk(AArgs[I]);
end;

// Confirmacao de assinatura: verbo, nome e a contagem como INTEIRO.
function Confirmacao(const AVerb, AName: string; ACount: Integer): string;
begin
  Result := '*3' + CRLF + Bulk(AVerb) + Bulk(AName) + ':' + IntToStr(ACount) + CRLF;
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

{ TRedisFakePubSubStream }

constructor TRedisFakePubSubStream.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FData := TEvent.Create(nil, True, False, '');
  FChannels := TStringList.Create;
  FPatterns := TStringList.Create;
  FShards := TStringList.Create;
end;

destructor TRedisFakePubSubStream.Destroy;
begin
  FChannels.Free;
  FPatterns.Free;
  FShards.Free;
  FData.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TRedisFakePubSubStream.Emit(const AData: TBytes);
begin
  if Length(AData) = 0 then
    Exit;
  if FOutLen + Length(AData) > Length(FOut) then
    SetLength(FOut, (FOutLen + Length(AData)) * 2);
  Move(AData[0], FOut[FOutLen], Length(AData));
  Inc(FOutLen, Length(AData));
  FData.SetEvent;
end;

procedure TRedisFakePubSubStream.EmitText(const AText: string);
begin
  Emit(RedisUtf8Encode(AText));
end;

function TRedisFakePubSubStream.TotalSubscriptions: Integer;
begin
  Result := FChannels.Count + FPatterns.Count + FShards.Count;
end;

procedure TRedisFakePubSubStream.HandleCommand(AArgs: TStringList);
var
  LCmd: string;
  LLista: TStringList;
  LVerbo, LNome: string;
  I, LIndice: Integer;
  LAdicionando: Boolean;
begin
  if AArgs.Count = 0 then
    Exit;
  LCmd := UpperCase(AArgs[0]);

  if LCmd = 'PING' then
  begin
    // Em modo de assinatura o Redis responde ['pong', '']; fora dele, +PONG.
    if TotalSubscriptions > 0 then
      EmitText('*2' + CRLF + Bulk('pong') + Bulk(''))
    else
      EmitText('+PONG' + CRLF);
    Exit;
  end;

  if (LCmd = 'SUBSCRIBE') or (LCmd = 'UNSUBSCRIBE') then
    LLista := FChannels
  else if (LCmd = 'PSUBSCRIBE') or (LCmd = 'PUNSUBSCRIBE') then
    LLista := FPatterns
  else if (LCmd = 'SSUBSCRIBE') or (LCmd = 'SUNSUBSCRIBE') then
    LLista := FShards
  else
  begin
    // Qualquer outro comando: o fake responde +OK. Quem decide o que o
    // servidor recusaria em RESP2 e' o assinante, antes de mandar.
    EmitText('+OK' + CRLF);
    Exit;
  end;

  LAdicionando := (LCmd = 'SUBSCRIBE') or (LCmd = 'PSUBSCRIBE') or
    (LCmd = 'SSUBSCRIBE');
  LVerbo := LowerCase(LCmd);

  if AArgs.Count = 1 then
  begin
    // Cancelamento sem nome: uma confirmacao por assinatura, ou uma com nome
    // nulo se nao havia nenhuma — e' o que o servidor faz.
    if LLista.Count = 0 then
    begin
      // Nada assinado daquele tipo: o servidor confirma com o nome NULO.
      EmitText('*3' + CRLF + Bulk(LVerbo) + '$-1' + CRLF +
        ':' + IntToStr(TotalSubscriptions) + CRLF);
      Exit;
    end;
    while LLista.Count > 0 do
    begin
      LNome := LLista[0];
      LLista.Delete(0);
      EmitText(Confirmacao(LVerbo, LNome, TotalSubscriptions));
    end;
    Exit;
  end;

  for I := 1 to AArgs.Count - 1 do
  begin
    LIndice := LLista.IndexOf(AArgs[I]);
    if LAdicionando then
    begin
      if LIndice < 0 then
        LLista.Add(AArgs[I]);
    end
    else if LIndice >= 0 then
      LLista.Delete(LIndice);
    EmitText(Confirmacao(LVerbo, AArgs[I], TotalSubscriptions));
  end;
end;

procedure TRedisFakePubSubStream.ParsePending;
var
  LPos, LInicio, LArgc, I: Integer;
  LLinha: string;
  LArgs: TStringList;
  LIncompleto: Boolean;
  LTexto: string;

  // Le uma linha terminada em CRLF a partir de LPos (1-based). False quando o
  // comando ainda nao chegou inteiro.
  function TomaLinha(out ALinha: string): Boolean;
  var
    J: Integer;
  begin
    Result := False;
    J := LPos;
    while J < Length(LTexto) do
    begin
      if (LTexto[J] = #13) and (LTexto[J + 1] = #10) then
      begin
        ALinha := Copy(LTexto, LPos, J - LPos);
        LPos := J + 2;
        Exit(True);
      end;
      Inc(J);
    end;
  end;

begin
  LTexto := RedisUtf8Decode(Copy(FIn, 0, FInLen));
  LPos := FParsePos + 1;
  LArgs := TStringList.Create;
  try
    while LPos <= Length(LTexto) do
    begin
      LInicio := LPos;
      if not TomaLinha(LLinha) then
        Break;
      if (LLinha = '') or (LLinha[1] <> '*') then
        Break;   // o cliente da lib so' emite unified request protocol
      LArgc := StrToIntDef(Copy(LLinha, 2, Length(LLinha) - 1), -1);
      if LArgc < 0 then
        Break;
      LArgs.Clear;
      LIncompleto := False;
      for I := 1 to LArgc do
      begin
        if not TomaLinha(LLinha) then      // cabecalho '$n'
        begin
          LIncompleto := True;
          Break;
        end;
        if not TomaLinha(LLinha) then      // conteudo
        begin
          LIncompleto := True;
          Break;
        end;
        LArgs.Add(LLinha);
      end;
      if LIncompleto then
      begin
        LPos := LInicio;
        Break;
      end;
      FParsePos := LPos - 1;
      HandleCommand(LArgs);
    end;
  finally
    LArgs.Free;
  end;
end;

function TRedisFakePubSubStream.Read(var Buffer; Count: Longint): Longint;
var
  LDeadline: UInt64;
begin
  // Trava de seguranca: sem ela, um teste que esquece de fechar o fake
  // penduraria a suite, porque a thread de leitura fica esperando o servidor
  // falar — que e' exatamente o comportamento correto dela.
  LDeadline := RedisTickMs + 10000;
  while True do
  begin
    FLock.Enter;
    try
      if FOutLen > 0 then
      begin
        Result := Count;
        if Result > FOutLen then
          Result := FOutLen;
        Move(FOut[0], Buffer, Result);
        if Result < FOutLen then
          Move(FOut[Result], FOut[0], FOutLen - Result);
        Dec(FOutLen, Result);
        if FOutLen = 0 then
          FData.ResetEvent;
        Exit;
      end;
      if FClosed then
        Exit(0);
    finally
      FLock.Leave;
    end;
    if RedisTickMs >= LDeadline then
      Exit(0);
    FData.WaitFor(20);
  end;
end;

function TRedisFakePubSubStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result := Count;
  if Result <= 0 then
  begin
    Result := 0;
    Exit;
  end;
  FLock.Enter;
  try
    if FClosed then
      Exit(0);
    if FInLen + Result > Length(FIn) then
      SetLength(FIn, (FInLen + Result) * 2);
    Move(Buffer, FIn[FInLen], Result);
    Inc(FInLen, Result);
    // Responde como um servidor de verdade: na hora, no mesmo fluxo.
    ParsePending;
  finally
    FLock.Leave;
  end;
end;

function TRedisFakePubSubStream.Seek(const Offset: Int64;
  Origin: TSeekOrigin): Int64;
begin
  Result := 0;
  raise ERedisException.Create('o servidor falso nao suporta Seek');
end;

procedure TRedisFakePubSubStream.PublishMessage(const AChannel,
  APayload: string);
begin
  FLock.Enter;
  try
    EmitText('*3' + CRLF + Bulk('message') + Bulk(AChannel) + Bulk(APayload));
  finally
    FLock.Leave;
  end;
end;

procedure TRedisFakePubSubStream.PublishPattern(const APattern, AChannel,
  APayload: string);
begin
  FLock.Enter;
  try
    EmitText('*4' + CRLF + Bulk('pmessage') + Bulk(APattern) + Bulk(AChannel) +
      Bulk(APayload));
  finally
    FLock.Leave;
  end;
end;

procedure TRedisFakePubSubStream.PublishShard(const AChannel, APayload: string);
begin
  FLock.Enter;
  try
    EmitText('*3' + CRLF + Bulk('smessage') + Bulk(AChannel) + Bulk(APayload));
  finally
    FLock.Leave;
  end;
end;

procedure TRedisFakePubSubStream.PublishBinary(const AChannel: string;
  const APayload: TBytes);
begin
  FLock.Enter;
  try
    EmitText('*3' + CRLF + Bulk('message') + Bulk(AChannel) +
      '$' + IntToStr(Length(APayload)) + CRLF);
    Emit(APayload);
    EmitText(CRLF);
  finally
    FLock.Leave;
  end;
end;

procedure TRedisFakePubSubStream.PublishRaw(const AText: string);
begin
  FLock.Enter;
  try
    EmitText(AText);
  finally
    FLock.Leave;
  end;
end;

procedure TRedisFakePubSubStream.CloseStream;
begin
  FLock.Enter;
  try
    FClosed := True;
    FData.SetEvent;   // acorda a leitura pendurada para ela ver o EOF
  finally
    FLock.Leave;
  end;
end;

function TRedisFakePubSubStream.WrittenText: string;
begin
  FLock.Enter;
  try
    Result := RedisUtf8Decode(Copy(FIn, 0, FInLen));
  finally
    FLock.Leave;
  end;
end;

procedure TRedisFakePubSubStream.ClearWritten;
begin
  FLock.Enter;
  try
    FInLen := 0;
    FParsePos := 0;
    SetLength(FIn, 0);
  finally
    FLock.Leave;
  end;
end;

{ TRedisPubSubColetor }

constructor TRedisPubSubColetor.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FMensagens := TStringList.Create;
  FPadroes := TStringList.Create;
  FErros := TStringList.Create;
  FAssinaturas := TStringList.Create;
end;

destructor TRedisPubSubColetor.Destroy;
begin
  FMensagens.Free;
  FPadroes.Free;
  FErros.Free;
  FAssinaturas.Free;
  FLock.Free;
  inherited Destroy;
end;

procedure TRedisPubSubColetor.Mensagem(ASender: TObject;
  const AMessage: TRedisPubSubMessage);
var
  LAssinante: TRedisSubscriber;
begin
  FLock.Enter;
  try
    LAssinante := FAssinante;
  finally
    FLock.Leave;
  end;

  // As travessuras vem ANTES de registrar a mensagem, e nao depois: quem
  // espera do lado de fora espera pela CONTAGEM de mensagens, entao registrar
  // primeiro deixaria o teste ler o resultado da travessura antes de ela
  // acontecer.
  if (LAssinante <> nil) and FTentaExecute then
  begin
    // Chamar Execute daqui e' erro de uso: quem leria a resposta e' esta
    // mesma thread. Tem de vir excecao clara, nao travamento. PING de
    // proposito — e' comando PERMITIDO em modo de assinatura, entao o unico
    // motivo possivel para a recusa e' a chamada vir da thread de leitura.
    try
      LAssinante.Execute('PING', []);
      FErroDeExecute := '(nao levantou)';
    except
      on E: Exception do
        FErroDeExecute := E.ClassName;
    end;
  end;

  if (LAssinante <> nil) and (FAssinaNoCallback <> '') then
  begin
    LAssinante.Subscribe([FAssinaNoCallback]);
    FAssinaNoCallback := '';
  end;

  FLock.Enter;
  try
    FMensagens.Add(AMessage.Channel + '=' + AMessage.Text);
    FPadroes.Add(AMessage.Pattern);
    FUltimoPayload := Copy(AMessage.Payload, 0, Length(AMessage.Payload));
  finally
    FLock.Leave;
  end;

  if FLevantarNoCallback then
    raise ERedisException.Create('estouro proposital dentro do callback');
end;

procedure TRedisPubSubColetor.Assinou(ASender: TObject;
  AKind: TRedisPubSubKind; const AName: string; ACount: Integer);
begin
  FLock.Enter;
  try
    FAssinaturas.Add('+' + AName + ':' + IntToStr(ACount));
  finally
    FLock.Leave;
  end;
end;

procedure TRedisPubSubColetor.Cancelou(ASender: TObject;
  AKind: TRedisPubSubKind; const AName: string; ACount: Integer);
begin
  FLock.Enter;
  try
    FAssinaturas.Add('-' + AName + ':' + IntToStr(ACount));
  finally
    FLock.Leave;
  end;
end;

procedure TRedisPubSubColetor.Erro(ASender: TObject; AError: Exception);
begin
  FLock.Enter;
  try
    FErros.Add(AError.ClassName);
  finally
    FLock.Leave;
  end;
end;

procedure TRedisPubSubColetor.Desconectou(ASender: TObject);
begin
  FLock.Enter;
  try
    Inc(FDesconexoes);
  finally
    FLock.Leave;
  end;
end;

function TRedisPubSubColetor.EsperaMensagens(ACount,
  ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
  LTem: Integer;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    FLock.Enter;
    try
      LTem := FMensagens.Count;
    finally
      FLock.Leave;
    end;
    if LTem >= ACount then
      Exit(True);
    Sleep(5);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

function TRedisPubSubColetor.EsperaEventos(ACount,
  ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
  LTem: Integer;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    FLock.Enter;
    try
      LTem := FAssinaturas.Count;
    finally
      FLock.Leave;
    end;
    if LTem >= ACount then
      Exit(True);
    Sleep(5);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

function TRedisPubSubColetor.EsperaErros(ACount, ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
  LTem: Integer;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    FLock.Enter;
    try
      LTem := FErros.Count;
    finally
      FLock.Leave;
    end;
    if LTem >= ACount then
      Exit(True);
    Sleep(5);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

function TRedisPubSubColetor.EsperaDesconexoes(ACount,
  ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
  LTem: Integer;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    FLock.Enter;
    try
      LTem := FDesconexoes;
    finally
      FLock.Leave;
    end;
    if LTem >= ACount then
      Exit(True);
    Sleep(5);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

// As listas viram texto com '|' entre os itens: assim um AssertEquals so'
// mostra a sequencia inteira quando falha, em vez de esconder a ordem.
function JuntaLista(AList: TStringList; ALock: TCriticalSection): string;
var
  I: Integer;
begin
  Result := '';
  ALock.Enter;
  try
    for I := 0 to AList.Count - 1 do
    begin
      if I > 0 then
        Result := Result + '|';
      Result := Result + AList[I];
    end;
  finally
    ALock.Leave;
  end;
end;

function TRedisPubSubColetor.Mensagens: string;
begin
  Result := JuntaLista(FMensagens, FLock);
end;

function TRedisPubSubColetor.Padroes: string;
begin
  Result := JuntaLista(FPadroes, FLock);
end;

function TRedisPubSubColetor.Assinaturas: string;
begin
  Result := JuntaLista(FAssinaturas, FLock);
end;

function TRedisPubSubColetor.Erros: string;
begin
  Result := JuntaLista(FErros, FLock);
end;

function TRedisPubSubColetor.UltimoPayload: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FUltimoPayload, 0, Length(FUltimoPayload));
  finally
    FLock.Leave;
  end;
end;

function TRedisPubSubColetor.ErroDeExecute: string;
begin
  Result := FErroDeExecute;
end;

// Monta assinante + coletor sobre o servidor falso, ja' iniciado. O assinante
// e' dono da conexao, e a conexao, do stream — por isso o fake so' pode ser
// tocado enquanto o assinante viver.
function NovoAssinante(out AFake: TRedisFakePubSubStream;
  out AColetor: TRedisPubSubColetor): TRedisSubscriber;
var
  LConn: TRedisConnection;
begin
  AFake := TRedisFakePubSubStream.Create;
  LConn := TRedisConnection.CreateOnStream(AFake, RedisDefaultParams);
  AColetor := TRedisPubSubColetor.Create;
  try
    Result := TRedisSubscriber.CreateOnConnection(LConn, True);
  except
    LConn.Free;
    AColetor.Free;
    raise;
  end;
  Result.OnMessage := AColetor.Mensagem;
  Result.OnSubscribed := AColetor.Assinou;
  Result.OnUnsubscribed := AColetor.Cancelou;
  Result.OnError := AColetor.Erro;
  Result.OnDisconnected := AColetor.Desconectou;
  Result.SubscribeTimeoutMs := 3000;
  Result.CommandTimeoutMs := 3000;
  Result.Start;
end;

// Espera o servidor confirmar ACount assinaturas de canal.
function EsperaAssinaturas(ASub: TRedisSubscriber; ACount,
  ATimeoutMs: Integer): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := RedisTickMs + UInt64(ATimeoutMs);
  repeat
    if Length(ASub.Channels) >= ACount then
      Exit(True);
    Sleep(5);
  until RedisTickMs >= LDeadline;
  Result := False;
end;

// Encerra na ordem certa: fecha o fake (que desbloqueia a leitura) e so'
// depois libera o assinante, que e' quem destroi conexao e stream.
procedure EncerraAssinante(ASub: TRedisSubscriber;
  AFake: TRedisFakePubSubStream; AColetor: TRedisPubSubColetor);
begin
  if AFake <> nil then
    AFake.CloseStream;
  ASub.Free;
  AColetor.Free;
end;

{ TRedisPubSubParseTests }

procedure TRedisPubSubParseTests.Message_EmResp2_EReconhecida;
begin
  TAssert.AssertTrue('array de 3 com verbo message',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('message'), RedisBulk('c'),
      RedisBulk('oi')])) = pvMessage);
end;

procedure TRedisPubSubParseTests.Message_EmResp3_ComoPush;
begin
  // Mesma mensagem, outro envelope: em RESP3 o servidor marca como push.
  TAssert.AssertTrue('push de 3 com verbo message',
    RedisPubSubVerbOf(RedisPushOf([RedisBulk('message'), RedisBulk('c'),
      RedisBulk('oi')])) = pvMessage);
end;

procedure TRedisPubSubParseTests.PMessage_TemQuatroItens;
begin
  TAssert.AssertTrue('pmessage com 4 itens',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('pmessage'), RedisBulk('c.*'),
      RedisBulk('c.x'), RedisBulk('oi')])) = pvPMessage);
  // Com 3 itens nao e' pmessage nenhum.
  TAssert.AssertTrue('pmessage com 3 itens nao vale',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('pmessage'), RedisBulk('c.*'),
      RedisBulk('oi')])) = pvNone);
end;

procedure TRedisPubSubParseTests.SMessage_EReconhecida;
begin
  TAssert.AssertTrue('smessage',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('smessage'), RedisBulk('c'),
      RedisBulk('oi')])) = pvSMessage);
end;

procedure TRedisPubSubParseTests.Confirmacoes_DasSeisFormas;
begin
  TAssert.AssertTrue('subscribe',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('subscribe'), RedisBulk('c'),
      RedisInteger(1)])) = pvSubscribe);
  TAssert.AssertTrue('unsubscribe',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('unsubscribe'), RedisBulk('c'),
      RedisInteger(0)])) = pvUnsubscribe);
  TAssert.AssertTrue('psubscribe',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('psubscribe'), RedisBulk('c.*'),
      RedisInteger(1)])) = pvPSubscribe);
  TAssert.AssertTrue('punsubscribe',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('punsubscribe'), RedisBulk('c.*'),
      RedisInteger(0)])) = pvPUnsubscribe);
  TAssert.AssertTrue('ssubscribe',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('ssubscribe'), RedisBulk('c'),
      RedisInteger(1)])) = pvSSubscribe);
  TAssert.AssertTrue('sunsubscribe',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('sunsubscribe'), RedisBulk('c'),
      RedisInteger(0)])) = pvSUnsubscribe);
end;

procedure TRedisPubSubParseTests.AridadeErrada_NaoEPubSub;
begin
  // 'message' com 2 itens nao existe no protocolo: e' array qualquer.
  TAssert.AssertTrue('message com 2 itens',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('message'),
      RedisBulk('c')])) = pvNone);
  TAssert.AssertTrue('subscribe com 4 itens',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('subscribe'), RedisBulk('c'),
      RedisInteger(1), RedisBulk('sobra')])) = pvNone);
end;

procedure TRedisPubSubParseTests.ContagemQueNaoEInteiro_NaoEConfirmacao;
begin
  // Este e' o caso traicoeiro: um PUBSUB CHANNELS pode devolver tres canais e
  // o primeiro chamar-se 'subscribe'. O que separa e' o terceiro item — numa
  // confirmacao ele e' SEMPRE inteiro.
  TAssert.AssertTrue('tres bulks nao sao confirmacao',
    RedisPubSubVerbOf(RedisArrayOf([RedisBulk('subscribe'), RedisBulk('message'),
      RedisBulk('outro')])) = pvNone);
end;

procedure TRedisPubSubParseTests.RespostaComum_NaoEPubSub;
begin
  TAssert.AssertTrue('inteiro', RedisPubSubVerbOf(RedisInteger(7)) = pvNone);
  TAssert.AssertTrue('bulk', RedisPubSubVerbOf(RedisBulk('message')) = pvNone);
  TAssert.AssertTrue('simple string',
    RedisPubSubVerbOf(RedisSimpleString('OK')) = pvNone);
  TAssert.AssertTrue('array vazio',
    RedisPubSubVerbOf(RedisArrayOf([])) = pvNone);
end;

procedure TRedisPubSubParseTests.Nulo_NaoEPubSub;
begin
  TAssert.AssertTrue('nulo', RedisPubSubVerbOf(RedisNull) = pvNone);
  TAssert.AssertTrue('nil', RedisPubSubVerbOf(nil) = pvNone);
end;

procedure TRedisPubSubParseTests.ParseMessage_PreencheCanalEPayload;
var
  LMsg: TRedisPubSubMessage;
begin
  TAssert.AssertTrue('parseou', RedisParsePubSubMessage(
    RedisArrayOf([RedisBulk('message'), RedisBulk('noticias'),
      RedisBulk('bom dia')]), LMsg));
  TAssert.AssertTrue('kind de canal', LMsg.Kind = pkChannel);
  TAssert.AssertEquals('noticias', LMsg.Channel);
  TAssert.AssertEquals('', LMsg.Pattern);
  TAssert.AssertEquals('bom dia', LMsg.Text);
end;

procedure TRedisPubSubParseTests.ParseMessage_PadraoTrazOCanalReal;
var
  LMsg: TRedisPubSubMessage;
begin
  // Quem assinou 'noticias.*' precisa saber de qual canal veio.
  TAssert.AssertTrue('parseou', RedisParsePubSubMessage(
    RedisArrayOf([RedisBulk('pmessage'), RedisBulk('noticias.*'),
      RedisBulk('noticias.esporte'), RedisBulk('gol')]), LMsg));
  TAssert.AssertTrue('kind de padrao', LMsg.Kind = pkPattern);
  TAssert.AssertEquals('noticias.*', LMsg.Pattern);
  TAssert.AssertEquals('noticias.esporte', LMsg.Channel);
  TAssert.AssertEquals('gol', LMsg.Text);
end;

procedure TRedisPubSubParseTests.ParseMessage_PayloadBinarioIntacto;
var
  LMsg: TRedisPubSubMessage;
  LBin: TBytes;
begin
  // Byte zero e CRLF no meio: o payload e' binario, nao texto.
  LBin := MontaBytes([0, 13, 10, 255, 65]);
  TAssert.AssertTrue('parseou', RedisParsePubSubMessage(
    RedisArrayOf([RedisBulk('message'), RedisBulk('c'), RedisBulk(LBin)]),
    LMsg));
  TAssert.AssertTrue('payload intacto', BytesIguais(LBin, LMsg.Payload));
end;

procedure TRedisPubSubParseTests.ParseMessage_NaoEMensagem_DevolveFalse;
var
  LMsg: TRedisPubSubMessage;
begin
  TAssert.AssertFalse(RedisParsePubSubMessage(
    RedisArrayOf([RedisBulk('subscribe'), RedisBulk('c'), RedisInteger(1)]),
    LMsg));
  TAssert.AssertFalse(RedisParsePubSubMessage(RedisInteger(1), LMsg));
end;

procedure TRedisPubSubParseTests.Permitido_EmModoDeAssinatura;
begin
  TAssert.AssertTrue(RedisAllowedWhileSubscribed('SUBSCRIBE'));
  TAssert.AssertTrue(RedisAllowedWhileSubscribed('unsubscribe'));
  TAssert.AssertTrue(RedisAllowedWhileSubscribed('PING'));
  TAssert.AssertTrue(RedisAllowedWhileSubscribed('reset'));
  TAssert.AssertTrue(RedisAllowedWhileSubscribed('QUIT'));
  TAssert.AssertFalse(RedisAllowedWhileSubscribed('GET'));
  TAssert.AssertFalse(RedisAllowedWhileSubscribed('PUBLISH'));
end;

{ TRedisSubscriberTests }

procedure TRedisSubscriberTests.Subscribe_EscreveOComandoNoFio;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a', 'b']);
    TAssert.AssertEquals(Wire(['SUBSCRIBE', 'a', 'b']), LFake.WrittenText);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Subscribe_EsperaAConfirmacaoDoServidor;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
  LCanais: TRedisStringArray;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    // Quando o Subscribe volta, o servidor JA' confirmou: nao ha' corrida com
    // o primeiro PUBLISH.
    LSub.Subscribe(['a', 'b']);
    TAssert.AssertEquals(2, LSub.SubscriptionCount);
    LCanais := LSub.Channels;
    TAssert.AssertEquals(2, Length(LCanais));
    TAssert.AssertEquals('a', LCanais[0]);
    TAssert.AssertEquals('b', LCanais[1]);
    // O evento vem DEPOIS do estado, de proposito: a lib atualiza a lista sob
    // o monitor e so' entao chama o callback, ja' fora dele — nenhum callback
    // roda segurando lock. Entao o OnSubscribed pode estar um fio atras do
    // que o Subscribe esperou, e o teste espera por ele.
    TAssert.AssertTrue('os dois eventos de confirmacao chegaram',
      LColetor.EsperaEventos(2, 3000));
    TAssert.AssertEquals('+a:1|+b:2', LColetor.Assinaturas);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Subscribe_DeDentroDoCallback_NaoEspera;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a']);
    LColetor.Assinante := LSub;
    LColetor.AssinaNoCallback := 'b';
    LFake.PublishMessage('a', 'oi');
    TAssert.AssertTrue('chegou', LColetor.EsperaMensagens(1, 3000));
    // Assinar de dentro do callback NAO espera confirmacao — quem confirmaria
    // e' esta mesma thread, logo adiante no laco. A confirmacao chega depois,
    // sozinha, e a lista se atualiza.
    TAssert.AssertTrue('a segunda assinatura foi confirmada',
      EsperaAssinaturas(LSub, 2, 3000));
    TAssert.AssertEquals('b', LSub.Channels[1]);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Unsubscribe_TiraDaListaConfirmada;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a', 'b']);
    LSub.Unsubscribe(['a']);
    TAssert.AssertEquals(1, LSub.SubscriptionCount);
    TAssert.AssertEquals('b', LSub.Channels[0]);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Unsubscribe_SemArgumentos_LimpaTudo;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a', 'b', 'c']);
    LSub.Unsubscribe;
    TAssert.AssertEquals(0, LSub.SubscriptionCount);
    TAssert.AssertEquals(0, Length(LSub.Channels));
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.PSubscribe_RegistraPadrao;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.PSubscribe(['noticias.*']);
    TAssert.AssertEquals(Wire(['PSUBSCRIBE', 'noticias.*']), LFake.WrittenText);
    TAssert.AssertEquals(1, Length(LSub.Patterns));
    TAssert.AssertEquals('noticias.*', LSub.Patterns[0]);
    // Padrao nao entra na lista de canais: sao contas separadas no servidor.
    TAssert.AssertEquals(0, Length(LSub.Channels));
    LSub.PUnsubscribe(['noticias.*']);
    TAssert.AssertEquals(0, Length(LSub.Patterns));
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.SSubscribe_RegistraShard;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.SSubscribe(['s1']);
    TAssert.AssertEquals(Wire(['SSUBSCRIBE', 's1']), LFake.WrittenText);
    TAssert.AssertEquals(1, Length(LSub.ShardChannels));
    TAssert.AssertEquals('s1', LSub.ShardChannels[0]);
    LSub.SUnsubscribe(['s1']);
    TAssert.AssertEquals(0, Length(LSub.ShardChannels));
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Mensagem_ChegaNoCallback;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['noticias']);
    LFake.PublishMessage('noticias', 'bom dia');
    TAssert.AssertTrue('chegou', LColetor.EsperaMensagens(1, 3000));
    TAssert.AssertEquals('noticias=bom dia', LColetor.Mensagens);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Mensagens_ChegamNaOrdem;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
  LEsperado: string;
  I: Integer;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    // Ordem e' o unico compromisso que o pub/sub do Redis cumpre, e o
    // despacho na propria thread de leitura existe para preserva-la. Se um
    // dia isso virar pool de workers, este teste cai.
    LSub.Subscribe(['fila']);
    LEsperado := '';
    for I := 1 to 20 do
    begin
      if I > 1 then
        LEsperado := LEsperado + '|';
      LEsperado := LEsperado + 'fila=' + IntToStr(I);
      LFake.PublishMessage('fila', IntToStr(I));
    end;
    TAssert.AssertTrue('chegaram as 20', LColetor.EsperaMensagens(20, 5000));
    TAssert.AssertEquals(LEsperado, LColetor.Mensagens);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.MensagemDePadrao_TrazPadraoECanal;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.PSubscribe(['noticias.*']);
    LFake.PublishPattern('noticias.*', 'noticias.esporte', 'gol');
    TAssert.AssertTrue('chegou', LColetor.EsperaMensagens(1, 3000));
    TAssert.AssertEquals('noticias.esporte=gol', LColetor.Mensagens);
    TAssert.AssertEquals('noticias.*', LColetor.Padroes);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.MensagemBinaria_ChegaIntacta;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
  LBin: TBytes;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    // CRLF no meio do payload: se algo aqui trabalhasse por linha, quebraria.
    LBin := MontaBytes([1, 13, 10, 0, 200, 65]);
    LSub.Subscribe(['bin']);
    LFake.PublishBinary('bin', LBin);
    TAssert.AssertTrue('chegou', LColetor.EsperaMensagens(1, 3000));
    TAssert.AssertTrue('payload intacto',
      BytesIguais(LBin, LColetor.UltimoPayload));
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.CallbackQueLevanta_NaoDerrubaOAssinante;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    // Bug na aplicacao nao pode virar reconexao: a excecao vai para o OnError
    // e a proxima mensagem e' entregue normalmente.
    LColetor.LevantarNoCallback := True;
    LSub.Subscribe(['a']);
    LFake.PublishMessage('a', 'um');
    TAssert.AssertTrue('erro reportado', LColetor.EsperaErros(1, 3000));
    LFake.PublishMessage('a', 'dois');
    TAssert.AssertTrue('a segunda tambem chegou',
      LColetor.EsperaMensagens(2, 3000));
    TAssert.AssertEquals('a=um|a=dois', LColetor.Mensagens);
    TAssert.AssertTrue('conexao continua de pe', LSub.Connected);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Execute_DeDentroDoCallback_Levanta;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a']);
    LColetor.Assinante := LSub;
    LColetor.TentaExecute := True;
    LFake.PublishMessage('a', 'oi');
    TAssert.AssertTrue('chegou', LColetor.EsperaMensagens(1, 3000));
    // Sem a guarda, isto seria um travamento ate' o CommandTimeoutMs: a
    // thread que leria a resposta e' a que esta' rodando o callback.
    TAssert.AssertEquals('ERedisPubSubError', LColetor.ErroDeExecute);
  finally
    LColetor.TentaExecute := False;
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Resp2_ComandoComumComAssinatura_Levanta;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
  LLevantou: Boolean;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a']);
    LLevantou := False;
    try
      LSub.Execute('GET', ['x']);
    except
      on E: ERedisPubSubError do
        LLevantou := True;
    end;
    // O servidor recusaria de qualquer jeito; recusar aqui rende mensagem que
    // explica o que fazer, e nao um erro cru do Redis.
    TAssert.AssertTrue('recusado antes de ir ao fio', LLevantou);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Resp2_ComandoComumSemAssinatura_Passa;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
  LReply: IRedisReply;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    // Sem assinatura ativa, a conexao RESP2 ainda e' uma conexao comum.
    LReply := LSub.Execute('GET', ['x']);
    TAssert.AssertEquals('OK', LReply.AsString);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Ping_RespondePong;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    TAssert.AssertTrue('fora do modo de assinatura', LSub.Ping);
    LSub.Subscribe(['a']);
    // Em modo de assinatura o PONG vem dentro de um array; os dois contam.
    TAssert.AssertTrue('dentro do modo de assinatura', LSub.Ping);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.NomeVazio_Levanta;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
  LLevantou: Boolean;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LLevantou := False;
    try
      LSub.Subscribe(['a', '']);
    except
      on E: ERedisPubSubError do
        LLevantou := True;
    end;
    TAssert.AssertTrue('canal vazio e recusado', LLevantou);
    // E nada foi para o fio: a validacao acontece antes de mandar.
    TAssert.AssertEquals('', LFake.WrittenText);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.SemStart_Levanta;
var
  LFake: TRedisFakePubSubStream;
  LConn: TRedisConnection;
  LSub: TRedisSubscriber;
  LLevantou: Boolean;
begin
  LFake := TRedisFakePubSubStream.Create;
  LConn := TRedisConnection.CreateOnStream(LFake, RedisDefaultParams);
  LSub := TRedisSubscriber.CreateOnConnection(LConn, True);
  try
    LLevantou := False;
    try
      LSub.Subscribe(['a']);
    except
      on E: ERedisPubSubError do
        LLevantou := True;
    end;
    TAssert.AssertTrue('assinar sem Start e recusado', LLevantou);
  finally
    LFake.CloseStream;
    LSub.Free;
  end;
end;

procedure TRedisSubscriberTests.Queda_ZeraAsAssinaturasEAvisa;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a']);
    TAssert.AssertEquals(1, LSub.SubscriptionCount);
    LFake.CloseStream;    // o servidor sumiu
    TAssert.AssertTrue('avisou a queda', LColetor.EsperaDesconexoes(1, 3000));
    // As assinaturas confirmadas morrem com a conexao: elas vivem NO
    // SERVIDOR, e o servidor esqueceu.
    TAssert.AssertEquals(0, LSub.SubscriptionCount);
    TAssert.AssertFalse(LSub.Connected);
    TAssert.AssertTrue('erro registrado', LSub.LastError <> '');
  finally
    EncerraAssinante(LSub, nil, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Adotado_NaoReconecta;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    // Conexao vinda de fora pode nem ser de socket: reabrir sairia
    // conectando em outro lugar.
    TAssert.AssertFalse(LSub.AutoReconnect);
  finally
    EncerraAssinante(LSub, LFake, LColetor);
  end;
end;

procedure TRedisSubscriberTests.Stop_EIdempotente;
var
  LSub: TRedisSubscriber;
  LFake: TRedisFakePubSubStream;
  LColetor: TRedisPubSubColetor;
begin
  LSub := NovoAssinante(LFake, LColetor);
  try
    LSub.Subscribe(['a']);
    LFake.CloseStream;
    LSub.Stop;
    TAssert.AssertFalse('parou', LSub.Active);
    LSub.Stop;              // o segundo nao pode explodir
    TAssert.AssertFalse('continua parado', LSub.Active);
  finally
    LSub.Free;
    LColetor.Free;
  end;
end;

initialization
  RegisterTest(TRedisPubSubParseTests);
  RegisterTest(TRedisSubscriberTests);

end.
