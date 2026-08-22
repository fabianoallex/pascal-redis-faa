unit Redis.Resp;

{ Codec RESP2/RESP3 da pascal-redis-faa: monta comandos e le respostas.

  Escrita e leitura sao assimetricas de proposito, porque o protocolo tambem e':
  o cliente SO' escreve comandos (array de bulk strings, o "unified request
  protocol") e SO' le respostas. Por isso ha uma funcao de codificacao de
  comando e uma classe de leitura, e nao um par simetrico.

  A leitura sai de um IRedisByteSource, nao de um socket. Duas razoes:

    1. Testabilidade sem rede. O TRedisBytesSource entrega os mesmos bytes em
       pedacos de tamanho controlado, o que reproduz leituras parciais de
       verdade — o modo de falha classico de parser de protocolo, e que numa
       LAN quase nunca acontece por acidente.
    2. A Redis.Resp nao depende da Redis.Transport. O adaptador socket->fonte
       nasce na Redis.Connection (M2), junto de quem sabe o que fazer quando a
       conexao morre.

  Nao ha maquina de estados incremental (do tipo "me de bytes, eu devolvo
  respostas prontas"): a arquitetura da lib le na propria thread chamadora, sob
  o lock da conexao (ver docs/DECISOES.md), entao bloquear esperando o resto da
  resposta e' exatamente o comportamento desejado. }

{$I redis.inc}

interface

uses
  SysUtils, Redis.Types;

const
  /// Teto de uma linha terminada em CRLF (cabecalhos, simple strings, erros).
  /// O Redis mantem essas linhas curtas; 64 KB sem CRLF a vista significa que
  /// o fluxo dessincronizou, e sem o teto a leitura cresceria o buffer ate' a
  /// memoria acabar.
  REDIS_MAX_LINE_LENGTH = 64 * 1024;

  /// Tamanho inicial do buffer de leitura. Cobre resposta pequena inteira numa
  /// syscall so'; cresce dobrando quando a resposta e' maior.
  REDIS_READ_BUFFER_SIZE = 8192;

type
  /// Fonte de bytes para o TRedisReader.
  ///
  /// O contrato e' o de read(2): devolve quantos bytes conseguiu, que pode ser
  /// MENOS do que o pedido, e 0 (ou negativo) quando a fonte acabou. Bloquear
  /// esperando pelo menos um byte e' permitido e esperado.
  IRedisByteSource = interface
    ['{1F9C4D82-57A6-4B33-8E71-2C0D9A5F63B4}']
    function ReadBytes(var ABuffer; ACount: Integer): Integer;
  end;

  /// Fonte que le de um TBytes ja' na memoria.
  ///
  /// AMaxChunk limita quantos bytes cada leitura devolve. E' o que torna as
  /// leituras parciais testaveis: com AMaxChunk = 1 o parser recebe a resposta
  /// byte a byte e qualquer suposicao de "a resposta chega inteira" quebra na
  /// hora. Zero (o padrao) entrega tudo o que couber.
  TRedisBytesSource = class(TInterfacedObject, IRedisByteSource)
  private
    FData: TBytes;
    FPos: Integer;
    FMaxChunk: Integer;
  public
    constructor Create(const AData: TBytes; AMaxChunk: Integer = 0); overload;
    constructor Create(const AData: string; AMaxChunk: Integer = 0); overload;
    function ReadBytes(var ABuffer; ACount: Integer): Integer;
    /// Quantos bytes ja' foram consumidos. Serve para checar, nos testes, que
    /// o parser leu a resposta inteira e nem um byte a mais.
    property Position: Integer read FPos;
  end;

  /// Leitor de respostas RESP.
  ///
  /// Uma instancia acompanha uma conexao a vida toda: o buffer interno guarda
  /// o que sobrou entre um ReadReply e o proximo (uma unica leitura de socket
  /// pode trazer varias respostas, e num pipeline traz mesmo).
  TRedisReader = class
  private
    FSource: IRedisByteSource;
    FBuf: TBytes;
    FHead: Integer;   // primeiro byte ainda nao consumido
    FTail: Integer;   // um alem do ultimo byte valido
    procedure Fill;
    procedure Compact;
    function ReadLine: TBytes;
    function ReadExact(ACount: Integer): TBytes;
    procedure ExpectCrLf;
    function ParseCount(const AText: string; const AWhat: string): Integer;
    function ReadNode(ADepth: Integer): IRedisReply;
    function ReadAggregate(AKind: TRedisReplyKind; ACount, ADepth: Integer): IRedisReply;
    function ReadBlob(const AHeader: string): TBytes;
  public
    constructor Create(const ASource: IRedisByteSource);

    /// Le uma resposta completa. Bloqueia ate' ela chegar inteira.
    /// Levanta ERedisProtocolError em bytes malformados e
    /// ERedisConnectionLost se a fonte acabar no meio de uma resposta.
    function ReadReply: IRedisReply;

    /// Bytes ja' lidos da fonte e ainda nao consumidos.
    ///
    /// Depois de ler a resposta de um comando isolado isto tem que ser zero.
    /// Se nao for, sobrou resposta orfa no fio (o caso de um comando que sofreu
    /// timeout e cuja resposta chegou depois), e devolver essa conexao ao pool
    /// contaminaria o proximo comando com a resposta do anterior. E' o bug
    /// classico de cliente Redis; o pool do M3 usa isto para detecta-lo.
    function Buffered: Integer;
  end;

/// Codifica um comando no unified request protocol: array de bulk strings.
///
/// E' o unico formato que a lib emite. O comando inline (texto separado por
/// espaco) seria mais curto, mas nao e' binario-seguro — um valor com espaco,
/// CR ou LF quebraria o comando em silencio.
function RedisEncodeCommand(const AArgs: array of TRedisArg): TBytes; overload;

/// Mesma coisa com o nome do comando destacado — a forma que o Execute usa:
/// RedisEncodeCommand('SET', ['chave', 'valor', 'EX', 60]).
function RedisEncodeCommand(const AName: string;
  const AArgs: array of TRedisArg): TBytes; overload;

/// Serializa uma arvore de resposta de volta para bytes RESP.
///
/// A lib em si nunca precisa disto — quem produz resposta e' o servidor. Existe
/// para os testes (round-trip de arvores arbitrarias) e para servidores falsos
/// de teste. Em rpRESP2 os tipos exclusivos do RESP3 sao rebaixados como um
/// servidor RESP2 os emitiria: nulo vira $-1, double e big number viram bulk
/// string, booleano vira :1 / :0, mapa vira array achatado, set e push viram
/// array. Assim da' para escrever um teste so' e roda-lo nos dois protocolos.
function RedisEncodeReply(const AReply: IRedisReply;
  AProtocol: TRedisProtocol): TBytes;

{ Construtores da arvore de resposta. Usados pelo leitor, pelos testes e, mais
  adiante, por MULTI/EXEC ao montar resultados sinteticos. }

function RedisNull: IRedisReply;
function RedisSimpleString(const AValue: string): IRedisReply;
function RedisError(const AMessage: string): IRedisReply;
function RedisInteger(const AValue: Int64): IRedisReply;
function RedisBulk(const AValue: TBytes): IRedisReply; overload;
function RedisBulk(const AValue: string): IRedisReply; overload;
function RedisVerbatim(const AFormat, AValue: string): IRedisReply;
function RedisDouble(const AValue: Double): IRedisReply;
function RedisBoolean(const AValue: Boolean): IRedisReply;
function RedisBigNumber(const AValue: string): IRedisReply;
function RedisArrayOf(const AItems: array of IRedisReply): IRedisReply;
function RedisMapOf(const AItems: array of IRedisReply): IRedisReply;
function RedisSetOf(const AItems: array of IRedisReply): IRedisReply;
function RedisPushOf(const AItems: array of IRedisReply): IRedisReply;

implementation

const
  CR = 13;
  LF = 10;

  // Prefixos de tipo do RESP. Os cinco primeiros sao RESP2; o resto entrou no
  // RESP3 (HELLO 3).
  RESP_SIMPLE_STRING = '+';
  RESP_ERROR         = '-';
  RESP_INTEGER       = ':';
  RESP_BULK_STRING   = '$';
  RESP_ARRAY         = '*';
  RESP_NULL          = '_';
  RESP_DOUBLE        = ',';
  RESP_BOOLEAN       = '#';
  RESP_BLOB_ERROR    = '!';
  RESP_VERBATIM      = '=';
  RESP_BIG_NUMBER    = '(';
  RESP_MAP           = '%';
  RESP_SET           = '~';
  RESP_ATTRIBUTE     = '|';
  RESP_PUSH          = '>';

type
  /// Porta de mutacao interna da arvore.
  ///
  /// So' existe por causa dos atributos ('|'): o leitor precisa pendurar o mapa
  /// numa resposta que ele ja' devolveu como IRedisReply, e converter interface
  /// de volta para a classe concreta nao e' portavel entre Delphi e FPC. Uma
  /// segunda interface resolve com QueryInterface, que se comporta igual nos
  /// dois. Fica na implementation: de fora, IRedisReply continua imutavel.
  IRedisReplyMutator = interface
    ['{0B7E5A16-9D34-42C8-A5F0-6E1C83B7D429}']
    procedure SetAttributes(const AValue: IRedisReply);
  end;

  /// Implementacao de IRedisReply. Fica na implementation de proposito: quem
  /// consome so' precisa da interface, e quem produz usa os construtores
  /// RedisXxx acima. Assim ninguem de fora consegue mutar uma resposta.
  TRedisReply = class(TInterfacedObject, IRedisReply, IRedisReplyMutator)
  private
    FKind: TRedisReplyKind;
    FBytes: TBytes;            // carga dos escalares (texto ou binario)
    FNumber: Int64;            // rkInteger
    FFloat: Double;            // rkDouble
    FFlag: Boolean;            // rkBoolean
    FVerbatimFormat: string;
    FItems: TRedisReplyArray;
    FAttributes: IRedisReply;
    procedure NeedScalar(const AOperation: string);
    procedure NeedAggregate(const AOperation: string);
    function ScalarText: string;
  public
    constructor CreateScalar(AKind: TRedisReplyKind; const ABytes: TBytes);
    constructor CreateAggregate(AKind: TRedisReplyKind;
      const AItems: TRedisReplyArray);

    function GetKind: TRedisReplyKind;
    function GetCount: Integer;
    function GetItem(AIndex: Integer): IRedisReply;
    function GetAttributes: IRedisReply;

    function IsNull: Boolean;
    function IsError: Boolean;
    function IsAggregate: Boolean;

    function AsBytes: TBytes;
    function AsString: string;
    function AsInteger: Int64;
    function AsDouble: Double;
    function AsBoolean: Boolean;

    function ErrorCode: string;
    function ErrorMessage: string;
    function VerbatimFormat: string;
    function ValueByKey(const AKey: string): IRedisReply;
    procedure RaiseIfError;

    procedure SetAttributes(const AValue: IRedisReply);
  end;

{ Helpers de bytes }

procedure AppendBytes(var ABuffer: TBytes; var ALength: Integer;
  const AData: TBytes);
var
  LNeed: Integer;
begin
  if Length(AData) = 0 then
    Exit;
  LNeed := ALength + Length(AData);
  if LNeed > Length(ABuffer) then
  begin
    // Dobra ate' caber: evita realocar uma vez por argumento num comando
    // com muitos argumentos (um MSET grande, um XADD com dezenas de campos).
    if Length(ABuffer) = 0 then
      SetLength(ABuffer, 64);
    while LNeed > Length(ABuffer) do
      SetLength(ABuffer, Length(ABuffer) * 2);
  end;
  Move(AData[0], ABuffer[ALength], Length(AData));
  ALength := LNeed;
end;

procedure AppendText(var ABuffer: TBytes; var ALength: Integer;
  const AText: string);
begin
  AppendBytes(ABuffer, ALength, RedisUtf8Encode(AText));
end;

procedure AppendCrLf(var ABuffer: TBytes; var ALength: Integer);
var
  LPair: TBytes;
begin
  SetLength(LPair, 2);
  LPair[0] := CR;
  LPair[1] := LF;
  AppendBytes(ABuffer, ALength, LPair);
end;

{ Construtores da arvore }

function RedisNull: IRedisReply;
begin
  Result := TRedisReply.CreateScalar(rkNull, nil);
end;

function RedisSimpleString(const AValue: string): IRedisReply;
begin
  Result := TRedisReply.CreateScalar(rkSimpleString, RedisUtf8Encode(AValue));
end;

function RedisError(const AMessage: string): IRedisReply;
begin
  Result := TRedisReply.CreateScalar(rkError, RedisUtf8Encode(AMessage));
end;

function RedisInteger(const AValue: Int64): IRedisReply;
var
  LReply: TRedisReply;
begin
  LReply := TRedisReply.CreateScalar(rkInteger, RedisUtf8Encode(IntToStr(AValue)));
  Result := LReply;  // assume a referencia antes de qualquer outra chamada
  LReply.FNumber := AValue;
end;

function RedisBulk(const AValue: TBytes): IRedisReply;
begin
  Result := TRedisReply.CreateScalar(rkBulkString, AValue);
end;

function RedisBulk(const AValue: string): IRedisReply;
begin
  Result := TRedisReply.CreateScalar(rkBulkString, RedisUtf8Encode(AValue));
end;

function RedisVerbatim(const AFormat, AValue: string): IRedisReply;
var
  LReply: TRedisReply;
begin
  LReply := TRedisReply.CreateScalar(rkVerbatim, RedisUtf8Encode(AValue));
  Result := LReply;  // assume a referencia antes de qualquer outra chamada
  LReply.FVerbatimFormat := AFormat;
end;

function RedisDouble(const AValue: Double): IRedisReply;
var
  LReply: TRedisReply;
begin
  LReply := TRedisReply.CreateScalar(rkDouble,
    RedisUtf8Encode(RedisFormatDouble(AValue)));
  Result := LReply;
  LReply.FFloat := AValue;
end;

function RedisBoolean(const AValue: Boolean): IRedisReply;
var
  LReply: TRedisReply;
  LText: string;
begin
  if AValue then
    LText := 't'
  else
    LText := 'f';
  LReply := TRedisReply.CreateScalar(rkBoolean, RedisUtf8Encode(LText));
  Result := LReply;
  LReply.FFlag := AValue;
end;

function RedisBigNumber(const AValue: string): IRedisReply;
begin
  Result := TRedisReply.CreateScalar(rkBigNumber, RedisUtf8Encode(AValue));
end;

function MakeAggregate(AKind: TRedisReplyKind;
  const AItems: array of IRedisReply): IRedisReply;
var
  LItems: TRedisReplyArray;
  I: Integer;
begin
  SetLength(LItems, Length(AItems));
  for I := 0 to High(AItems) do
    LItems[I] := AItems[I];
  Result := TRedisReply.CreateAggregate(AKind, LItems);
end;

function RedisArrayOf(const AItems: array of IRedisReply): IRedisReply;
begin
  Result := MakeAggregate(rkArray, AItems);
end;

function RedisMapOf(const AItems: array of IRedisReply): IRedisReply;
begin
  if Odd(Length(AItems)) then
    raise ERedisProtocolError.Create(
      'mapa precisa de um numero par de elementos (chave, valor, ...)');
  Result := MakeAggregate(rkMap, AItems);
end;

function RedisSetOf(const AItems: array of IRedisReply): IRedisReply;
begin
  Result := MakeAggregate(rkSet, AItems);
end;

function RedisPushOf(const AItems: array of IRedisReply): IRedisReply;
begin
  Result := MakeAggregate(rkPush, AItems);
end;

{ TRedisReply }

constructor TRedisReply.CreateScalar(AKind: TRedisReplyKind;
  const ABytes: TBytes);
begin
  inherited Create;
  FKind := AKind;
  FBytes := ABytes;
end;

constructor TRedisReply.CreateAggregate(AKind: TRedisReplyKind;
  const AItems: TRedisReplyArray);
begin
  inherited Create;
  FKind := AKind;
  FItems := AItems;
end;

function TRedisReply.GetKind: TRedisReplyKind;
begin
  Result := FKind;
end;

function TRedisReply.GetCount: Integer;
begin
  Result := Length(FItems);
end;

function TRedisReply.GetItem(AIndex: Integer): IRedisReply;
begin
  NeedAggregate('Items');
  if (AIndex < 0) or (AIndex >= Length(FItems)) then
    raise ERedisTypeError.CreateFmt(
      'indice %d fora dos limites: o %s tem %d elemento(s)',
      [AIndex, RedisReplyKindName(FKind), Length(FItems)]);
  Result := FItems[AIndex];
end;

function TRedisReply.GetAttributes: IRedisReply;
begin
  Result := FAttributes;
end;

procedure TRedisReply.SetAttributes(const AValue: IRedisReply);
begin
  FAttributes := AValue;
end;

function TRedisReply.IsNull: Boolean;
begin
  Result := FKind = rkNull;
end;

function TRedisReply.IsError: Boolean;
begin
  Result := FKind = rkError;
end;

function TRedisReply.IsAggregate: Boolean;
begin
  Result := FKind in [rkArray, rkMap, rkSet, rkPush];
end;

procedure TRedisReply.NeedScalar(const AOperation: string);
begin
  if IsAggregate then
    raise ERedisTypeError.CreateFmt('%s nao se aplica a um %s',
      [AOperation, RedisReplyKindName(FKind)]);
end;

procedure TRedisReply.NeedAggregate(const AOperation: string);
begin
  if not IsAggregate then
    raise ERedisTypeError.CreateFmt('%s so se aplica a agregados; este e um %s',
      [AOperation, RedisReplyKindName(FKind)]);
end;

function TRedisReply.ScalarText: string;
begin
  Result := RedisUtf8Decode(FBytes);
end;

function TRedisReply.AsBytes: TBytes;
begin
  NeedScalar('AsBytes');
  Result := FBytes;
end;

function TRedisReply.AsString: string;
begin
  NeedScalar('AsString');
  if FKind = rkNull then
    Result := ''
  else
    Result := ScalarText;
end;

function TRedisReply.AsInteger: Int64;
begin
  NeedScalar('AsInteger');
  case FKind of
    rkInteger:
      Result := FNumber;
    rkBoolean:
      Result := Ord(FFlag);
    rkDouble:
      begin
        // Um score inteiro (ZSCORE de 42) chega como double em RESP3 e como
        // bulk string em RESP2; aceitar o caso exato deixa o codigo da
        // aplicacao igual nos dois. Fracao seria perda silenciosa de dados.
        if (Frac(FFloat) <> 0) or (Abs(FFloat) > 9.2e18) then
          raise ERedisTypeError.CreateFmt(
            'o double %s nao e um inteiro exato', [ScalarText]);
        Result := Round(FFloat);
      end;
    rkNull:
      // Devolver 0 confundiria "chave ausente" com "chave que vale zero" — e'
      // justamente a distincao que o Redis faz questao de manter.
      raise ERedisTypeError.Create(
        'resposta nula nao tem valor inteiro; teste IsNull antes');
  else
    if not RedisTryParseInt64(ScalarText, Result) then
      raise ERedisTypeError.CreateFmt('%s nao e um inteiro', [ScalarText]);
  end;
end;

function TRedisReply.AsDouble: Double;
begin
  NeedScalar('AsDouble');
  case FKind of
    rkDouble:
      Result := FFloat;
    rkInteger:
      Result := FNumber;
    rkBoolean:
      Result := Ord(FFlag);
    rkNull:
      raise ERedisTypeError.Create(
        'resposta nula nao tem valor numerico; teste IsNull antes');
  else
    if not RedisTryParseDouble(ScalarText, Result) then
      raise ERedisTypeError.CreateFmt('%s nao e um numero', [ScalarText]);
  end;
end;

function TRedisReply.AsBoolean: Boolean;
var
  LText: string;
begin
  NeedScalar('AsBoolean');
  case FKind of
    rkBoolean:
      Result := FFlag;
    rkInteger:
      Result := FNumber <> 0;
    rkNull:
      // E' assim que o RESP2 diz "nao" a um SET NX: bulk string nula.
      Result := False;
  else
    LText := ScalarText;
    if LText = 'OK' then
      Result := True
    else if LText = '1' then
      Result := True
    else if (LText = '0') or (LText = '') then
      Result := False
    else
      raise ERedisTypeError.CreateFmt('%s nao e um booleano', [LText]);
  end;
end;

function TRedisReply.ErrorCode: string;
var
  LText: string;
  LSpace: Integer;
begin
  Result := '';
  if FKind <> rkError then
    Exit;
  LText := ScalarText;
  LSpace := Pos(' ', LText);
  if LSpace > 0 then
    Result := Copy(LText, 1, LSpace - 1)
  else
    Result := LText;
end;

function TRedisReply.ErrorMessage: string;
begin
  if FKind = rkError then
    Result := ScalarText
  else
    Result := '';
end;

function TRedisReply.VerbatimFormat: string;
begin
  Result := FVerbatimFormat;
end;

function TRedisReply.ValueByKey(const AKey: string): IRedisReply;
var
  I: Integer;
begin
  Result := nil;
  NeedAggregate('ValueByKey');
  I := 0;
  // Anda de dois em dois: o mapa e' guardado achatado, e um array RESP2 no
  // formato chave,valor,chave,valor tem exatamente a mesma forma.
  while I + 1 < Length(FItems) do
  begin
    if (FItems[I] <> nil) and (not FItems[I].IsAggregate) and
       (FItems[I].AsString = AKey) then
      Exit(FItems[I + 1]);
    Inc(I, 2);
  end;
end;

procedure TRedisReply.RaiseIfError;
begin
  if FKind = rkError then
    raise ERedisReplyError.CreateReply(ErrorCode, ErrorMessage);
end;

{ TRedisBytesSource }

constructor TRedisBytesSource.Create(const AData: TBytes; AMaxChunk: Integer);
begin
  inherited Create;
  FData := AData;
  FMaxChunk := AMaxChunk;
end;

constructor TRedisBytesSource.Create(const AData: string; AMaxChunk: Integer);
begin
  // Latin-1 byte a byte seria errado aqui: os fixtures de teste sao escritos
  // como texto e precisam virar exatamente os bytes que o servidor mandaria.
  Create(RedisUtf8Encode(AData), AMaxChunk);
end;

function TRedisBytesSource.ReadBytes(var ABuffer; ACount: Integer): Integer;
var
  LDest: PByte;
begin
  Result := Length(FData) - FPos;
  if Result <= 0 then
    Exit(0);
  if Result > ACount then
    Result := ACount;
  if (FMaxChunk > 0) and (Result > FMaxChunk) then
    Result := FMaxChunk;
  LDest := PByte(@ABuffer);
  Move(FData[FPos], LDest^, Result);
  Inc(FPos, Result);
end;

{ TRedisReader }

constructor TRedisReader.Create(const ASource: IRedisByteSource);
begin
  inherited Create;
  if ASource = nil then
    raise ERedisException.Create('TRedisReader exige uma fonte de bytes');
  FSource := ASource;
  SetLength(FBuf, REDIS_READ_BUFFER_SIZE);
end;

function TRedisReader.Buffered: Integer;
begin
  Result := FTail - FHead;
end;

procedure TRedisReader.Compact;
begin
  if FHead = 0 then
    Exit;
  if FTail > FHead then
    Move(FBuf[FHead], FBuf[0], FTail - FHead);
  FTail := FTail - FHead;
  FHead := 0;
end;

procedure TRedisReader.Fill;
var
  LRead: Integer;
begin
  Compact;
  if FTail = Length(FBuf) then
    SetLength(FBuf, Length(FBuf) * 2);
  LRead := FSource.ReadBytes(FBuf[FTail], Length(FBuf) - FTail);
  if LRead <= 0 then
    raise ERedisConnectionLost.Create(
      'a conexao terminou no meio de uma resposta');
  Inc(FTail, LRead);
end;

function TRedisReader.ReadLine: TBytes;
var
  LScan, I: Integer;
begin
  LScan := 0;  // quantos bytes a partir de FHead ja' foram conferidos
  repeat
    I := FHead + LScan;
    while I + 1 < FTail do
    begin
      if (FBuf[I] = CR) and (FBuf[I + 1] = LF) then
      begin
        Result := Copy(FBuf, FHead, I - FHead);
        FHead := I + 2;
        if FHead = FTail then
        begin
          FHead := 0;
          FTail := 0;
        end;
        Exit;
      end;
      Inc(I);
    end;
    // Para no ultimo byte: ele ainda pode ser o CR de um CRLF que chega na
    // proxima leitura, entao nao entra na contagem de "ja' conferido".
    LScan := I - FHead;
    if LScan > REDIS_MAX_LINE_LENGTH then
      raise ERedisProtocolError.CreateFmt(
        'linha sem CRLF depois de %d bytes: o fluxo dessincronizou', [LScan]);
    Fill;  // Compact pode zerar FHead, mas LScan e' relativo a ele
  until False;
end;

function TRedisReader.ReadExact(ACount: Integer): TBytes;
var
  LGot, LChunk, LRead: Integer;
begin
  Result := nil;
  if ACount = 0 then
    Exit;
  SetLength(Result, ACount);
  LGot := 0;
  while LGot < ACount do
  begin
    if FHead = FTail then
    begin
      // Carga grande com o buffer vazio: le direto no destino em vez de
      // passar por um buffer de 8 KB N vezes. Um valor de 10 MB agradece.
      if ACount - LGot >= Length(FBuf) then
      begin
        LRead := FSource.ReadBytes(Result[LGot], ACount - LGot);
        if LRead <= 0 then
          raise ERedisConnectionLost.Create(
            'a conexao terminou no meio de uma bulk string');
        Inc(LGot, LRead);
        Continue;
      end;
      Fill;
    end;
    LChunk := FTail - FHead;
    if LChunk > ACount - LGot then
      LChunk := ACount - LGot;
    Move(FBuf[FHead], Result[LGot], LChunk);
    Inc(FHead, LChunk);
    Inc(LGot, LChunk);
    if FHead = FTail then
    begin
      FHead := 0;
      FTail := 0;
    end;
  end;
end;

procedure TRedisReader.ExpectCrLf;
var
  LPair: TBytes;
begin
  LPair := ReadExact(2);
  if (LPair[0] <> CR) or (LPair[1] <> LF) then
    raise ERedisProtocolError.Create(
      'faltou o CRLF no fim da carga: o fluxo dessincronizou');
end;

function TRedisReader.ParseCount(const AText: string;
  const AWhat: string): Integer;
var
  LValue: Int64;
begin
  if (AText <> '') and (AText[1] = '?') then
    // Agregados e strings em streaming ('$?', '*?') existem no RESP3 mas o
    // servidor Redis nao os emite — so' clientes, ao enviar. Falhar claro e'
    // melhor do que travar esperando um terminador que nunca vem.
    raise ERedisProtocolError.Create(
      'resposta em streaming (comprimento "?") nao e suportada');
  if not RedisTryParseInt64(AText, LValue) then
    raise ERedisProtocolError.CreateFmt('%s: "%s" nao e um comprimento valido',
      [AWhat, AText]);
  if LValue < -1 then
    raise ERedisProtocolError.CreateFmt('%s: comprimento negativo %d',
      [AWhat, LValue]);
  if LValue > REDIS_MAX_BULK_LENGTH then
    raise ERedisProtocolError.CreateFmt('%s: comprimento %d acima do teto de %d',
      [AWhat, LValue, REDIS_MAX_BULK_LENGTH]);
  Result := LValue;
end;

function TRedisReader.ReadBlob(const AHeader: string): TBytes;
var
  LLength: Integer;
begin
  LLength := ParseCount(AHeader, 'bulk string');
  if LLength < 0 then
    raise ERedisProtocolError.Create('bulk string nula em contexto que nao aceita nulo');
  Result := ReadExact(LLength);
  ExpectCrLf;
end;

function TRedisReader.ReadAggregate(AKind: TRedisReplyKind;
  ACount, ADepth: Integer): IRedisReply;
var
  LItems: TRedisReplyArray;
  I: Integer;
begin
  SetLength(LItems, ACount);
  for I := 0 to ACount - 1 do
    LItems[I] := ReadNode(ADepth + 1);
  Result := TRedisReply.CreateAggregate(AKind, LItems);
end;

function TRedisReader.ReadNode(ADepth: Integer): IRedisReply;
var
  LLine: TBytes;
  LPrefix: Char;
  LText: string;
  LCount, LLength: Integer;
  LPayload: TBytes;
  LDouble: Double;
  LReply: TRedisReply;
  LAttributes: IRedisReply;
begin
  if ADepth > REDIS_MAX_DEPTH then
    raise ERedisProtocolError.CreateFmt(
      'resposta aninhada mais de %d niveis', [REDIS_MAX_DEPTH]);

  LLine := ReadLine;
  if Length(LLine) = 0 then
    raise ERedisProtocolError.Create('linha vazia onde se esperava uma resposta');

  LPrefix := Chr(LLine[0]);
  LText := RedisUtf8Decode(Copy(LLine, 1, Length(LLine) - 1));

  case LPrefix of
    RESP_SIMPLE_STRING:
      Result := TRedisReply.CreateScalar(rkSimpleString,
        Copy(LLine, 1, Length(LLine) - 1));

    RESP_ERROR:
      Result := TRedisReply.CreateScalar(rkError,
        Copy(LLine, 1, Length(LLine) - 1));

    RESP_INTEGER:
      begin
        LReply := TRedisReply.CreateScalar(rkInteger,
          Copy(LLine, 1, Length(LLine) - 1));
        Result := LReply;  // assume a referencia antes de poder levantar
        if not RedisTryParseInt64(LText, LReply.FNumber) then
          raise ERedisProtocolError.CreateFmt(
            'inteiro invalido: "%s"', [LText]);
      end;

    RESP_BULK_STRING:
      begin
        LLength := ParseCount(LText, 'bulk string');
        if LLength < 0 then
          Result := TRedisReply.CreateScalar(rkNull, nil)  // $-1: nulo do RESP2
        else
        begin
          LPayload := ReadExact(LLength);
          ExpectCrLf;
          Result := TRedisReply.CreateScalar(rkBulkString, LPayload);
        end;
      end;

    RESP_ARRAY:
      begin
        LCount := ParseCount(LText, 'array');
        if LCount < 0 then
          Result := TRedisReply.CreateScalar(rkNull, nil)  // *-1: nulo do RESP2
        else
          Result := ReadAggregate(rkArray, LCount, ADepth);
      end;

    RESP_NULL:
      begin
        if LText <> '' then
          raise ERedisProtocolError.CreateFmt(
            'nulo do RESP3 deve ser so "_": veio "_%s"', [LText]);
        Result := TRedisReply.CreateScalar(rkNull, nil);
      end;

    RESP_DOUBLE:
      begin
        if not RedisTryParseDouble(LText, LDouble) then
          raise ERedisProtocolError.CreateFmt('double invalido: "%s"', [LText]);
        LReply := TRedisReply.CreateScalar(rkDouble,
          Copy(LLine, 1, Length(LLine) - 1));
        Result := LReply;
        LReply.FFloat := LDouble;
      end;

    RESP_BOOLEAN:
      begin
        if (LText <> 't') and (LText <> 'f') then
          raise ERedisProtocolError.CreateFmt(
            'booleano deve ser "#t" ou "#f": veio "#%s"', [LText]);
        LReply := TRedisReply.CreateScalar(rkBoolean,
          Copy(LLine, 1, Length(LLine) - 1));
        Result := LReply;
        LReply.FFlag := LText = 't';
      end;

    RESP_BLOB_ERROR:
      // Mesmo kind do '-': para quem consome, um erro e' um erro. A diferenca
      // e' so' de codificacao — o blob error e' binario-seguro e pode conter
      // CRLF, o que o erro de linha unica nao pode.
      Result := TRedisReply.CreateScalar(rkError, ReadBlob(LText));

    RESP_VERBATIM:
      begin
        LPayload := ReadBlob(LText);
        // Formato: 3 caracteres, ':' e o conteudo — 'txt:ola' ou 'mkd:# ola'.
        if (Length(LPayload) < 4) or (LPayload[3] <> Ord(':')) then
          raise ERedisProtocolError.Create(
            'verbatim string sem o prefixo de formato "xxx:"');
        LReply := TRedisReply.CreateScalar(rkVerbatim,
          Copy(LPayload, 4, Length(LPayload) - 4));
        Result := LReply;
        LReply.FVerbatimFormat := RedisUtf8Decode(Copy(LPayload, 0, 3));
      end;

    RESP_BIG_NUMBER:
      begin
        // Nao cabe em Int64 por definicao; fica como texto e quem precisar que
        // use uma lib de precisao arbitraria.
        if LText = '' then
          raise ERedisProtocolError.Create('big number vazio');
        Result := TRedisReply.CreateScalar(rkBigNumber,
          Copy(LLine, 1, Length(LLine) - 1));
      end;

    RESP_MAP:
      begin
        LCount := ParseCount(LText, 'mapa');
        if LCount < 0 then
          raise ERedisProtocolError.Create('mapa com contagem negativa');
        // A contagem do RESP conta PARES; a arvore guarda achatado.
        Result := ReadAggregate(rkMap, LCount * 2, ADepth);
      end;

    RESP_SET:
      begin
        LCount := ParseCount(LText, 'set');
        if LCount < 0 then
          raise ERedisProtocolError.Create('set com contagem negativa');
        Result := ReadAggregate(rkSet, LCount, ADepth);
      end;

    RESP_PUSH:
      begin
        LCount := ParseCount(LText, 'push');
        if LCount < 0 then
          raise ERedisProtocolError.Create('push com contagem negativa');
        Result := ReadAggregate(rkPush, LCount, ADepth);
      end;

    RESP_ATTRIBUTE:
      begin
        // Por especificacao o atributo NAO e' uma resposta: e' metadado que
        // vem grudado na frente da resposta de verdade. Le o mapa, le a
        // resposta seguinte e pendura um no outro.
        LCount := ParseCount(LText, 'atributo');
        if LCount < 0 then
          raise ERedisProtocolError.Create('atributo com contagem negativa');
        LAttributes := ReadAggregate(rkMap, LCount * 2, ADepth);
        Result := ReadNode(ADepth);
        (Result as IRedisReplyMutator).SetAttributes(LAttributes);
      end;
  else
    raise ERedisProtocolError.CreateFmt(
      'prefixo RESP desconhecido: %s (byte %d)', [LPrefix, Ord(LPrefix)]);
  end;
end;

function TRedisReader.ReadReply: IRedisReply;
begin
  Result := ReadNode(0);
end;

{ Codificacao de comandos }

function RedisEncodeCommand(const AArgs: array of TRedisArg): TBytes;
var
  LLength, I: Integer;
begin
  if Length(AArgs) = 0 then
    raise ERedisException.Create('comando sem argumentos');
  Result := nil;
  LLength := 0;
  AppendText(Result, LLength, RESP_ARRAY + IntToStr(Length(AArgs)));
  AppendCrLf(Result, LLength);
  for I := 0 to High(AArgs) do
  begin
    AppendText(Result, LLength,
      RESP_BULK_STRING + IntToStr(Length(AArgs[I].Bytes)));
    AppendCrLf(Result, LLength);
    AppendBytes(Result, LLength, AArgs[I].Bytes);
    AppendCrLf(Result, LLength);
  end;
  SetLength(Result, LLength);
end;

function RedisEncodeCommand(const AName: string;
  const AArgs: array of TRedisArg): TBytes;
var
  LAll: TRedisArgs;
  I: Integer;
begin
  SetLength(LAll, Length(AArgs) + 1);
  LAll[0] := AName;
  for I := 0 to High(AArgs) do
    LAll[I + 1] := AArgs[I];
  Result := RedisEncodeCommand(LAll);
end;

{ Codificacao de respostas (testes e servidores falsos) }

procedure EncodeReplyInto(const AReply: IRedisReply; AProtocol: TRedisProtocol;
  var ABuffer: TBytes; var ALength: Integer);
var
  I, LPrefixLen: Integer;
  LPayload, LContent: TBytes;
  LKind: TRedisReplyKind;

  procedure EmitBlob(const APrefix: string; const ABytes: TBytes);
  begin
    AppendText(ABuffer, ALength, APrefix + IntToStr(Length(ABytes)));
    AppendCrLf(ABuffer, ALength);
    AppendBytes(ABuffer, ALength, ABytes);
    AppendCrLf(ABuffer, ALength);
  end;

  procedure EmitLine(const APrefix, AText: string);
  begin
    AppendText(ABuffer, ALength, APrefix + AText);
    AppendCrLf(ABuffer, ALength);
  end;

begin
  LKind := AReply.Kind;
  case LKind of
    rkNull:
      if AProtocol = rpRESP3 then
        EmitLine(RESP_NULL, '')
      else
        EmitLine(RESP_BULK_STRING, '-1');

    rkSimpleString:
      EmitLine(RESP_SIMPLE_STRING, AReply.AsString);

    rkError:
      EmitLine(RESP_ERROR, AReply.ErrorMessage);

    rkInteger:
      EmitLine(RESP_INTEGER, IntToStr(AReply.AsInteger));

    rkBulkString:
      EmitBlob(RESP_BULK_STRING, AReply.AsBytes);

    rkVerbatim:
      if AProtocol = rpRESP3 then
      begin
        // Concatena a mao: o operador '+' de array dinamico depende de
        // modeswitch no FPC 3.2 e nao vale a dependencia por tres linhas.
        LContent := AReply.AsBytes;
        LPayload := RedisUtf8Encode(AReply.VerbatimFormat + ':');
        LPrefixLen := Length(LPayload);
        SetLength(LPayload, LPrefixLen + Length(LContent));
        if Length(LContent) > 0 then
          Move(LContent[0], LPayload[LPrefixLen], Length(LContent));
        EmitBlob(RESP_VERBATIM, LPayload);
      end
      else
        EmitBlob(RESP_BULK_STRING, AReply.AsBytes);

    rkDouble:
      if AProtocol = rpRESP3 then
        EmitLine(RESP_DOUBLE, RedisFormatDouble(AReply.AsDouble))
      else
        EmitBlob(RESP_BULK_STRING,
          RedisUtf8Encode(RedisFormatDouble(AReply.AsDouble)));

    rkBoolean:
      if AProtocol = rpRESP3 then
      begin
        if AReply.AsBoolean then
          EmitLine(RESP_BOOLEAN, 't')
        else
          EmitLine(RESP_BOOLEAN, 'f');
      end
      else
        EmitLine(RESP_INTEGER, IntToStr(Ord(AReply.AsBoolean)));

    rkBigNumber:
      if AProtocol = rpRESP3 then
        EmitLine(RESP_BIG_NUMBER, AReply.AsString)
      else
        EmitBlob(RESP_BULK_STRING, AReply.AsBytes);

    rkArray, rkMap, rkSet, rkPush:
      begin
        if (LKind = rkMap) and (AProtocol = rpRESP3) then
          // A contagem do mapa e' em pares; a arvore guarda achatado.
          EmitLine(RESP_MAP, IntToStr(AReply.Count div 2))
        else if (LKind = rkSet) and (AProtocol = rpRESP3) then
          EmitLine(RESP_SET, IntToStr(AReply.Count))
        else if (LKind = rkPush) and (AProtocol = rpRESP3) then
          EmitLine(RESP_PUSH, IntToStr(AReply.Count))
        else
          EmitLine(RESP_ARRAY, IntToStr(AReply.Count));
        for I := 0 to AReply.Count - 1 do
          EncodeReplyInto(AReply.Items[I], AProtocol, ABuffer, ALength);
      end;
  else
    raise ERedisException.CreateFmt('kind %s nao codificavel',
      [RedisReplyKindName(LKind)]);
  end;
end;

function RedisEncodeReply(const AReply: IRedisReply;
  AProtocol: TRedisProtocol): TBytes;
var
  LLength: Integer;
begin
  if AReply = nil then
    raise ERedisException.Create('resposta nula (ponteiro) nao e codificavel');
  Result := nil;
  LLength := 0;
  EncodeReplyInto(AReply, AProtocol, Result, LLength);
  SetLength(Result, LLength);
end;

end.
