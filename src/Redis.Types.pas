unit Redis.Types;

{ Tipos fundamentais da pascal-redis-faa: a arvore de respostas (IRedisReply),
  os argumentos de comando (TRedisArg), as excecoes e os parametros de conexao.

  Esta unit nao conhece socket nem protocolo — e' so' o vocabulario. Quem fala
  RESP e' a Redis.Resp; quem fala TCP e' a Redis.Transport.

  Por que IRedisReply e nao TValue: o TValue rendeu dois erros internos do FPC
  3.2.2 no porte da pascal-amqp-faa (e o contorno AmqpUnwrapValue). A arvore
  RESP e' pequena e fechada, entao vale modelar a mao. Sendo interface, o
  refcount libera a arvore inteira sozinho — as suites rodam com zero leaks sem
  ninguem cacar Free. Ver docs/DECISOES.md. }

{$I redis.inc}

interface

uses
  SysUtils, Math;

const
  /// Porta padrao do Redis em texto claro.
  REDIS_DEFAULT_PORT = 6379;

  /// Porta convencional do Redis sob TLS (a que o docker-compose.tls.yml usa).
  REDIS_DEFAULT_TLS_PORT = 6380;

  /// Teto de uma bulk string na leitura: 512 MB, o mesmo limite que o Redis
  /// impoe a um valor (proto-max-bulk-len). Comprimento acima disso nao vem de
  /// servidor sao — vem de buffer dessincronizado ou de servidor hostil — e
  /// alocar antes de desconfiar seria um jeito barato de derrubar o processo.
  REDIS_MAX_BULK_LENGTH = 512 * 1024 * 1024;

  /// Profundidade maxima de aninhamento de agregados numa resposta. O Redis
  /// real nao passa de meia duzia de niveis; o limite existe porque a leitura
  /// e' recursiva e um '*1' repetido ao infinito estouraria a pilha.
  REDIS_MAX_DEPTH = 64;

  /// Banco padrao (o SELECT so' e' emitido quando o parametro difere deste).
  REDIS_DEFAULT_DATABASE = 0;

type
  /// Versao do protocolo falada na conexao. RESP2 e' o padrao (todo servidor
  /// entende); RESP3 e' opt-in via HELLO 3 e traz tipos proprios e o push '>'
  /// fora de banda, que permite pub/sub sem sequestrar a conexao.
  TRedisProtocol = (rpRESP2, rpRESP3);

  /// Tipo de um no' da arvore de resposta.
  ///
  /// Os tipos exclusivos do RESP3 aparecem aqui mesmo quando a conexao fala
  /// RESP2 — simplesmente nunca sao produzidos. O mapeamento byte->kind:
  ///
  ///   +  rkSimpleString    -  rkError         :  rkInteger
  ///   $  rkBulkString      *  rkArray
  ///   _  rkNull  (RESP3)   ,  rkDouble        #  rkBoolean
  ///   !  rkError  (blob error: binario-seguro, mesmo kind do '-')
  ///   =  rkVerbatim        (  rkBigNumber
  ///   %  rkMap             ~  rkSet           >  rkPush
  ///
  /// O atributo '|' nao vira um kind: por especificacao ele nao e' uma
  /// resposta, e' metadado que PRECEDE a resposta real. A leitura anexa o
  /// mapa em IRedisReply.Attributes da resposta seguinte.
  ///
  /// Os nulos do RESP2 ($-1 e *-1) tambem viram rkNull, e nao "bulk vazia" /
  /// "array de zero itens": confundir nulo com vazio e' a diferenca entre
  /// "a chave nao existe" e "a chave existe e vale ''".
  TRedisReplyKind = (
    rkNull,
    rkSimpleString,
    rkError,
    rkInteger,
    rkBulkString,
    rkVerbatim,
    rkDouble,
    rkBoolean,
    rkBigNumber,
    rkArray,
    rkMap,
    rkSet,
    rkPush);

  /// Raiz de todas as excecoes da lib.
  ERedisException = class(Exception);

  /// Os bytes recebidos nao formam RESP valido: prefixo desconhecido,
  /// comprimento nao numerico, agregado aninhado fundo demais, fim de fluxo no
  /// meio de uma resposta. Depois disso o buffer esta dessincronizado e a
  /// conexao nao tem mais conserto — quem pegar esta excecao deve DESTRUIR a
  /// conexao, nunca devolve-la ao pool.
  ERedisProtocolError = class(ERedisException);

  /// O servidor respondeu um erro ('-' ou '!'): WRONGTYPE, NOSCRIPT, NOAUTH,
  /// READONLY, MOVED... A conexao continua sa' — isto e' uma resposta valida,
  /// so' que negativa. Levantada por IRedisReply.RaiseIfError.
  ERedisReplyError = class(ERedisException)
  private
    FCode: string;
  public
    constructor CreateReply(const ACode, AMessage: string);
    /// Primeira palavra do erro em maiusculas ('WRONGTYPE', 'ERR', 'MOVED').
    /// E' o que da' para testar em codigo; o resto da mensagem e' prosa.
    property Code: string read FCode;
  end;

  /// A resposta veio, mas nao no formato pedido — AsInteger num array,
  /// AsInteger num texto nao numerico, Items num escalar. Indica bug de quem
  /// chama (ou comando trocado), nao problema de conexao.
  ERedisTypeError = class(ERedisException);

  /// A conexao morreu no meio da operacao. O comando em voo NAO e'
  /// re-executado: INCR, LPUSH e SETNX nao sao idempotentes e repetir em
  /// silencio corromperia os dados. A decisao de repetir e' de quem chamou.
  ERedisConnectionLost = class(ERedisException);

  /// Estourou o read/write timeout do socket. Como pode haver resposta orfa a
  /// caminho, a conexao e' descartada, nunca reciclada.
  ERedisTimeout = class(ERedisException);

  IRedisReply = interface;

  /// Array de respostas. Alias nomeado para nao esbarrar no parser do FPC 3.2
  /// com generics aninhados em expressao.
  TRedisReplyArray = array of IRedisReply;

  /// Um no' da arvore de resposta RESP.
  ///
  /// Imutavel do ponto de vista de quem consome, e com contagem de referencia:
  /// guardar um item de um array mantem o array vivo, e soltar a raiz libera
  /// tudo. Os acessores As* convertem quando faz sentido (AsString de um
  /// rkInteger devolve os digitos) e levantam ERedisTypeError quando nao.
  IRedisReply = interface
    ['{6A3B7C41-2E5D-4F18-9C0A-8D6E1B4F7A20}']
    function GetKind: TRedisReplyKind;
    function GetCount: Integer;
    function GetItem(AIndex: Integer): IRedisReply;
    function GetAttributes: IRedisReply;

    /// Tipo deste no'.
    property Kind: TRedisReplyKind read GetKind;

    /// Numero de elementos de um agregado (rkArray, rkMap, rkSet, rkPush);
    /// 0 nos escalares.
    ///
    /// Num rkMap conta os elementos ACHATADOS, nao os pares: um mapa de 2
    /// pares tem Count = 4, com chave em 0 e 2 e valor em 1 e 3. E' de
    /// proposito — assim HGETALL, CONFIG GET e XPENDING tem exatamente a
    /// mesma forma em RESP2 (array achatado) e em RESP3 (mapa), e o codigo da
    /// aplicacao nao precisa perguntar qual protocolo esta em uso.
    property Count: Integer read GetCount;

    /// Elemento de um agregado, 0-based. Levanta ERedisTypeError num escalar
    /// ou fora dos limites.
    property Items[AIndex: Integer]: IRedisReply read GetItem; default;

    /// Mapa de atributos ('|') que precedia esta resposta, ou nil — o caso
    /// normal. O Redis usa atributos para metadado opcional, como o
    /// 'key-popularity' do client-side caching; ignora-los e' sempre seguro.
    property Attributes: IRedisReply read GetAttributes;

    /// True para rkNull. Vale para os tres nulos: '_' do RESP3 e os '$-1' /
    /// '*-1' do RESP2.
    function IsNull: Boolean;

    /// True para rkError (venha do '-' ou do '!').
    function IsError: Boolean;

    /// True para os agregados: rkArray, rkMap, rkSet e rkPush.
    function IsAggregate: Boolean;

    /// Conteudo cru, sem interpretar codepage. E' a leitura binario-segura —
    /// use esta quando o valor pode nao ser texto (imagem, protobuf, um blob
    /// com CRLF no meio). Num rkNull devolve nil.
    function AsBytes: TBytes;

    /// Conteudo decodificado como UTF-8. Num rkInteger devolve os digitos; num
    /// rkDouble, o numero formatado; num rkBoolean, 't' ou 'f'; num rkNull,
    /// string vazia — se a diferenca entre nulo e '' importa (e no Redis quase
    /// sempre importa), teste IsNull antes.
    ///
    /// Em valor que NAO e' UTF-8 valido (um JPEG, um protobuf) isto NAO
    /// levanta: os bytes indecifraveis viram caractere de substituicao e a
    /// vida segue. Levantar ali transformaria um engano brando — chamar
    /// AsString onde cabia AsBytes — em excecao no meio de um callback de
    /// pub/sub, por exemplo. Para valor binario o caminho e' AsBytes.
    function AsString: string;

    /// Valor inteiro. Aceita rkInteger e, por conveniencia, escalares de texto
    /// que contenham so' digitos (o retorno de um script Lua, por exemplo).
    function AsInteger: Int64;

    /// Valor em ponto flutuante. Aceita rkDouble, rkInteger e escalares de
    /// texto numericos — o mesmo score de um ZSCORE chega como bulk string em
    /// RESP2 e como double em RESP3. Entende 'inf', '-inf' e 'nan'.
    function AsDouble: Double;

    /// Valor booleano. Aceita rkBoolean, rkInteger (0 = False), rkNull (False,
    /// que e' como o RESP2 nega um SET NX) e a simple string 'OK'.
    function AsBoolean: Boolean;

    /// Primeira palavra de um rkError ('WRONGTYPE'); '' nos demais kinds.
    function ErrorCode: string;

    /// Texto completo de um rkError, codigo incluido; '' nos demais kinds.
    function ErrorMessage: string;

    /// Prefixo de formato de um rkVerbatim: 'txt' para texto puro, 'mkd' para
    /// Markdown (e' o que o LATENCY DOCTOR devolve em RESP3). '' nos demais.
    /// AsBytes/AsString ja' devolvem o conteudo SEM este prefixo.
    function VerbatimFormat: string;

    /// Valor associado a uma chave num rkMap (ou num rkArray achatado no
    /// formato chave,valor,chave,valor — a forma RESP2 do mesmo dado).
    /// Devolve nil se a chave nao existir. Comparacao sensivel a maiusculas,
    /// como o Redis devolve.
    function ValueByKey(const AKey: string): IRedisReply;

    /// Levanta ERedisReplyError se este no' for rkError; nao faz nada nos
    /// demais. E' o jeito curto de propagar erro do servidor.
    procedure RaiseIfError;
  end;

  /// Um argumento de comando, sempre em bytes.
  ///
  /// Os operadores Implicit fazem 'SET', 42 e um TBytes conviverem no mesmo
  /// construtor de array — Execute('SET', ['chave', 'valor', 'EX', 60]) —
  /// mantendo o contrato binario-seguro: o que chega como TBytes vai no fio
  /// byte a byte, e o que chega como string passa por RedisUtf8Encode.
  TRedisArg = record
  public
    Bytes: TBytes;
    class operator Implicit(const AValue: string): TRedisArg;
    class operator Implicit(const AValue: Int64): TRedisArg;
    class operator Implicit(const AValue: Integer): TRedisArg;
    class operator Implicit(const AValue: Double): TRedisArg;
    class operator Implicit(const AValue: Boolean): TRedisArg;
    class operator Implicit(const AValue: TBytes): TRedisArg;
  end;

  TRedisArgs = array of TRedisArg;

  /// Parametros de uma conexao. Preencha com RedisDefaultParams e ajuste o que
  /// interessa — assim campos novos em versoes futuras nascem com valor sao.
  TRedisParams = record
    Host: string;
    Port: Word;
    /// Usuario ACL (Redis 6+). Vazio faz o AUTH ir no formato de senha unica,
    /// que e' o que o 'requirepass' entende.
    Username: string;
    Password: string;
    /// Banco a selecionar apos o handshake. So' emite SELECT se diferir de 0.
    Database: Integer;
    /// Nome anunciado em CLIENT SETNAME — aparece no CLIENT LIST e economiza
    /// horas quando varias apps dividem o mesmo servidor.
    ClientName: string;
    /// RESP2 (padrao, universal) ou RESP3 (HELLO 3).
    Protocol: TRedisProtocol;

    /// Cifra a conexao com TLS. O Redis NAO faz upgrade em banda (nao existe
    /// STARTTLS): o servidor escuta TLS numa porta separada, entao ligar isto
    /// quase sempre anda junto com trocar a Port para REDIS_DEFAULT_TLS_PORT.
    ///
    /// O backend e' decidido em compilacao — SChannel no Windows, OpenSSL com
    /// -dREDIS_OPENSSL em qualquer plataforma; ver RedisTlsBackendName. Num
    /// build sem backend nenhum, abrir a conexao levanta ERedisTls em vez de
    /// cair para texto claro.
    UseTls: Boolean;

    /// Valida o certificado do servidor: cadeia de confianca do sistema mais
    /// conferencia do nome contra Host. **Desligar aceita qualquer
    /// certificado**, o que anula a defesa contra man-in-the-middle e deixa
    /// apenas a cifragem do canal.
    ///
    /// Existe por um motivo so': certificado self-signed em desenvolvimento (e'
    /// o caso do docker/docker-compose.tls.yml). Em producao, instale a CA no
    /// sistema e deixe isto em True.
    TlsVerifyPeer: Boolean;
    ConnectTimeoutMs: Integer;
    /// Teto de espera por uma resposta. Zero desliga o timeout — nunca faca
    /// isso numa conexao do pool: um comando pendurado prende a conexao E a
    /// thread que chamou, para sempre.
    ReceiveTimeoutMs: Integer;
    SendTimeoutMs: Integer;
  end;

/// Converte texto para os bytes UTF-8 correspondentes.
///
/// Existe porque TEncoding.UTF8.GetBytes nao esta' disponivel de forma
/// equivalente nos dois compiladores: no FPC uma 'string' carrega codepage
/// dinamico, e deixar a conversao implicita corrompe acentuacao em silencio —
/// e so' no FPC, o que torna o bug caro de achar. Equivale ao AmqpUtf8Encode
/// da pascal-amqp-faa.
function RedisUtf8Encode(const AValue: string): TBytes;

/// Inverso de RedisUtf8Encode: interpreta os bytes como UTF-8.
function RedisUtf8Decode(const ABytes: TBytes): string;

/// Formata um Double no formato que o Redis espera: ponto decimal, 'inf',
/// '-inf' e 'nan' por extenso.
///
/// Passa por FormatSettings proprio de proposito: numa maquina configurada em
/// pt-BR o FloatToStr solta '1,5', e o Redis recusaria o ZADD com "value is
/// not a valid float". E' o tipo de bug que so' aparece na maquina do cliente.
function RedisFormatDouble(const AValue: Double): string;

/// Inverso de RedisFormatDouble, tolerante a espacos em volta. Devolve False
/// se o texto nao for numero (nem infinito, nem nan).
function RedisTryParseDouble(const AText: string; out AValue: Double): Boolean;

/// Converte texto para Int64 aceitando so' digitos com sinal opcional.
/// Devolve False em texto vazio, com espacos internos ou nao numerico.
function RedisTryParseInt64(const AText: string; out AValue: Int64): Boolean;

/// Nome do kind em texto, para mensagens de erro e depuracao.
function RedisReplyKindName(AKind: TRedisReplyKind): string;

/// Parametros com os defaults saos: localhost:6379, banco 0, RESP2, sem TLS,
/// timeouts de 5 s.
function RedisDefaultParams: TRedisParams;

/// Como RedisDefaultParams, mas cifrado: porta REDIS_DEFAULT_TLS_PORT (6380) e
/// UseTls ligado. Existe porque a porta muda junto com o TLS — o Redis nao faz
/// upgrade em banda — e esquecer disso rende um handshake TLS contra o listener
/// de texto claro, que falha com uma mensagem de criptografia quando o problema
/// era o numero da porta.
///
/// TlsVerifyPeer continua True. Contra o certificado self-signed do
/// docker-compose.tls.yml e' preciso desliga-lo numa linha explicita:
///
///   LParams := RedisDefaultTlsParams;
///   LParams.TlsVerifyPeer := False;   // so' em desenvolvimento
///
/// Essa linha e' deliberadamente sua, e nao um atalho da lib: baixar a
/// validacao do certificado e' uma decisao de seguranca, e ela tem que aparecer
/// no diff de quem a tomou.
function RedisDefaultTlsParams: TRedisParams;

implementation

const
  /// Timeout inicial de conexao e de I/O. Cinco segundos e' folgado para uma
  /// rede local e curto o bastante para nao parecer travamento.
  DEFAULT_TIMEOUT_MS = 5000;

var
  /// FormatSettings invariante (ponto decimal), montado na inicializacao.
  GFloatFormat: TFormatSettings;
  {$IFNDEF FPC}
  /// Decodificador UTF-8 TOLERANTE do Delphi. Ver a nota na inicializacao.
  GUtf8Lenient: TEncoding;
  {$ENDIF}

{ ERedisReplyError }

constructor ERedisReplyError.CreateReply(const ACode, AMessage: string);
begin
  inherited Create(AMessage);
  FCode := ACode;
end;

{ Conversao UTF-8 }

{$IFDEF FPC}
function RedisUtf8Encode(const AValue: string): TBytes;
var
  LRaw: RawByteString;
begin
  Result := nil;
  LRaw := AValue;
  if (LRaw <> '') and (StringCodePage(LRaw) <> CP_UTF8) then
    SetCodePage(LRaw, CP_UTF8, True);  // converte do codepage real para UTF-8
  SetLength(Result, Length(LRaw));
  if LRaw <> '' then
    Move(LRaw[1], Result[0], Length(LRaw));
end;

function RedisUtf8Decode(const ABytes: TBytes): string;
var
  LRaw: RawByteString;
begin
  SetLength(LRaw, Length(ABytes));
  if Length(ABytes) > 0 then
    Move(ABytes[0], LRaw[1], Length(ABytes));
  SetCodePage(LRaw, CP_UTF8, False);  // marca como UTF-8, sem converter
  Result := LRaw;                     // a conversao respeita o codepage destino
end;
{$ELSE}
function RedisUtf8Encode(const AValue: string): TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(AValue);
end;

function RedisUtf8Decode(const ABytes: TBytes): string;
begin
  if Length(ABytes) = 0 then
    Exit('');
  // GUtf8Lenient, e nao TEncoding.UTF8: ver a nota na inicializacao. Bytes que
  // nao formam UTF-8 valido viram U+FFFD em vez de excecao.
  Result := GUtf8Lenient.GetString(ABytes);
end;
{$ENDIF}

{ Numeros }

function RedisFormatDouble(const AValue: Double): string;
begin
  if IsNan(AValue) then
    Result := 'nan'
  else if IsInfinite(AValue) then
  begin
    if AValue > 0 then
      Result := 'inf'
    else
      Result := '-inf';
  end
  else
    // 17 digitos significativos e' o que garante round-trip de um Double.
    Result := FloatToStrF(AValue, ffGeneral, 17, 0, GFloatFormat);
end;

function RedisTryParseDouble(const AText: string; out AValue: Double): Boolean;
var
  LText: string;
begin
  AValue := 0;
  LText := LowerCase(Trim(AText));
  if (LText = 'inf') or (LText = '+inf') or (LText = 'infinity') then
  begin
    AValue := Infinity;
    Exit(True);
  end;
  if (LText = '-inf') or (LText = '-infinity') then
  begin
    AValue := NegInfinity;
    Exit(True);
  end;
  if LText = 'nan' then
  begin
    AValue := NaN;
    Exit(True);
  end;
  Result := TryStrToFloat(LText, AValue, GFloatFormat);
end;

function RedisTryParseInt64(const AText: string; out AValue: Int64): Boolean;
var
  I, LStart: Integer;
begin
  AValue := 0;
  Result := False;
  if AText = '' then
    Exit;
  LStart := 1;
  if (AText[1] = '-') or (AText[1] = '+') then
    LStart := 2;
  if Length(AText) < LStart then
    Exit;  // so' o sinal, sem digito
  for I := LStart to Length(AText) do
    if (AText[I] < '0') or (AText[I] > '9') then
      Exit;
  // TryStrToInt64 ja' cuida do overflow; o laco acima e' quem barra o que o
  // TryStrToInt64 aceitaria de bom grado, como espacos em volta.
  Result := TryStrToInt64(AText, AValue);
end;

function RedisReplyKindName(AKind: TRedisReplyKind): string;
begin
  case AKind of
    rkNull:         Result := 'null';
    rkSimpleString: Result := 'simple string';
    rkError:        Result := 'error';
    rkInteger:      Result := 'integer';
    rkBulkString:   Result := 'bulk string';
    rkVerbatim:     Result := 'verbatim string';
    rkDouble:       Result := 'double';
    rkBoolean:      Result := 'boolean';
    rkBigNumber:    Result := 'big number';
    rkArray:        Result := 'array';
    rkMap:          Result := 'map';
    rkSet:          Result := 'set';
    rkPush:         Result := 'push';
  else
    Result := 'desconhecido';
  end;
end;

function RedisDefaultParams: TRedisParams;
begin
  Result.Host := 'localhost';
  Result.Port := REDIS_DEFAULT_PORT;
  Result.Username := '';
  Result.Password := '';
  Result.Database := REDIS_DEFAULT_DATABASE;
  Result.ClientName := '';
  Result.Protocol := rpRESP2;
  Result.UseTls := False;
  Result.TlsVerifyPeer := True;
  Result.ConnectTimeoutMs := DEFAULT_TIMEOUT_MS;
  Result.ReceiveTimeoutMs := DEFAULT_TIMEOUT_MS;
  Result.SendTimeoutMs := DEFAULT_TIMEOUT_MS;
end;

function RedisDefaultTlsParams: TRedisParams;
begin
  Result := RedisDefaultParams;
  Result.Port := REDIS_DEFAULT_TLS_PORT;
  Result.UseTls := True;
end;

{ TRedisArg }

class operator TRedisArg.Implicit(const AValue: string): TRedisArg;
begin
  Result.Bytes := RedisUtf8Encode(AValue);
end;

class operator TRedisArg.Implicit(const AValue: Int64): TRedisArg;
begin
  Result.Bytes := RedisUtf8Encode(IntToStr(AValue));
end;

class operator TRedisArg.Implicit(const AValue: Integer): TRedisArg;
begin
  Result.Bytes := RedisUtf8Encode(IntToStr(AValue));
end;

class operator TRedisArg.Implicit(const AValue: Double): TRedisArg;
begin
  Result.Bytes := RedisUtf8Encode(RedisFormatDouble(AValue));
end;

// Booleano vira '1' / '0': e' como o Redis representa flags em argumento e o
// que os scripts Lua devolvem. 'True'/'False' seriam recusados pelo servidor.
class operator TRedisArg.Implicit(const AValue: Boolean): TRedisArg;
begin
  if AValue then
    Result.Bytes := RedisUtf8Encode('1')
  else
    Result.Bytes := RedisUtf8Encode('0');
end;

class operator TRedisArg.Implicit(const AValue: TBytes): TRedisArg;
begin
  Result.Bytes := Copy(AValue, 0, Length(AValue));
end;

initialization
  {$IFNDEF FPC}
  { Por que NAO usar TEncoding.UTF8 para decodificar.

    O TUTF8Encoding do Delphi nasce com MB_ERR_INVALID_CHARS, e o
    TEncoding.GetString levanta EEncodingError quando a conversao nao produz
    caractere nenhum. Resultado: AsString num valor BINARIO — coisa
    corriqueira no Redis, que guarda bytes — explodia com uma excecao da RTL,
    e so' no Delphi: no FPC a mesma rotina apenas remarca o codepage e nunca
    falha. Divergencia dessas e' exatamente o que estas duas funcoes existem
    para eliminar.

    Com os flags zerados, o MultiByteToWideChar troca o byte invalido por
    U+FFFD e segue. Continua valendo o contrato: valor que pode nao ser texto
    se le com AsBytes; AsString e' para quando se sabe que e' texto. }
  GUtf8Lenient := TMBCSEncoding.Create(CP_UTF8, 0, 0);
  {$ENDIF}

  // FormatSettings dedicado: ponto decimal e sem separador de milhar,
  // independente do locale da maquina.
  {$IFDEF FPC}
  GFloatFormat := DefaultFormatSettings;
  {$ELSE}
  GFloatFormat := TFormatSettings.Create;
  {$ENDIF}
  GFloatFormat.DecimalSeparator := '.';
  GFloatFormat.ThousandSeparator := #0;

{$IFNDEF FPC}
finalization
  GUtf8Lenient.Free;
{$ENDIF}

end.
