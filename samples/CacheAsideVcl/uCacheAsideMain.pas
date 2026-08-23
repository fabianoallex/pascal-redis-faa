unit uCacheAsideMain;

{ CACHE-ASIDE: a aplicacao pergunta ao cache primeiro; no miss, vai a' fonte
  lenta e GRAVA o resultado no cache com prazo de validade. O Redis nao sabe
  nada da fonte — quem orquestra e' a aplicacao, e e' dai' que vem o nome.

  O sample simula a fonte lenta com um Sleep (campo editavel na tela), porque o
  que interessa aqui nao e' de onde o dado vem: e' o ciclo miss -> grava com
  TTL -> hit -> expira -> miss de novo.

  ARMADILHA EMBUTIDA (o botao "Regravar SEM KEEPTTL"): um SET simples **apaga o
  TTL** da chave. Regravar um valor cacheado com Strings.SetValue transforma o
  cache num vazamento permanente — a chave nunca mais expira, e o bug so'
  aparece semanas depois, como memoria que nao para de crescer. O botao ao lado
  faz a mesma gravacao com Expiry = seKeepTtl e preserva o prazo. O rotulo
  "TTL restante" mostra a diferenca na hora.

  SEGUNDA DEMONSTRACAO (grupo "Expiracao em massa"): 20 chaves aquecidas ao
  mesmo tempo com TTL FIXO expiram todas juntas, e a barra despenca de 20 para
  0 de uma vez — e' a debandada que joga um lote de misses simultaneos na fonte.
  Com JITTER (o mesmo TTL mais ou menos 20% sorteados por chave), a barra desce
  aos poucos. Mesma chamada, mesma carga: muda so' o prazo sorteado.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild CacheAsideVcl.lpi
    Delphi: abrir CacheAsideVcl.dproj no IDE

  PADRAO DE THREADS (este sample estabelece o esqueleto que os outros samples
  do M9 copiam):

  - **Nada de rede na main thread.** O Redis e' request/response: fora do
    pub/sub o servidor so' fala quando perguntado, entao quem chama
    Strings.Get BLOQUEIA a propria thread ate' a resposta chegar. Chamar da
    thread da UI congela a janela — e justo quando o servidor esta' lento, que
    e' quando alguem esta' olhando. Toda operacao aqui e' um TRedisWorkItem
    enfileirado no RedisPool.
  - **Volta para a UI por marshal descartavel + TThread.Queue.** O FPC nao tem
    o overload de closure anonima do TThread.Queue, entao cada resultado vira
    um objetinho com os dados da chamada e um Execute que chama o metodo da
    form e se libera. Campo compartilhado na form nao serve: varios workers
    postam ao mesmo tempo.
  - **O cliente e' contado, nao apenas anulado.** TRedisClient e'
    compartilhavel entre threads (ele E' o pool), mas destrui-lo com um worker
    ainda executando derruba o worker. Por isso UsarCliente/SoltarCliente
    mantem um contador de operacoes em voo, e o Desconectar espera esse
    contador zerar — dentro de um worker, para nao travar a UI.

  Literais em ASCII de proposito, como no resto do projeto; o texto com acento
  fica nos captions do .dfm/.lfm, que o sistema de streaming resolve nos dois
  compiladores. }

interface

uses
  // No FPC, a camada de emulacao da LCL cobre as chamadas WinAPI do autoscroll
  // do log em qualquer widgetset (win32, gtk2...). Ver CLAUDE.md.
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes, SyncObjs,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ComCtrls, ExtCtrls,
  Redis.Types, Redis.Threading, Redis.Transport, Redis.Client,
  Redis.Commands.Strings;

type
  /// De onde a consulta foi atendida.
  TOrigemConsulta = (ocCache, ocFonte, ocErro);

  TfrmCacheAside = class(TForm)
    gbConexao: TGroupBox;
    lblHost: TLabel;
    edtHost: TEdit;
    lblPorta: TLabel;
    edtPorta: TEdit;
    lblSenha: TLabel;
    edtSenha: TEdit;
    lblDb: TLabel;
    edtDb: TEdit;
    chkTls: TCheckBox;
    btnConectar: TButton;
    btnDesconectar: TButton;
    lblStatus: TLabel;
    gbConsulta: TGroupBox;
    lblCodigo: TLabel;
    edtCodigo: TEdit;
    btnConsultar: TButton;
    btnConsultar5: TButton;
    lblAtraso: TLabel;
    edtAtraso: TEdit;
    lblResultado: TLabel;
    lblOrigem: TLabel;
    lblPlacar: TLabel;
    gbCache: TGroupBox;
    lblTtl: TLabel;
    edtTtl: TEdit;
    chkJitter: TCheckBox;
    lblTtlRestante: TLabel;
    btnRegravarSem: TButton;
    btnRegravarCom: TButton;
    btnDel: TButton;
    btnUnlink: TButton;
    lblArmadilha: TLabel;
    gbLote: TGroupBox;
    lblLoteTtl: TLabel;
    edtLoteTtl: TEdit;
    btnLoteFixo: TButton;
    btnLoteJitter: TButton;
    lblSobreviventes: TLabel;
    pbSobreviventes: TProgressBar;
    mmLog: TMemo;
    btnLimpar: TButton;
    tmrAmostra: TTimer;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormCloseQuery(Sender: TObject; var CanClose: Boolean);
    procedure chkTlsClick(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnDesconectarClick(Sender: TObject);
    procedure btnConsultarClick(Sender: TObject);
    procedure btnConsultar5Click(Sender: TObject);
    procedure btnRegravarSemClick(Sender: TObject);
    procedure btnRegravarComClick(Sender: TObject);
    procedure btnDelClick(Sender: TObject);
    procedure btnUnlinkClick(Sender: TObject);
    procedure btnLoteFixoClick(Sender: TObject);
    procedure btnLoteJitterClick(Sender: TObject);
    procedure btnLimparClick(Sender: TObject);
    procedure tmrAmostraTimer(Sender: TObject);
  private
    FLock: TCriticalSection;
    FClient: TRedisClient;
    FEmVoo: Integer;
    FEncerrando: Boolean;
    FAmostraEmVoo: Boolean;
    FHits: Integer;
    FMisses: Integer;
    procedure AtualizaBotoes(AConectado: Boolean);
    procedure AtualizaPlacar;
    function LeParams(out AParams: TRedisParams): Boolean;
    function TtlEscolhido: Integer;
    procedure Regrava(APreservaTtl: Boolean);
    procedure Invalida(AUnlink: Boolean);
    procedure Aquece(AComJitter: Boolean);
  public
    { --- Chamados pelos workers, sempre pela thread da UI (via marshal) --- }
    procedure Log(const ATexto: string);
    procedure ConexaoAberta(const AInfo: string);
    procedure ConexaoFechada(const AMotivo: string);
    procedure MostraConsulta(const AValor: string; AOrigem: TOrigemConsulta;
      AMs: Int64);
    procedure MostraAmostra(ATtl: Int64; ASobreviventes: Integer);

    { --- Usados pelos workers, de qualquer thread --- }
    /// Reserva o cliente para uma operacao. False quando nao ha' conexao (ou a
    /// aplicacao esta' encerrando) — nesse caso NAO chame SoltarCliente.
    function UsarCliente(out AClient: TRedisClient): Boolean;
    procedure SoltarCliente;
    /// Prefixo das chaves deste sample. Nada de FLUSHDB em sample: o servidor
    /// pode estar sendo usado por outra coisa.
    function ChaveDe(const ACodigo: string): string;
    function ChaveLote(AIndice: Integer): string;
    function CodigoAtual: string;
    function AtrasoDaFonte: Integer;
  end;

const
  /// Tamanho do lote da demonstracao de expiracao em massa.
  LOTE_TOTAL = 20;
  PREFIXO = 'pascal-redis-faa:cache:';

var
  frmCacheAside: TfrmCacheAside;

implementation

{$IFDEF FPC}
{$R *.lfm}
const
  WM_VSCROLL = LM_VSCROLL;   // mesmo valor ($0115); a LCL nao tem unit Messages
{$ELSE}
{$R *.dfm}
{$ENDIF}

{ ---------------------------------------------------------------------------
  Marshals: levam o resultado de um worker para a thread da UI.

  Cada um carrega os dados da chamada e se libera no fim do Execute. Um campo
  compartilhado na form teria corrida — varios workers postam ao mesmo tempo,
  e o botao "Consultar 5x" faz exatamente isso de proposito.
  --------------------------------------------------------------------------- }

type
  TLogMarshal = class
    Form: TfrmCacheAside;
    Texto: string;
    procedure Execute;
  end;

  TStatusMarshal = class
    Form: TfrmCacheAside;
    Aberta: Boolean;
    Texto: string;
    procedure Execute;
  end;

  TConsultaMarshal = class
    Form: TfrmCacheAside;
    Valor: string;
    Origem: TOrigemConsulta;
    Ms: Int64;
    procedure Execute;
  end;

  TAmostraMarshal = class
    Form: TfrmCacheAside;
    Ttl: Int64;
    Sobreviventes: Integer;
    procedure Execute;
  end;

procedure TLogMarshal.Execute;
begin
  Form.Log(Texto);
  Free;
end;

procedure TStatusMarshal.Execute;
begin
  if Aberta then
    Form.ConexaoAberta(Texto)
  else
    Form.ConexaoFechada(Texto);
  Free;
end;

procedure TConsultaMarshal.Execute;
begin
  Form.MostraConsulta(Valor, Origem, Ms);
  Free;
end;

procedure TAmostraMarshal.Execute;
begin
  Form.MostraAmostra(Ttl, Sobreviventes);
  Free;
end;

{ Atalhos de postagem, para os workers nao repetirem o ritual. }

procedure PostaLog(AForm: TfrmCacheAside; const ATexto: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := AForm;
  LMarshal.Texto := ATexto;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure PostaStatus(AForm: TfrmCacheAside; AAberta: Boolean;
  const ATexto: string);
var
  LMarshal: TStatusMarshal;
begin
  LMarshal := TStatusMarshal.Create;
  LMarshal.Form := AForm;
  LMarshal.Aberta := AAberta;
  LMarshal.Texto := ATexto;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ ---------------------------------------------------------------------------
  Work items: rodam num worker do RedisPool (threads persistentes).
  --------------------------------------------------------------------------- }

type
  TConectarWork = class(TRedisWorkItem)
  private
    FForm: TfrmCacheAside;
    FParams: TRedisParams;
  public
    constructor Create(AForm: TfrmCacheAside; const AParams: TRedisParams);
    procedure Execute; override;
  end;

  TDesconectarWork = class(TRedisWorkItem)
  private
    FForm: TfrmCacheAside;
  public
    constructor Create(AForm: TfrmCacheAside);
    procedure Execute; override;
  end;

  TConsultaWork = class(TRedisWorkItem)
  private
    FForm: TfrmCacheAside;
    FCodigo: string;
    FTtl: Integer;
    FJitter: Boolean;
    FRotulo: string;
  public
    constructor Create(AForm: TfrmCacheAside; const ACodigo: string;
      ATtl: Integer; AJitter: Boolean; const ARotulo: string);
    procedure Execute; override;
  end;

  TOperacaoChave = (okRegravaSemTtl, okRegravaKeepTtl, okDel, okUnlink);

  TChaveWork = class(TRedisWorkItem)
  private
    FForm: TfrmCacheAside;
    FCodigo: string;
    FOperacao: TOperacaoChave;
  public
    constructor Create(AForm: TfrmCacheAside; const ACodigo: string;
      AOperacao: TOperacaoChave);
    procedure Execute; override;
  end;

  TAquecerWork = class(TRedisWorkItem)
  private
    FForm: TfrmCacheAside;
    FTtl: Integer;
    FJitter: Boolean;
  public
    constructor Create(AForm: TfrmCacheAside; ATtl: Integer; AJitter: Boolean);
    procedure Execute; override;
  end;

  TAmostraWork = class(TRedisWorkItem)
  private
    FForm: TfrmCacheAside;
    FCodigo: string;
  public
    constructor Create(AForm: TfrmCacheAside; const ACodigo: string);
    procedure Execute; override;
  end;

{ Sorteia o TTL efetivo. Com jitter, o mesmo prazo mais ou menos 20% — e' o
  que impede um lote aquecido junto de expirar junto. }
function TtlEfetivo(ABase: Integer; AJitter: Boolean): Integer;
var
  LMargem: Integer;
begin
  Result := ABase;
  if not AJitter then
    Exit;
  LMargem := ABase div 5;
  if LMargem < 1 then
    LMargem := 1;
  Result := ABase - LMargem + Random(2 * LMargem + 1);
  if Result < 1 then
    Result := 1;
end;

{ A "fonte lenta": um banco, um ERP, um servico externo. O Sleep esta' aqui
  para o custo do miss ser visivel na tela. }
function ConsultaFonteLenta(const ACodigo: string; AAtrasoMs: Integer): string;
begin
  Sleep(AAtrasoMs);
  Result := 'Cliente ' + ACodigo + ' / lido da fonte as ' +
    FormatDateTime('hh:nn:ss', Now);
end;

{ TConectarWork }

constructor TConectarWork.Create(AForm: TfrmCacheAside;
  const AParams: TRedisParams);
begin
  inherited Create;
  FForm := AForm;
  FParams := AParams;
end;

procedure TConectarWork.Execute;
var
  LClient: TRedisClient;
begin
  LClient := nil;
  try
    LClient := TRedisClient.Create(FParams);
    // Ping forca a primeira conexao a abrir AGORA: sem isso, um host errado so'
    // apareceria na primeira consulta, longe do botao que causou o problema.
    LClient.Ping;
    FForm.FLock.Enter;
    try
      FForm.FClient := LClient;
      FForm.FEncerrando := False;
    finally
      FForm.FLock.Leave;
    end;
    LClient := nil;
    PostaStatus(FForm, True, FParams.Host + ':' + IntToStr(FParams.Port));
  except
    on E: Exception do
    begin
      LClient.Free;
      PostaStatus(FForm, False, E.ClassName + ': ' + E.Message);
    end;
  end;
end;

{ TDesconectarWork }

constructor TDesconectarWork.Create(AForm: TfrmCacheAside);
begin
  inherited Create;
  FForm := AForm;
end;

procedure TDesconectarWork.Execute;
var
  LClient: TRedisClient;
  LEmVoo: Integer;
begin
  // Fecha a porta para novas operacoes e espera as que ja' estao no ar.
  // Destruir o cliente com um worker ainda dentro dele derrubaria o worker.
  FForm.FLock.Enter;
  try
    FForm.FEncerrando := True;
  finally
    FForm.FLock.Leave;
  end;

  repeat
    FForm.FLock.Enter;
    try
      LEmVoo := FForm.FEmVoo;
    finally
      FForm.FLock.Leave;
    end;
    if LEmVoo > 0 then
      Sleep(10);
  until LEmVoo = 0;

  FForm.FLock.Enter;
  try
    LClient := FForm.FClient;
    FForm.FClient := nil;
  finally
    FForm.FLock.Leave;
  end;

  LClient.Free;
  PostaStatus(FForm, False, '');
end;

{ TConsultaWork }

constructor TConsultaWork.Create(AForm: TfrmCacheAside; const ACodigo: string;
  ATtl: Integer; AJitter: Boolean; const ARotulo: string);
begin
  inherited Create;
  FForm := AForm;
  FCodigo := ACodigo;
  FTtl := ATtl;
  FJitter := AJitter;
  FRotulo := ARotulo;
end;

procedure TConsultaWork.Execute;
var
  LClient: TRedisClient;
  LChave, LValor: string;
  LOpcoes: TRedisSetOptions;
  LPrazo: Integer;
  LInicio: UInt64;
  LMarshal: TConsultaMarshal;
  LOrigem: TOrigemConsulta;
begin
  LMarshal := TConsultaMarshal.Create;
  LMarshal.Form := FForm;
  if not FForm.UsarCliente(LClient) then
  begin
    LMarshal.Valor := 'sem conexao';
    LMarshal.Origem := ocErro;
    TThread.Queue(nil, LMarshal.Execute);
    Exit;
  end;

  LChave := FForm.ChaveDe(FCodigo);
  LInicio := RedisTickMs;
  LOrigem := ocErro;
  LValor := '';
  try
    try
      // O cache-aside inteiro esta' nestas linhas.
      if LClient.Strings.TryGet(LChave, LValor) then
      begin
        LOrigem := ocCache;
        PostaLog(FForm, FRotulo + 'HIT  ' + LChave);
      end
      else
      begin
        LOrigem := ocFonte;
        PostaLog(FForm, FRotulo + 'MISS ' + LChave + ' -> consultando a fonte');
        LValor := ConsultaFonteLenta(FCodigo, FForm.AtrasoDaFonte);
        LPrazo := TtlEfetivo(FTtl, FJitter);
        LOpcoes := RedisDefaultSetOptions;
        LOpcoes.Expiry := seSeconds;
        LOpcoes.ExpiryValue := LPrazo;
        LClient.Strings.SetWithOptions(LChave, LValor, LOpcoes);
        PostaLog(FForm, FRotulo + 'gravado no cache com TTL de ' +
          IntToStr(LPrazo) + 's');
      end;
    except
      on E: Exception do
      begin
        LOrigem := ocErro;
        LValor := E.Message;
        PostaLog(FForm, 'ERRO na consulta: ' + E.ClassName + ': ' + E.Message);
      end;
    end;
  finally
    FForm.SoltarCliente;
  end;

  LMarshal.Valor := LValor;
  LMarshal.Origem := LOrigem;
  LMarshal.Ms := Int64(RedisTickMs - LInicio);
  TThread.Queue(nil, LMarshal.Execute);
end;

{ TChaveWork }

constructor TChaveWork.Create(AForm: TfrmCacheAside; const ACodigo: string;
  AOperacao: TOperacaoChave);
begin
  inherited Create;
  FForm := AForm;
  FCodigo := ACodigo;
  FOperacao := AOperacao;
end;

procedure TChaveWork.Execute;
var
  LClient: TRedisClient;
  LChave, LValor: string;
  LOpcoes: TRedisSetOptions;
begin
  if not FForm.UsarCliente(LClient) then
  begin
    PostaLog(FForm, 'sem conexao');
    Exit;
  end;
  LChave := FForm.ChaveDe(FCodigo);
  try
    try
      case FOperacao of
        okRegravaSemTtl:
          begin
            if not LClient.Strings.TryGet(LChave, LValor) then
              LValor := 'Cliente ' + FCodigo;
            LClient.Strings.SetValue(LChave, LValor + ' [regravado]');
            PostaLog(FForm, 'SET simples em ' + LChave +
              ' -- o TTL foi apagado junto (armadilha)');
          end;
        okRegravaKeepTtl:
          begin
            if not LClient.Strings.TryGet(LChave, LValor) then
              LValor := 'Cliente ' + FCodigo;
            LOpcoes := RedisDefaultSetOptions;
            LOpcoes.Expiry := seKeepTtl;
            LClient.Strings.SetWithOptions(LChave, LValor + ' [regravado]',
              LOpcoes);
            PostaLog(FForm, 'SET ... KEEPTTL em ' + LChave +
              ' -- o prazo continua correndo');
          end;
        okDel:
          PostaLog(FForm, 'DEL ' + LChave + ' -> apagou: ' +
            BoolToStr(LClient.Keys.Del(LChave), True) +
            ' (libera a memoria na hora, na thread do servidor)');
        okUnlink:
          PostaLog(FForm, 'UNLINK ' + LChave + ' -> chaves: ' +
            IntToStr(LClient.Keys.Unlink([LChave])) +
            ' (tira do keyspace na hora e libera em segundo plano)');
      end;
    except
      on E: Exception do
        PostaLog(FForm, 'ERRO: ' + E.ClassName + ': ' + E.Message);
    end;
  finally
    FForm.SoltarCliente;
  end;
end;

{ TAquecerWork }

constructor TAquecerWork.Create(AForm: TfrmCacheAside; ATtl: Integer;
  AJitter: Boolean);
begin
  inherited Create;
  FForm := AForm;
  FTtl := ATtl;
  FJitter := AJitter;
end;

procedure TAquecerWork.Execute;
var
  LClient: TRedisClient;
  LOpcoes: TRedisSetOptions;
  LMenor, LMaior, LPrazo, I: Integer;
begin
  if not FForm.UsarCliente(LClient) then
  begin
    PostaLog(FForm, 'sem conexao');
    Exit;
  end;
  LMenor := MaxInt;
  LMaior := 0;
  try
    try
      for I := 0 to LOTE_TOTAL - 1 do
      begin
        LPrazo := TtlEfetivo(FTtl, FJitter);
        if LPrazo < LMenor then
          LMenor := LPrazo;
        if LPrazo > LMaior then
          LMaior := LPrazo;
        LOpcoes := RedisDefaultSetOptions;
        LOpcoes.Expiry := seSeconds;
        LOpcoes.ExpiryValue := LPrazo;
        LClient.Strings.SetWithOptions(FForm.ChaveLote(I),
          'item ' + IntToStr(I), LOpcoes);
      end;
      if FJitter then
        PostaLog(FForm, 'lote aquecido COM jitter: prazos entre ' +
          IntToStr(LMenor) + 's e ' + IntToStr(LMaior) +
          's -- a barra desce aos poucos')
      else
        PostaLog(FForm, 'lote aquecido com TTL FIXO de ' + IntToStr(LMenor) +
          's -- as 20 chaves vao expirar no mesmo segundo');
    except
      on E: Exception do
        PostaLog(FForm, 'ERRO ao aquecer: ' + E.ClassName + ': ' + E.Message);
    end;
  finally
    FForm.SoltarCliente;
  end;
end;

{ TAmostraWork }

constructor TAmostraWork.Create(AForm: TfrmCacheAside; const ACodigo: string);
begin
  inherited Create;
  FForm := AForm;
  FCodigo := ACodigo;
end;

procedure TAmostraWork.Execute;
var
  LClient: TRedisClient;
  LChaves: array of TRedisArg;
  LMarshal: TAmostraMarshal;
  LTtl: Int64;
  LVivas: Integer;
  I: Integer;
begin
  LChaves := nil;
  LTtl := -2;      // -2 e' a resposta do proprio Redis para "chave nao existe"
  LVivas := 0;
  if FForm.UsarCliente(LClient) then
  try
    try
      LTtl := LClient.Keys.Ttl(FForm.ChaveDe(FCodigo));
      SetLength(LChaves, LOTE_TOTAL);
      for I := 0 to LOTE_TOTAL - 1 do
        LChaves[I] := FForm.ChaveLote(I);
      LVivas := Integer(LClient.Keys.ExistsMany(LChaves));
    except
      // Amostragem de tela nao merece caixa de erro nem log a cada 500 ms: se a
      // conexao caiu, o proximo comando de verdade vai dizer.
      on E: Exception do
        LTtl := -2;
    end;
  finally
    FForm.SoltarCliente;
  end;

  LMarshal := TAmostraMarshal.Create;
  LMarshal.Form := FForm;
  LMarshal.Ttl := LTtl;
  LMarshal.Sobreviventes := LVivas;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ ---------------------------------------------------------------------------
  TfrmCacheAside
  --------------------------------------------------------------------------- }

procedure TfrmCacheAside.FormCreate(Sender: TObject);
begin
  Randomize;
  FLock := TCriticalSection.Create;
  AtualizaBotoes(False);
  AtualizaPlacar;
  Log('Suba o servidor com: docker compose -f docker/docker-compose.yml up -d');
  Log('Para TLS, use tambem o override docker-compose.tls.yml (porta 6380).');
end;

procedure TfrmCacheAside.FormCloseQuery(Sender: TObject; var CanClose: Boolean);
var
  LClient: TRedisClient;
  LEspera: Integer;
begin
  CanClose := True;
  tmrAmostra.Enabled := False;

  FLock.Enter;
  try
    FEncerrando := True;
    LClient := FClient;
  finally
    FLock.Leave;
  end;
  if LClient = nil then
    Exit;

  // No fechamento a espera e' na propria thread da UI, de proposito: a janela
  // ja' esta' indo embora e o que importa e' nao destruir o cliente por baixo
  // de um worker. Teto de 5 s para nao pendurar a aplicacao.
  LEspera := 0;
  while LEspera < 500 do
  begin
    FLock.Enter;
    try
      if FEmVoo = 0 then
        Break;
    finally
      FLock.Leave;
    end;
    Sleep(10);
    Inc(LEspera);
  end;

  FLock.Enter;
  try
    FClient := nil;
  finally
    FLock.Leave;
  end;
  LClient.Free;
end;

procedure TfrmCacheAside.FormDestroy(Sender: TObject);
begin
  FLock.Free;
end;

function TfrmCacheAside.UsarCliente(out AClient: TRedisClient): Boolean;
begin
  FLock.Enter;
  try
    Result := (FClient <> nil) and not FEncerrando;
    if Result then
    begin
      AClient := FClient;
      Inc(FEmVoo);
    end
    else
      AClient := nil;
  finally
    FLock.Leave;
  end;
end;

procedure TfrmCacheAside.SoltarCliente;
begin
  FLock.Enter;
  try
    Dec(FEmVoo);
  finally
    FLock.Leave;
  end;
end;

function TfrmCacheAside.ChaveDe(const ACodigo: string): string;
begin
  Result := PREFIXO + ACodigo;
end;

function TfrmCacheAside.ChaveLote(AIndice: Integer): string;
begin
  Result := PREFIXO + 'lote:' + IntToStr(AIndice);
end;

function TfrmCacheAside.CodigoAtual: string;
begin
  Result := Trim(edtCodigo.Text);
  if Result = '' then
    Result := '1001';
end;

function TfrmCacheAside.AtrasoDaFonte: Integer;
begin
  Result := StrToIntDef(Trim(edtAtraso.Text), 800);
  if Result < 0 then
    Result := 0;
end;

function TfrmCacheAside.TtlEscolhido: Integer;
begin
  Result := StrToIntDef(Trim(edtTtl.Text), 60);
  if Result < 1 then
    Result := 1;
end;

procedure TfrmCacheAside.Log(const ATexto: string);
begin
  mmLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + ATexto);
  // Autoscroll sem depender do caret: WM_VSCROLL/SB_BOTTOM vale nos dois
  // mundos (na LCL o valor vem de LM_VSCROLL, identico).
  mmLog.Perform(WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmCacheAside.AtualizaBotoes(AConectado: Boolean);
begin
  btnConectar.Enabled := not AConectado;
  btnDesconectar.Enabled := AConectado;
  edtHost.Enabled := not AConectado;
  edtPorta.Enabled := not AConectado;
  edtSenha.Enabled := not AConectado;
  edtDb.Enabled := not AConectado;
  chkTls.Enabled := not AConectado;

  gbConsulta.Enabled := AConectado;
  gbCache.Enabled := AConectado;
  gbLote.Enabled := AConectado;
end;

procedure TfrmCacheAside.AtualizaPlacar;
var
  LTotal: Integer;
  LTaxa: string;
begin
  LTotal := FHits + FMisses;
  if LTotal = 0 then
    LTaxa := '-'
  else
    LTaxa := IntToStr(Round(100 * FHits / LTotal)) + '%';
  lblPlacar.Caption := 'Hits: ' + IntToStr(FHits) + '   Misses: ' +
    IntToStr(FMisses) + '   Taxa de acerto: ' + LTaxa;
end;

function TfrmCacheAside.LeParams(out AParams: TRedisParams): Boolean;
begin
  Result := True;
  if chkTls.Checked then
    AParams := RedisDefaultTlsParams
  else
    AParams := RedisDefaultParams;

  AParams.Host := Trim(edtHost.Text);
  AParams.Port := StrToIntDef(Trim(edtPorta.Text), AParams.Port);
  AParams.Password := edtSenha.Text;
  AParams.Database := StrToIntDef(Trim(edtDb.Text), 0);
  AParams.ClientName := 'CacheAsideVcl';
  // O container de dev usa certificado self-signed; em producao isto fica em
  // True e a CA vai para o repositorio de confianca do sistema.
  if AParams.UseTls then
    AParams.TlsVerifyPeer := False;

  if AParams.Host = '' then
  begin
    ShowMessage('Informe o host.');
    Result := False;
  end;
end;

procedure TfrmCacheAside.chkTlsClick(Sender: TObject);
begin
  // TLS no Redis nao e' upgrade em banda: e' outra porta. Trocar as duas coisas
  // juntas e' o que o RedisDefaultTlsParams faz, e a tela acompanha.
  if chkTls.Checked then
  begin
    if Trim(edtPorta.Text) = IntToStr(REDIS_DEFAULT_PORT) then
      edtPorta.Text := IntToStr(REDIS_DEFAULT_TLS_PORT);
  end
  else
  begin
    if Trim(edtPorta.Text) = IntToStr(REDIS_DEFAULT_TLS_PORT) then
      edtPorta.Text := IntToStr(REDIS_DEFAULT_PORT);
  end;
end;

procedure TfrmCacheAside.btnConectarClick(Sender: TObject);
var
  LParams: TRedisParams;
begin
  if not LeParams(LParams) then
    Exit;
  btnConectar.Enabled := False;
  lblStatus.Caption := 'Conectando...';
  RedisPool.Queue(TConectarWork.Create(Self, LParams));
end;

procedure TfrmCacheAside.btnDesconectarClick(Sender: TObject);
begin
  btnDesconectar.Enabled := False;
  tmrAmostra.Enabled := False;
  lblStatus.Caption := 'Encerrando as operacoes em voo...';
  RedisPool.Queue(TDesconectarWork.Create(Self));
end;

procedure TfrmCacheAside.ConexaoAberta(const AInfo: string);
begin
  AtualizaBotoes(True);
  lblStatus.Caption := 'Conectado a ' + AInfo + '  |  backend TLS: ' +
    RedisTlsBackendName;
  Log('conectado a ' + AInfo);
  tmrAmostra.Enabled := True;
end;

procedure TfrmCacheAside.ConexaoFechada(const AMotivo: string);
begin
  AtualizaBotoes(False);
  tmrAmostra.Enabled := False;
  lblTtlRestante.Caption := 'TTL restante: -';
  lblSobreviventes.Caption := 'No cache: -';
  pbSobreviventes.Position := 0;
  if AMotivo = '' then
  begin
    lblStatus.Caption := 'Desconectado.';
    Log('desconectado');
  end
  else
  begin
    lblStatus.Caption := 'Falha: ' + AMotivo;
    Log('falha ao conectar: ' + AMotivo);
  end;
end;

procedure TfrmCacheAside.btnConsultarClick(Sender: TObject);
begin
  RedisPool.Queue(TConsultaWork.Create(Self, CodigoAtual, TtlEscolhido,
    chkJitter.Checked, ''));
end;

procedure TfrmCacheAside.btnConsultar5Click(Sender: TObject);
var
  I: Integer;
begin
  // Cinco consultas ao MESMO tempo, na mesma chave fria. Todas erram o cache
  // (nenhuma gravou ainda) e todas vao a' fonte: e' a debandada, e o
  // cache-aside sozinho nao protege contra ela. O placar mostra 5 misses.
  Log('--- 5 consultas concorrentes na mesma chave ---');
  for I := 1 to 5 do
    RedisPool.Queue(TConsultaWork.Create(Self, CodigoAtual, TtlEscolhido,
      chkJitter.Checked, '[' + IntToStr(I) + '] '));
end;

procedure TfrmCacheAside.MostraConsulta(const AValor: string;
  AOrigem: TOrigemConsulta; AMs: Int64);
begin
  case AOrigem of
    ocCache:
      begin
        Inc(FHits);
        lblOrigem.Caption := 'Origem: CACHE  (' + IntToStr(AMs) + ' ms)';
        lblResultado.Caption := 'Resultado: ' + AValor;
      end;
    ocFonte:
      begin
        Inc(FMisses);
        lblOrigem.Caption := 'Origem: FONTE LENTA  (' + IntToStr(AMs) + ' ms)';
        lblResultado.Caption := 'Resultado: ' + AValor;
      end;
  else
    lblOrigem.Caption := 'Origem: erro';
    lblResultado.Caption := 'Resultado: ' + AValor;
  end;
  AtualizaPlacar;
end;

procedure TfrmCacheAside.Regrava(APreservaTtl: Boolean);
begin
  if APreservaTtl then
    RedisPool.Queue(TChaveWork.Create(Self, CodigoAtual, okRegravaKeepTtl))
  else
    RedisPool.Queue(TChaveWork.Create(Self, CodigoAtual, okRegravaSemTtl));
end;

procedure TfrmCacheAside.btnRegravarSemClick(Sender: TObject);
begin
  Regrava(False);
end;

procedure TfrmCacheAside.btnRegravarComClick(Sender: TObject);
begin
  Regrava(True);
end;

procedure TfrmCacheAside.Invalida(AUnlink: Boolean);
begin
  if AUnlink then
    RedisPool.Queue(TChaveWork.Create(Self, CodigoAtual, okUnlink))
  else
    RedisPool.Queue(TChaveWork.Create(Self, CodigoAtual, okDel));
end;

procedure TfrmCacheAside.btnDelClick(Sender: TObject);
begin
  Invalida(False);
end;

procedure TfrmCacheAside.btnUnlinkClick(Sender: TObject);
begin
  Invalida(True);
end;

procedure TfrmCacheAside.Aquece(AComJitter: Boolean);
var
  LTtl: Integer;
begin
  LTtl := StrToIntDef(Trim(edtLoteTtl.Text), 15);
  if LTtl < 2 then
    LTtl := 2;
  RedisPool.Queue(TAquecerWork.Create(Self, LTtl, AComJitter));
end;

procedure TfrmCacheAside.btnLoteFixoClick(Sender: TObject);
begin
  Aquece(False);
end;

procedure TfrmCacheAside.btnLoteJitterClick(Sender: TObject);
begin
  Aquece(True);
end;

procedure TfrmCacheAside.tmrAmostraTimer(Sender: TObject);
begin
  // Uma amostra por vez: se o servidor engasgar, a fila do pool nao pode virar
  // um estoque de amostras velhas esperando para rodar.
  FLock.Enter;
  try
    if FAmostraEmVoo or (FClient = nil) or FEncerrando then
      Exit;
    FAmostraEmVoo := True;
  finally
    FLock.Leave;
  end;
  RedisPool.Queue(TAmostraWork.Create(Self, CodigoAtual));
end;

procedure TfrmCacheAside.MostraAmostra(ATtl: Int64; ASobreviventes: Integer);
begin
  FLock.Enter;
  try
    FAmostraEmVoo := False;
  finally
    FLock.Leave;
  end;

  if ATtl = -2 then
    lblTtlRestante.Caption := 'TTL restante: chave ausente'
  else if ATtl = -1 then
    lblTtlRestante.Caption :=
      'TTL restante: SEM PRAZO -- a chave nao expira mais'
  else
    lblTtlRestante.Caption := 'TTL restante: ' + IntToStr(ATtl) + 's';

  lblSobreviventes.Caption := 'No cache: ' + IntToStr(ASobreviventes) + ' de ' +
    IntToStr(LOTE_TOTAL);
  pbSobreviventes.Position := ASobreviventes;
end;

procedure TfrmCacheAside.btnLimparClick(Sender: TObject);
begin
  mmLog.Lines.Clear;
end;

end.
