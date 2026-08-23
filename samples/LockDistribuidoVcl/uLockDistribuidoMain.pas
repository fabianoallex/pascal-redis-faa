unit uLockDistribuidoMain;

{ LOCK DISTRIBUIDO: SET NX PX grava a chave so' se ela nao existir, com prazo
  de validade -- e' o "eu cheguei primeiro" do Redis. O TOKEN e' o que separa
  "um lock" de "um lock que eu sei que e' meu": sem ele, DEL apaga qualquer
  coisa que estiver na chave, seja ela sua ou de outro dono.

  ATENCAO, escrito na propria tela: isto e' um lock de INSTANCIA UNICA (um
  Redis so, sem coordenacao entre varios). NAO e' Redlock. Se o processo dono
  morrer ou pausar por mais tempo que o TTL, a garantia cai junto -- o mesmo
  vale para qualquer app que confie soh num unico Redis.

  ARMADILHA EMBUTIDA (o botao "Liberar com DEL direto"): DEL apaga o que
  estiver na chave AGORA, sem perguntar de quem e'. Deixe o lock expirar,
  ligue o "concorrente simulado" (que fica tentando adquirir a cada 1s), e
  quando ele conseguir, clique em "Liberar com DEL direto" pensando que ainda
  e' seu -- o log mostra o lock do concorrente sendo apagado por engano. O
  botao ao lado, "Liberar com script", faz a mesma coisa com um Lua que
  compara o token antes de apagar: so' apaga se ainda for seu.

  SEGUNDA DEMONSTRACAO (grupo "Renovacao"): com "Renovar automaticamente"
  marcado, um script roda a cada 1s e so' estende o TTL (PEXPIRE) se o token
  no servidor ainda for o meu -- e' o jeito de manter a posse sem deixar o
  prazo estourar. Se alguem ja' tomou o lock, a renovacao falha e a propria
  tela desmarca o checkbox.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild LockDistribuidoVcl.lpi
    Delphi: abrir LockDistribuidoVcl.dproj no IDE

  PADRAO DE THREADS: identico ao CacheAsideVcl (nada de rede na main thread,
  marshal descartavel + TThread.Queue, cliente contado por UsarCliente /
  SoltarCliente). Ver o comentario de cabecalho de uCacheAsideMain.pas para o
  racional completo -- aqui vai so' o que muda.

  Literais em ASCII de proposito, como no resto do projeto; o texto com
  acento fica nos captions do .dfm/.lfm, que o sistema de streaming resolve
  nos dois compiladores. }

interface

uses
  {$IFDEF FPC}
  LCLIntf, LCLType, LMessages,
  {$ELSE}
  Windows, Messages,
  {$ENDIF}
  SysUtils, Classes, SyncObjs,
  Graphics, Controls, Forms, Dialogs, StdCtrls, ExtCtrls,
  Redis.Types, Redis.Threading, Redis.Transport, Redis.Client,
  Redis.Commands.Strings, Redis.Commands.Keys, Redis.Commands.Scripting;

type
  TfrmLockDistribuido = class(TForm)
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
    gbLock: TGroupBox;
    lblAviso: TLabel;
    lblRecurso: TLabel;
    edtRecurso: TEdit;
    lblTtl: TLabel;
    edtTtl: TEdit;
    chkComToken: TCheckBox;
    btnAdquirir: TButton;
    lblMeuToken: TLabel;
    lblDono: TLabel;
    lblTtlRestante: TLabel;
    gbConcorrente: TGroupBox;
    chkConcorrente: TCheckBox;
    lblConcorrenteToken: TLabel;
    lblConcorrenteInfo: TLabel;
    gbLiberacao: TGroupBox;
    btnLiberarSeguro: TButton;
    btnLiberarArmadilha: TButton;
    lblArmadilha: TLabel;
    gbRenovacao: TGroupBox;
    chkRenovarAuto: TCheckBox;
    lblRenovacaoInfo: TLabel;
    mmLog: TMemo;
    btnLimpar: TButton;
    tmrAmostra: TTimer;
    tmrConcorrente: TTimer;
    tmrRenovar: TTimer;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormCloseQuery(Sender: TObject; var CanClose: Boolean);
    procedure chkTlsClick(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnDesconectarClick(Sender: TObject);
    procedure chkComTokenClick(Sender: TObject);
    procedure btnAdquirirClick(Sender: TObject);
    procedure chkConcorrenteClick(Sender: TObject);
    procedure btnLiberarSeguroClick(Sender: TObject);
    procedure btnLiberarArmadilhaClick(Sender: TObject);
    procedure chkRenovarAutoClick(Sender: TObject);
    procedure btnLimparClick(Sender: TObject);
    procedure tmrAmostraTimer(Sender: TObject);
    procedure tmrConcorrenteTimer(Sender: TObject);
    procedure tmrRenovarTimer(Sender: TObject);
  private
    FLock: TCriticalSection;
    FClient: TRedisClient;
    FEmVoo: Integer;
    FEncerrando: Boolean;
    FAmostraEmVoo: Boolean;
    FConcorrenteEmVoo: Boolean;
    FRenovarEmVoo: Boolean;
    { So' escritos pela thread da UI (sempre via marshal) -- sem lock. }
    FMeuToken: string;
    FConcorrenteToken: string;
    procedure AtualizaBotoes(AConectado: Boolean);
    procedure AtualizaEstadoLock;
    function LeParams(out AParams: TRedisParams): Boolean;
    function ChaveDoLock: string;
    function TtlEscolhido: Integer;
  public
    { --- Chamados pelos workers, sempre pela thread da UI (via marshal) --- }
    procedure Log(const ATexto: string);
    procedure ConexaoAberta(const AInfo: string);
    procedure ConexaoFechada(const AMotivo: string);
    procedure MostraAdquirir(ASucesso: Boolean; const AToken: string;
      AConcorrente, AJaExplicado: Boolean);
    procedure MostraLiberar(ASeguro, ASucesso: Boolean;
      const AValorApagado: string; AJaExplicado: Boolean);
    procedure MostraRenovacao(ASucesso, AJaExplicado: Boolean);
    procedure MostraAmostra(const ADono: string; APttlMs: Int64);

    { --- Usados pelos workers, de qualquer thread --- }
    function UsarCliente(out AClient: TRedisClient): Boolean;
    procedure SoltarCliente;
  end;

const
  PREFIXO = 'pascal-redis-faa:lock:';

var
  frmLockDistribuido: TfrmLockDistribuido;

implementation

{$IFDEF FPC}
{$R *.lfm}
const
  WM_VSCROLL = LM_VSCROLL;   // mesmo valor ($0115); a LCL nao tem unit Messages
{$ELSE}
{$R *.dfm}
{$ENDIF}

const
  { O release classico: so' apaga se o valor guardado ainda for o MEU token.
    GET seguido de DEL, sem script, teria a corrida classica -- entre os dois
    comandos o lock pode expirar e ser tomado por outro processo, e o DEL
    apagaria o lock DELE. }
  SCRIPT_LIBERA =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then' + sLineBreak +
    '  return redis.call("DEL", KEYS[1])' + sLineBreak +
    'else' + sLineBreak +
    '  return 0' + sLineBreak +
    'end';

  { Mesma ideia para renovar: so' estica o TTL se o dono continuar sendo eu. }
  SCRIPT_RENOVA =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then' + sLineBreak +
    '  return redis.call("PEXPIRE", KEYS[1], ARGV[2])' + sLineBreak +
    'else' + sLineBreak +
    '  return 0' + sLineBreak +
    'end';

{ Token legivel: prefixo (quem sou eu) + relogio + numero aleatorio. Nao
  precisa ser um UUID -- so' precisa ser diferente a cada chamada e dar para
  reconhecer no log quem gerou o que. }
function GeraToken(const APrefixo: string): string;
begin
  Result := APrefixo + '-' + IntToHex(Int64(RedisTickMs), 10) + '-' +
    IntToHex(Random(MaxInt), 8);
end;

{ ---------------------------------------------------------------------------
  Marshals: levam o resultado de um worker para a thread da UI.
  --------------------------------------------------------------------------- }

type
  TLogMarshal = class
    Form: TfrmLockDistribuido;
    Texto: string;
    procedure Execute;
  end;

  TStatusMarshal = class
    Form: TfrmLockDistribuido;
    Aberta: Boolean;
    Texto: string;
    procedure Execute;
  end;

  TAdquirirMarshal = class
    Form: TfrmLockDistribuido;
    Sucesso: Boolean;
    Token: string;
    Concorrente: Boolean;
    ComConexao: Boolean;
    /// True quando o proprio worker ja' logou o motivo (erro de servidor) --
    /// evita que MostraAdquirir emende por cima um "chave ja ocupada"
    /// generico e enganoso.
    JaExplicado: Boolean;
    procedure Execute;
  end;

  TLiberarMarshal = class
    Form: TfrmLockDistribuido;
    Seguro: Boolean;
    Sucesso: Boolean;
    ValorApagado: string;
    ComConexao: Boolean;
    JaExplicado: Boolean;
    procedure Execute;
  end;

  TRenovarMarshal = class
    Form: TfrmLockDistribuido;
    Sucesso: Boolean;
    ComConexao: Boolean;
    JaExplicado: Boolean;
    procedure Execute;
  end;

  TAmostraMarshal = class
    Form: TfrmLockDistribuido;
    Dono: string;
    PttlMs: Int64;
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

procedure TAdquirirMarshal.Execute;
begin
  if ComConexao then
    Form.MostraAdquirir(Sucesso, Token, Concorrente, JaExplicado)
  else if not Concorrente then
    Form.Log('sem conexao');
  Free;
end;

procedure TLiberarMarshal.Execute;
begin
  if ComConexao then
    Form.MostraLiberar(Seguro, Sucesso, ValorApagado, JaExplicado)
  else
    Form.Log('sem conexao');
  Free;
end;

procedure TRenovarMarshal.Execute;
begin
  if ComConexao then
    Form.MostraRenovacao(Sucesso, JaExplicado)
  else
    Form.Log('sem conexao');
  Free;
end;

procedure TAmostraMarshal.Execute;
begin
  Form.MostraAmostra(Dono, PttlMs);
  Free;
end;

procedure PostaLog(AForm: TfrmLockDistribuido; const ATexto: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := AForm;
  LMarshal.Texto := ATexto;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure PostaStatus(AForm: TfrmLockDistribuido; AAberta: Boolean;
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
    FForm: TfrmLockDistribuido;
    FParams: TRedisParams;
  public
    constructor Create(AForm: TfrmLockDistribuido; const AParams: TRedisParams);
    procedure Execute; override;
  end;

  TDesconectarWork = class(TRedisWorkItem)
  private
    FForm: TfrmLockDistribuido;
  public
    constructor Create(AForm: TfrmLockDistribuido);
    procedure Execute; override;
  end;

  TAdquirirWork = class(TRedisWorkItem)
  private
    FForm: TfrmLockDistribuido;
    FChave: string;
    FToken: string;
    FTtlMs: Integer;
    FConcorrente: Boolean;
  public
    constructor Create(AForm: TfrmLockDistribuido; const AChave, AToken: string;
      ATtlMs: Integer; AConcorrente: Boolean);
    procedure Execute; override;
  end;

  TLiberarWork = class(TRedisWorkItem)
  private
    FForm: TfrmLockDistribuido;
    FChave: string;
    FToken: string;
    FSeguro: Boolean;
  public
    constructor Create(AForm: TfrmLockDistribuido; const AChave, AToken: string;
      ASeguro: Boolean);
    procedure Execute; override;
  end;

  TRenovarWork = class(TRedisWorkItem)
  private
    FForm: TfrmLockDistribuido;
    FChave: string;
    FToken: string;
    FTtlMs: Integer;
  public
    constructor Create(AForm: TfrmLockDistribuido; const AChave, AToken: string;
      ATtlMs: Integer);
    procedure Execute; override;
  end;

  TAmostraWork = class(TRedisWorkItem)
  private
    FForm: TfrmLockDistribuido;
    FChave: string;
  public
    constructor Create(AForm: TfrmLockDistribuido; const AChave: string);
    procedure Execute; override;
  end;

{ TConectarWork }

constructor TConectarWork.Create(AForm: TfrmLockDistribuido;
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
    // apareceria no primeiro Adquirir, longe do botao que causou o problema.
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

constructor TDesconectarWork.Create(AForm: TfrmLockDistribuido);
begin
  inherited Create;
  FForm := AForm;
end;

procedure TDesconectarWork.Execute;
var
  LClient: TRedisClient;
  LEmVoo: Integer;
begin
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

{ TAdquirirWork }

constructor TAdquirirWork.Create(AForm: TfrmLockDistribuido;
  const AChave, AToken: string; ATtlMs: Integer; AConcorrente: Boolean);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
  FToken := AToken;
  FTtlMs := ATtlMs;
  FConcorrente := AConcorrente;
end;

procedure TAdquirirWork.Execute;
var
  LClient: TRedisClient;
  LOpcoes: TRedisSetOptions;
  LReply: IRedisReply;
  LMarshal: TAdquirirMarshal;
  LSucesso: Boolean;
begin
  LMarshal := TAdquirirMarshal.Create;
  LMarshal.Form := FForm;
  LMarshal.Token := FToken;
  LMarshal.Concorrente := FConcorrente;

  if not FForm.UsarCliente(LClient) then
  begin
    LMarshal.Sucesso := False;
    LMarshal.ComConexao := False;
    TThread.Queue(nil, LMarshal.Execute);
    Exit;
  end;
  LMarshal.ComConexao := True;

  LSucesso := False;
  try
    try
      LOpcoes := RedisDefaultSetOptions;
      LOpcoes.Condition := scNotExists;
      LOpcoes.Expiry := seMilliseconds;
      LOpcoes.ExpiryValue := FTtlMs;
      // SET NX PX: grava so' se a chave nao existir. E' o "eu cheguei
      // primeiro" do lock -- reply nulo significa que alguem chegou antes.
      LReply := LClient.Strings.SetWithOptions(FChave, FToken, LOpcoes);
      LSucesso := not LReply.IsNull;
    except
      on E: Exception do
      begin
        if not FConcorrente then
          PostaLog(FForm, 'ERRO ao adquirir: ' + E.ClassName + ': ' + E.Message);
        LMarshal.JaExplicado := True;
      end;
    end;
  finally
    FForm.SoltarCliente;
  end;

  LMarshal.Sucesso := LSucesso;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ TLiberarWork }

constructor TLiberarWork.Create(AForm: TfrmLockDistribuido;
  const AChave, AToken: string; ASeguro: Boolean);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
  FToken := AToken;
  FSeguro := ASeguro;
end;

procedure TLiberarWork.Execute;
var
  LClient: TRedisClient;
  LMarshal: TLiberarMarshal;
  LReply: IRedisReply;
  LValorAntes: string;
begin
  LMarshal := TLiberarMarshal.Create;
  LMarshal.Form := FForm;
  LMarshal.Seguro := FSeguro;

  if not FForm.UsarCliente(LClient) then
  begin
    LMarshal.Sucesso := False;
    LMarshal.ComConexao := False;
    TThread.Queue(nil, LMarshal.Execute);
    Exit;
  end;
  LMarshal.ComConexao := True;

  try
    try
      if FSeguro then
      begin
        // Compare-and-delete atomico: so' apaga se o token no servidor ainda
        // for o meu. E' a UNICA forma segura de liberar -- GET seguido de DEL
        // teria a corrida classica (o lock pode expirar entre os dois).
        LReply := LClient.Scripting.Run(SCRIPT_LIBERA, [FChave], [FToken]);
        LMarshal.Sucesso := LReply.AsInteger = 1;
        LMarshal.ValorApagado := FToken;
      end
      else
      begin
        // A ARMADILHA: DEL nao compara nada. O GET antes e' so' para o log
        // conseguir contar de quem era o lock apagado -- ha' uma janela
        // pequena entre os dois comandos, e essa janela e' parte da licao:
        // check-then-act nao e' atomico, do mesmo jeito que nao seria se a
        // aplicacao fizesse GET+DEL na mao em vez do script ao lado.
        if not LClient.Strings.TryGet(FChave, LValorAntes) then
          LValorAntes := '';
        LMarshal.Sucesso := LClient.Keys.Del(FChave);
        LMarshal.ValorApagado := LValorAntes;
      end;
    except
      on E: Exception do
      begin
        LMarshal.Sucesso := False;
        PostaLog(FForm, 'ERRO ao liberar: ' + E.ClassName + ': ' + E.Message);
        LMarshal.JaExplicado := True;
      end;
    end;
  finally
    FForm.SoltarCliente;
  end;

  TThread.Queue(nil, LMarshal.Execute);
end;

{ TRenovarWork }

constructor TRenovarWork.Create(AForm: TfrmLockDistribuido;
  const AChave, AToken: string; ATtlMs: Integer);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
  FToken := AToken;
  FTtlMs := ATtlMs;
end;

procedure TRenovarWork.Execute;
var
  LClient: TRedisClient;
  LMarshal: TRenovarMarshal;
  LReply: IRedisReply;
begin
  LMarshal := TRenovarMarshal.Create;
  LMarshal.Form := FForm;

  if not FForm.UsarCliente(LClient) then
  begin
    LMarshal.Sucesso := False;
    LMarshal.ComConexao := False;
    TThread.Queue(nil, LMarshal.Execute);
    Exit;
  end;
  LMarshal.ComConexao := True;

  try
    try
      LReply := LClient.Scripting.Run(SCRIPT_RENOVA, [FChave],
        [FToken, FTtlMs]);
      LMarshal.Sucesso := LReply.AsInteger = 1;
    except
      on E: Exception do
      begin
        LMarshal.Sucesso := False;
        PostaLog(FForm, 'ERRO ao renovar: ' + E.ClassName + ': ' + E.Message);
        LMarshal.JaExplicado := True;
      end;
    end;
  finally
    FForm.SoltarCliente;
  end;

  TThread.Queue(nil, LMarshal.Execute);
end;

{ TAmostraWork }

constructor TAmostraWork.Create(AForm: TfrmLockDistribuido; const AChave: string);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
end;

procedure TAmostraWork.Execute;
var
  LClient: TRedisClient;
  LMarshal: TAmostraMarshal;
  LDono: string;
  LPttl: Int64;
begin
  LDono := '';
  LPttl := -2;
  if FForm.UsarCliente(LClient) then
  try
    try
      if not LClient.Strings.TryGet(FChave, LDono) then
        LDono := '';
      LPttl := LClient.Keys.PTtl(FChave);
    except
      // Amostragem de tela nao merece caixa de erro nem log a cada 500 ms: se
      // a conexao caiu, o proximo Adquirir/Liberar de verdade vai dizer.
      on E: Exception do
      begin
        LDono := '';
        LPttl := -2;
      end;
    end;
  finally
    FForm.SoltarCliente;
  end;

  LMarshal := TAmostraMarshal.Create;
  LMarshal.Form := FForm;
  LMarshal.Dono := LDono;
  LMarshal.PttlMs := LPttl;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ ---------------------------------------------------------------------------
  TfrmLockDistribuido
  --------------------------------------------------------------------------- }

procedure TfrmLockDistribuido.FormCreate(Sender: TObject);
begin
  Randomize;
  FLock := TCriticalSection.Create;
  AtualizaBotoes(False);
  Log('Suba o servidor com: docker compose -f docker/docker-compose.yml up -d');
  Log('Para TLS, use tambem o override docker-compose.tls.yml (porta 6380).');
end;

procedure TfrmLockDistribuido.FormCloseQuery(Sender: TObject;
  var CanClose: Boolean);
var
  LClient: TRedisClient;
  LEspera: Integer;
begin
  CanClose := True;
  tmrAmostra.Enabled := False;
  tmrConcorrente.Enabled := False;
  tmrRenovar.Enabled := False;

  FLock.Enter;
  try
    FEncerrando := True;
    LClient := FClient;
  finally
    FLock.Leave;
  end;
  if LClient = nil then
    Exit;

  // Mesma logica do CacheAsideVcl: espera na propria thread da UI porque a
  // janela ja' esta' indo embora, com teto para nao pendurar a aplicacao.
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

procedure TfrmLockDistribuido.FormDestroy(Sender: TObject);
begin
  FLock.Free;
end;

function TfrmLockDistribuido.UsarCliente(out AClient: TRedisClient): Boolean;
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

procedure TfrmLockDistribuido.SoltarCliente;
begin
  FLock.Enter;
  try
    Dec(FEmVoo);
  finally
    FLock.Leave;
  end;
end;

function TfrmLockDistribuido.ChaveDoLock: string;
var
  LRecurso: string;
begin
  LRecurso := Trim(edtRecurso.Text);
  if LRecurso = '' then
    LRecurso := 'pedido:42';
  Result := PREFIXO + LRecurso;
end;

function TfrmLockDistribuido.TtlEscolhido: Integer;
begin
  Result := StrToIntDef(Trim(edtTtl.Text), 4000);
  if Result < 200 then
    Result := 200;
end;

procedure TfrmLockDistribuido.Log(const ATexto: string);
begin
  mmLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + ATexto);
  mmLog.Perform(WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmLockDistribuido.AtualizaBotoes(AConectado: Boolean);
begin
  btnConectar.Enabled := not AConectado;
  btnDesconectar.Enabled := AConectado;
  edtHost.Enabled := not AConectado;
  edtPorta.Enabled := not AConectado;
  edtSenha.Enabled := not AConectado;
  edtDb.Enabled := not AConectado;
  chkTls.Enabled := not AConectado;

  gbLock.Enabled := AConectado;
  gbConcorrente.Enabled := AConectado;
  gbLiberacao.Enabled := AConectado;
  gbRenovacao.Enabled := AConectado;

  AtualizaEstadoLock;
end;

procedure TfrmLockDistribuido.AtualizaEstadoLock;
begin
  // Sem token nao ha' o que comparar: liberar so' pode ser pela via
  // insegura, e e' assim de proposito -- e' a licao da variante "sem token".
  btnLiberarSeguro.Enabled := chkComToken.Checked and (FMeuToken <> '');
end;

function TfrmLockDistribuido.LeParams(out AParams: TRedisParams): Boolean;
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
  AParams.ClientName := 'LockDistribuidoVcl';
  if AParams.UseTls then
    AParams.TlsVerifyPeer := False;

  if AParams.Host = '' then
  begin
    ShowMessage('Informe o host.');
    Result := False;
  end;
end;

procedure TfrmLockDistribuido.chkTlsClick(Sender: TObject);
begin
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

procedure TfrmLockDistribuido.btnConectarClick(Sender: TObject);
var
  LParams: TRedisParams;
begin
  if not LeParams(LParams) then
    Exit;
  btnConectar.Enabled := False;
  lblStatus.Caption := 'Conectando...';
  RedisPool.Queue(TConectarWork.Create(Self, LParams));
end;

procedure TfrmLockDistribuido.btnDesconectarClick(Sender: TObject);
begin
  btnDesconectar.Enabled := False;
  tmrAmostra.Enabled := False;
  tmrConcorrente.Enabled := False;
  tmrRenovar.Enabled := False;
  lblStatus.Caption := 'Encerrando as operacoes em voo...';
  RedisPool.Queue(TDesconectarWork.Create(Self));
end;

procedure TfrmLockDistribuido.ConexaoAberta(const AInfo: string);
begin
  AtualizaBotoes(True);
  lblStatus.Caption := 'Conectado a ' + AInfo + '  |  backend TLS: ' +
    RedisTlsBackendName;
  Log('conectado a ' + AInfo);
  tmrAmostra.Enabled := True;
  tmrConcorrente.Enabled := chkConcorrente.Checked;
end;

procedure TfrmLockDistribuido.ConexaoFechada(const AMotivo: string);
begin
  AtualizaBotoes(False);
  tmrAmostra.Enabled := False;
  tmrConcorrente.Enabled := False;
  tmrRenovar.Enabled := False;
  chkConcorrente.Checked := False;
  chkRenovarAuto.Checked := False;
  FMeuToken := '';
  FConcorrenteToken := '';
  lblMeuToken.Caption := 'Meu token: -';
  lblConcorrenteToken.Caption := 'Token do concorrente: -';
  lblDono.Caption := 'Dono atual no servidor: -';
  lblTtlRestante.Caption := 'TTL restante: -';
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

procedure TfrmLockDistribuido.chkComTokenClick(Sender: TObject);
begin
  AtualizaEstadoLock;
end;

procedure TfrmLockDistribuido.btnAdquirirClick(Sender: TObject);
var
  LToken: string;
begin
  if chkComToken.Checked then
    LToken := GeraToken('eu')
  else
    LToken := 'sem-token';
  RedisPool.Queue(TAdquirirWork.Create(Self, ChaveDoLock, LToken,
    TtlEscolhido, False));
end;

procedure TfrmLockDistribuido.MostraAdquirir(ASucesso: Boolean;
  const AToken: string; AConcorrente, AJaExplicado: Boolean);
begin
  if AConcorrente then
  begin
    FLock.Enter;
    try
      FConcorrenteEmVoo := False;
    finally
      FLock.Leave;
    end;
    if ASucesso then
    begin
      FConcorrenteToken := AToken;
      lblConcorrenteToken.Caption := 'Token do concorrente: ' + AToken;
      Log('concorrente ADQUIRIU o lock com token ' + AToken);
    end;
    // Falha do concorrente e' silenciosa: ele so' tenta de novo no proximo
    // tick, e logar toda tentativa encheria o log sem ensinar nada novo.
    Exit;
  end;

  if ASucesso then
  begin
    FMeuToken := AToken;
    lblMeuToken.Caption := 'Meu token: ' + AToken;
    Log('adquiri o lock com token ' + AToken);
  end
  else if not AJaExplicado then
    Log('NAO consegui adquirir -- a chave ja esta ocupada');
  AtualizaEstadoLock;
end;

procedure TfrmLockDistribuido.chkConcorrenteClick(Sender: TObject);
begin
  tmrConcorrente.Enabled := chkConcorrente.Checked and (FClient <> nil);
  if chkConcorrente.Checked then
    Log('concorrente simulado ligado: vai tentar adquirir a cada 1s quando a chave estiver livre');
end;

procedure TfrmLockDistribuido.tmrConcorrenteTimer(Sender: TObject);
var
  LOcupado: Boolean;
begin
  FLock.Enter;
  try
    LOcupado := FConcorrenteEmVoo;
    if not LOcupado then
      FConcorrenteEmVoo := True;
  finally
    FLock.Leave;
  end;
  if LOcupado then
    Exit;
  RedisPool.Queue(TAdquirirWork.Create(Self, ChaveDoLock, GeraToken('concorrente'),
    TtlEscolhido, True));
end;

procedure TfrmLockDistribuido.btnLiberarSeguroClick(Sender: TObject);
begin
  if FMeuToken = '' then
  begin
    ShowMessage('Voce ainda nao adquiriu um lock com token.');
    Exit;
  end;
  RedisPool.Queue(TLiberarWork.Create(Self, ChaveDoLock, FMeuToken, True));
end;

procedure TfrmLockDistribuido.btnLiberarArmadilhaClick(Sender: TObject);
begin
  RedisPool.Queue(TLiberarWork.Create(Self, ChaveDoLock, FMeuToken, False));
end;

procedure TfrmLockDistribuido.MostraLiberar(ASeguro, ASucesso: Boolean;
  const AValorApagado: string; AJaExplicado: Boolean);
var
  LEraMeu: Boolean;
begin
  if AJaExplicado then
    // Erro de servidor/rede ja' foi logado pelo worker: nao sabemos se o
    // lock ainda e' meu, entao nao mexe no estado (nem AtualizaEstadoLock).
    Exit;

  LEraMeu := (AValorApagado <> '') and (AValorApagado = FMeuToken);

  if ASeguro then
  begin
    if ASucesso then
      Log('liberei com o script (compare-and-delete): era mesmo o meu lock')
    else
      Log('o script nao apagou nada -- eu ja nao era mais o dono ' +
        '(o TTL deve ter passado antes)');
  end
  else
  begin
    if AValorApagado = '' then
      Log('DEL direto: a chave ja estava livre, nada para apagar')
    else if LEraMeu then
      Log('DEL direto apagou o MEU proprio lock (token ' + AValorApagado + ')')
    else
      Log('ARMADILHA: DEL direto apagou um lock que NAO era meu (token ' +
        AValorApagado + ') -- provavelmente o do concorrente');
  end;

  FMeuToken := '';
  lblMeuToken.Caption := 'Meu token: -';
  chkRenovarAuto.Checked := False;
  tmrRenovar.Enabled := False;
  AtualizaEstadoLock;
end;

procedure TfrmLockDistribuido.chkRenovarAutoClick(Sender: TObject);
begin
  if chkRenovarAuto.Checked and (FMeuToken = '') then
  begin
    chkRenovarAuto.Checked := False;
    ShowMessage('Adquira o lock com token primeiro.');
    Exit;
  end;
  tmrRenovar.Enabled := chkRenovarAuto.Checked;
  if chkRenovarAuto.Checked then
    Log('renovacao automatica ligada: PEXPIRE a cada 1s enquanto eu for o dono');
end;

procedure TfrmLockDistribuido.tmrRenovarTimer(Sender: TObject);
var
  LOcupado: Boolean;
  LToken: string;
begin
  LToken := FMeuToken;
  if LToken = '' then
  begin
    tmrRenovar.Enabled := False;
    chkRenovarAuto.Checked := False;
    Exit;
  end;
  FLock.Enter;
  try
    LOcupado := FRenovarEmVoo;
    if not LOcupado then
      FRenovarEmVoo := True;
  finally
    FLock.Leave;
  end;
  if LOcupado then
    Exit;
  RedisPool.Queue(TRenovarWork.Create(Self, ChaveDoLock, LToken, TtlEscolhido));
end;

procedure TfrmLockDistribuido.MostraRenovacao(ASucesso, AJaExplicado: Boolean);
begin
  FLock.Enter;
  try
    FRenovarEmVoo := False;
  finally
    FLock.Leave;
  end;

  if AJaExplicado then
    // Erro de servidor/rede ja' foi logado pelo worker: mantem o lock como
    // "meu" e tenta renovar de novo no proximo tick, em vez de desistir.
    Exit;

  if ASucesso then
    Log('renovado: PEXPIRE aplicado, continuo sendo o dono')
  else
  begin
    Log('renovacao falhou: eu ja nao sou mais o dono ' +
      '(o TTL passou antes da renovacao chegar)');
    FMeuToken := '';
    lblMeuToken.Caption := 'Meu token: -';
    chkRenovarAuto.Checked := False;
    tmrRenovar.Enabled := False;
    AtualizaEstadoLock;
  end;
end;

procedure TfrmLockDistribuido.tmrAmostraTimer(Sender: TObject);
var
  LOcupado: Boolean;
begin
  FLock.Enter;
  try
    LOcupado := FAmostraEmVoo or (FClient = nil) or FEncerrando;
    if not LOcupado then
      FAmostraEmVoo := True;
  finally
    FLock.Leave;
  end;
  if LOcupado then
    Exit;
  RedisPool.Queue(TAmostraWork.Create(Self, ChaveDoLock));
end;

procedure TfrmLockDistribuido.MostraAmostra(const ADono: string; APttlMs: Int64);
begin
  FLock.Enter;
  try
    FAmostraEmVoo := False;
  finally
    FLock.Leave;
  end;

  if ADono = '' then
    lblDono.Caption := 'Dono atual no servidor: ninguem (livre)'
  else if ADono = FMeuToken then
    lblDono.Caption := 'Dono atual no servidor: EU (' + ADono + ')'
  else if ADono = FConcorrenteToken then
    lblDono.Caption := 'Dono atual no servidor: concorrente (' + ADono + ')'
  else
    lblDono.Caption := 'Dono atual no servidor: ' + ADono;

  if APttlMs < 0 then
    lblTtlRestante.Caption := 'TTL restante: -'
  else
    lblTtlRestante.Caption := 'TTL restante: ' + IntToStr(APttlMs) + ' ms';
end;

procedure TfrmLockDistribuido.btnLimparClick(Sender: TObject);
begin
  mmLog.Lines.Clear;
end;

end.
