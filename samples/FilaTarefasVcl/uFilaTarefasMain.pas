unit uFilaTarefasMain;

{ FILA DE TAREFAS DURAVEL: um stream com um consumer group e' o unico tipo do
  Redis com entrega confiavel. A diferenca para pub/sub cabe numa frase: aqui
  a tarefa fica GRAVADA, e o grupo registra quem recebeu (a PEL, pending
  entries list) ate' alguem confirmar com XACK. Se o worker morre no meio, a
  tarefa nao evapora -- fica pendente, esperando alguem reivindicar.

  ARMADILHA 1 (pendencia sobrevivendo ao worker morto): o botao "Matar" faz o
  consumidor receber a proxima tarefa e parar SEM confirmar -- exatamente como
  um processo que morre no meio do trabalho. A tarefa fica na PEL dele para
  sempre, ate' que "Reivindicar (XAUTOCLAIM)" a transfira para o consumidor
  "recuperador", que processa e confirma.

  ARMADILHA 2 (entrada apagada): "Apagar a ultima tarefa abandonada (XDEL)"
  tira a entrada do STREAM, mas NAO da PEL -- a pendencia continua ali.

  A forma de ver isso e' RETOMAR pelo MESMO consumidor que a recebeu
  (XREADGROUP com '0', relendo a propria PEL): a entrada chega SEM CAMPOS
  (TRedisStreamEntry.IsDeleted = True), porque so' sobrou o id. Achado
  testando contra o servidor: XAUTOCLAIM/XCLAIM NAO se comportam assim no
  Redis 7+ -- eles PURGAM sozinhos da PEL a entrada que ja' nao existe mais no
  stream (reportando o id na terceira parte da resposta, ADeletedIds) e nunca
  a devolvem no array principal. IsDeleted so' aparece por quem rele' a PEL
  diretamente (XREADGROUP '0' ou XPENDING); por isso o sample tem os DOIS
  botoes -- "Retomar" (mesmo consumidor, mostra o IsDeleted) e "Reivindicar"
  (XAUTOCLAIM, mostra a purga automatica do Redis 7+).

  VARIACAO (um x dois consumidores): os grupos "Consumidor A" e "Consumidor B"
  sao independentes -- ligue so' um para ver a fila se acumular sem ninguem
  reivindicar o que o outro abandonou, ou os dois para ver o trabalho se
  repartir e o sobrevivente seguir trabalhando enquanto o morto fica pendente.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild FilaTarefasVcl.lpi
    Delphi: abrir FilaTarefasVcl.dproj no IDE

  PADRAO DE THREADS: o mesmo dos outros samples do M9 (nada de rede na main
  thread, marshal descartavel + TThread.Queue, cliente contado por
  UsarCliente/SoltarCliente -- ver o cabecalho de uCacheAsideMain.pas). A
  novidade aqui e' que cada consumidor e' um LACO PERSISTENTE num worker do
  RedisPool, nao uma operacao unica: ele fica bloqueado em XReadGroupBlocking,
  acorda com uma tarefa ou com o timeout, e repete ate' ser desligado. Cada
  ida ao servidor (o XReadGroupBlocking, o XAck) tem seu PROPRIO
  UsarCliente/SoltarCliente -- nunca um so' abrangendo o laco inteiro --
  porque isso e' o que deixa o FEmVoo do TRedisClient.Create refletir o que
  esta' em voo AGORA, e nao "o laco inteiro enquanto ele existir". Sem isso,
  desconectar com um consumidor ligado esperaria o laco inteiro morrer, e nao
  so' a chamada corrente.

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
  Redis.Commands, Redis.Commands.Streams;

type
  TfrmFilaTarefas = class(TForm)
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
    gbFila: TGroupBox;
    lblFila: TLabel;
    edtFila: TEdit;
    btnCriarFila: TButton;
    lblFilaInfo: TLabel;
    lblTamanho: TLabel;
    lblPendencias: TLabel;
    gbProducao: TGroupBox;
    lblPayload: TLabel;
    edtPayload: TEdit;
    btnAdicionar: TButton;
    btnAdicionar5: TButton;
    gbConsumidorA: TGroupBox;
    chkLigadoA: TCheckBox;
    lblAtrasoA: TLabel;
    edtAtrasoA: TEdit;
    btnMatarA: TButton;
    lblStatusA: TLabel;
    gbConsumidorB: TGroupBox;
    chkLigadoB: TCheckBox;
    lblAtrasoB: TLabel;
    edtAtrasoB: TEdit;
    btnMatarB: TButton;
    lblStatusB: TLabel;
    gbRecuperacao: TGroupBox;
    lblIdleMin: TLabel;
    edtIdleMin: TEdit;
    btnReivindicar: TButton;
    btnApagarAbandonada: TButton;
    btnRetomar: TButton;
    lblArmadilha: TLabel;
    mmLog: TMemo;
    btnLimpar: TButton;
    tmrAmostra: TTimer;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormCloseQuery(Sender: TObject; var CanClose: Boolean);
    procedure chkTlsClick(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnDesconectarClick(Sender: TObject);
    procedure btnCriarFilaClick(Sender: TObject);
    procedure btnAdicionarClick(Sender: TObject);
    procedure btnAdicionar5Click(Sender: TObject);
    procedure chkLigadoAClick(Sender: TObject);
    procedure chkLigadoBClick(Sender: TObject);
    procedure btnMatarAClick(Sender: TObject);
    procedure btnMatarBClick(Sender: TObject);
    procedure btnReivindicarClick(Sender: TObject);
    procedure btnApagarAbandonadaClick(Sender: TObject);
    procedure btnRetomarClick(Sender: TObject);
    procedure btnLimparClick(Sender: TObject);
    procedure tmrAmostraTimer(Sender: TObject);
  private
    FLock: TCriticalSection;
    FClient: TRedisClient;
    FEmVoo: Integer;
    FEncerrando: Boolean;
    FAmostraEmVoo: Boolean;
    FLigadoA, FLigadoB: Boolean;
    FRodandoA, FRodandoB: Boolean;
    FMatarA, FMatarB: Boolean;
    FContador: Integer;
    { So' escrito pela thread da UI (sempre via marshal) -- sem lock. }
    FUltimoAbandonadoId: string;
    FUltimoAbandonadoConsumidor: string;
    procedure AtualizaBotoes(AConectado: Boolean);
    function LeParams(out AParams: TRedisParams): Boolean;
    function ChaveFila: string;
    function AtrasoEscolhido(AEdit: TEdit): Integer;
  public
    { --- Chamados pelos workers, sempre pela thread da UI (via marshal) --- }
    procedure Log(const ATexto: string);
    procedure ConexaoAberta(const AInfo: string);
    procedure ConexaoFechada(const AMotivo: string);
    procedure ConsumidorParou(const ANome: string; AMorreu: Boolean;
      const AIdAbandonado, ADescricao: string);
    procedure MostraAmostra(ATamanho, APendCount: Int64;
      const APendMinId, APendMaxId: string);

    { --- Usados pelos workers, de qualquer thread --- }
    function UsarCliente(out AClient: TRedisClient): Boolean;
    procedure SoltarCliente;
    function ConsumidorLigado(const ANome: string): Boolean;
    function ConsomeFlagMatar(const ANome: string): Boolean;
  end;

const
  PREFIXO = 'pascal-redis-faa:filatarefas:';
  GRUPO = 'trabalhadores';
  CONSUMIDOR_RECUPERADOR = 'recuperador';
  BLOCK_MS = 1500;

var
  frmFilaTarefas: TfrmFilaTarefas;

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
  --------------------------------------------------------------------------- }

type
  TLogMarshal = class
    Form: TfrmFilaTarefas;
    Texto: string;
    procedure Execute;
  end;

  TStatusMarshal = class
    Form: TfrmFilaTarefas;
    Aberta: Boolean;
    Texto: string;
    procedure Execute;
  end;

  TConsumidorFimMarshal = class
    Form: TfrmFilaTarefas;
    Nome: string;
    Morreu: Boolean;
    IdAbandonado: string;
    Descricao: string;
    procedure Execute;
  end;

  TAmostraMarshal = class
    Form: TfrmFilaTarefas;
    Tamanho: Int64;
    PendCount: Int64;
    PendMinId: string;
    PendMaxId: string;
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

procedure TConsumidorFimMarshal.Execute;
begin
  Form.ConsumidorParou(Nome, Morreu, IdAbandonado, Descricao);
  Free;
end;

procedure TAmostraMarshal.Execute;
begin
  Form.MostraAmostra(Tamanho, PendCount, PendMinId, PendMaxId);
  Free;
end;

procedure PostaLog(AForm: TfrmFilaTarefas; const ATexto: string);
var
  LMarshal: TLogMarshal;
begin
  LMarshal := TLogMarshal.Create;
  LMarshal.Form := AForm;
  LMarshal.Texto := ATexto;
  TThread.Queue(nil, LMarshal.Execute);
end;

procedure PostaStatus(AForm: TfrmFilaTarefas; AAberta: Boolean;
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

procedure PostaFim(AForm: TfrmFilaTarefas; const ANome: string; AMorreu: Boolean;
  const AId, ADescricao: string);
var
  LMarshal: TConsumidorFimMarshal;
begin
  LMarshal := TConsumidorFimMarshal.Create;
  LMarshal.Form := AForm;
  LMarshal.Nome := ANome;
  LMarshal.Morreu := AMorreu;
  LMarshal.IdAbandonado := AId;
  LMarshal.Descricao := ADescricao;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ ---------------------------------------------------------------------------
  Work items: rodam num worker do RedisPool (threads persistentes).
  --------------------------------------------------------------------------- }

type
  TConectarWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FParams: TRedisParams;
  public
    constructor Create(AForm: TfrmFilaTarefas; const AParams: TRedisParams);
    procedure Execute; override;
  end;

  TDesconectarWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
  public
    constructor Create(AForm: TfrmFilaTarefas);
    procedure Execute; override;
  end;

  TCriarFilaWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FChave: string;
  public
    constructor Create(AForm: TfrmFilaTarefas; const AChave: string);
    procedure Execute; override;
  end;

  TAdicionarWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FChave: string;
    FDescricao: string;
  public
    constructor Create(AForm: TfrmFilaTarefas; const AChave, ADescricao: string);
    procedure Execute; override;
  end;

  { Laco PERSISTENTE: fica rodando ate' ser desligado (checkbox) ou morrer
    (botao Matar). Ver o comentario de cabecalho da unit sobre por que cada
    ida ao servidor tem seu proprio UsarCliente/SoltarCliente. }
  TConsumidorWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FNome: string;
    FChave: string;
    FAtrasoMs: Integer;
  public
    constructor Create(AForm: TfrmFilaTarefas; const ANome, AChave: string;
      AAtrasoMs: Integer);
    procedure Execute; override;
  end;

  TReivindicarWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FChave: string;
    FMinIdleMs: Int64;
  public
    constructor Create(AForm: TfrmFilaTarefas; const AChave: string;
      AMinIdleMs: Int64);
    procedure Execute; override;
  end;

  { Rele' a PEL do MESMO consumidor que recebeu a tarefa (XREADGROUP com '0'),
    como um worker que reinicia e retoma o proprio trabalho nao confirmado.
    E' o unico caminho deste sample que exercita TRedisStreamEntry.IsDeleted:
    XAUTOCLAIM/XCLAIM purgam a entrada apagada sozinhos no Redis 7+ (ver
    TReivindicarWork), mas reler a PEL diretamente ainda mostra o id com os
    campos nulos. }
  TRetomarWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FChave: string;
    FConsumidor: string;
  public
    constructor Create(AForm: TfrmFilaTarefas; const AChave, AConsumidor: string);
    procedure Execute; override;
  end;

  TApagarAbandonadaWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FChave: string;
    FId: string;
  public
    constructor Create(AForm: TfrmFilaTarefas; const AChave, AId: string);
    procedure Execute; override;
  end;

  TAmostraWork = class(TRedisWorkItem)
  private
    FForm: TfrmFilaTarefas;
    FChave: string;
  public
    constructor Create(AForm: TfrmFilaTarefas; const AChave: string);
    procedure Execute; override;
  end;

{ TConectarWork }

constructor TConectarWork.Create(AForm: TfrmFilaTarefas;
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
    // apareceria no primeiro Adicionar/Consumidor, longe do botao que causou
    // o problema.
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

constructor TDesconectarWork.Create(AForm: TfrmFilaTarefas);
begin
  inherited Create;
  FForm := AForm;
end;

procedure TDesconectarWork.Execute;
var
  LClient: TRedisClient;
  LEmVoo: Integer;
begin
  // FEncerrando := True e' o que faz ConsumidorLigado devolver False e
  // UsarCliente recusar: os lacos dos consumidores notam isso na proxima
  // volta (o mais tardar, quando o XReadGroupBlocking atual estourar o
  // BLOCK_MS) e terminam sozinhos, sem que este work precise saber deles.
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

{ TCriarFilaWork }

constructor TCriarFilaWork.Create(AForm: TfrmFilaTarefas; const AChave: string);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
end;

procedure TCriarFilaWork.Execute;
var
  LClient: TRedisClient;
  LCriada: Boolean;
begin
  if not FForm.UsarCliente(LClient) then
  begin
    PostaLog(FForm, 'sem conexao');
    Exit;
  end;
  try
    try
      LCriada := LClient.Streams.XGroupTryCreate(FChave, GRUPO, '0', True);
      if LCriada then
        PostaLog(FForm, 'grupo "' + GRUPO + '" criado em ' + FChave +
          ' (enxerga desde o inicio do stream)')
      else
        PostaLog(FForm, 'grupo "' + GRUPO + '" ja existia em ' + FChave +
          ' -- nada mudou');
    except
      on E: Exception do
        PostaLog(FForm, 'ERRO ao criar a fila: ' + E.ClassName + ': ' + E.Message);
    end;
  finally
    FForm.SoltarCliente;
  end;
end;

{ TAdicionarWork }

constructor TAdicionarWork.Create(AForm: TfrmFilaTarefas;
  const AChave, ADescricao: string);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
  FDescricao := ADescricao;
end;

procedure TAdicionarWork.Execute;
var
  LClient: TRedisClient;
  LId: string;
begin
  if not FForm.UsarCliente(LClient) then
  begin
    PostaLog(FForm, 'sem conexao');
    Exit;
  end;
  try
    try
      LId := LClient.Streams.XAdd(FChave, ['descricao', FDescricao]);
      PostaLog(FForm, 'XADD ' + LId + ': ' + FDescricao);
    except
      on E: Exception do
        PostaLog(FForm, 'ERRO ao adicionar: ' + E.ClassName + ': ' + E.Message);
    end;
  finally
    FForm.SoltarCliente;
  end;
end;

{ TConsumidorWork }

constructor TConsumidorWork.Create(AForm: TfrmFilaTarefas;
  const ANome, AChave: string; AAtrasoMs: Integer);
begin
  inherited Create;
  FForm := AForm;
  FNome := ANome;
  FChave := AChave;
  FAtrasoMs := AAtrasoMs;
end;

procedure TConsumidorWork.Execute;
var
  LClient: TRedisClient;
  LDados: TRedisStreamDataArray;
  LEntrada: TRedisStreamEntry;
  LTemEntrada, LErro, LMatar: Boolean;
  LDescricao: string;
begin
  while True do
  begin
    if not FForm.ConsumidorLigado(FNome) then
      Break;

    if not FForm.UsarCliente(LClient) then
    begin
      PostaLog(FForm, 'Consumidor ' + FNome + ': sem conexao, parando');
      Break;
    end;

    LTemEntrada := False;
    LErro := False;
    try
      try
        // '>' pede o que nunca foi entregue a ninguem do grupo -- e cria
        // pendencia na PEL deste consumidor a partir de agora.
        LDados := LClient.Streams.XReadGroupBlocking(GRUPO, FNome, [FChave],
          [REDIS_STREAM_NEW], BLOCK_MS, 1);
        LTemEntrada := (Length(LDados) > 0) and (Length(LDados[0].Entries) > 0);
        if LTemEntrada then
          LEntrada := LDados[0].Entries[0];
      except
        on E: Exception do
        begin
          PostaLog(FForm, 'ERRO no consumidor ' + FNome + ': ' + E.ClassName +
            ': ' + E.Message + ' -- parando (crie a fila antes de ligar)');
          LErro := True;
        end;
      end;
    finally
      FForm.SoltarCliente;
    end;

    if LErro then
      Break;
    if not LTemEntrada then
      Continue; // BLOCK estourou sem novidade -- ocioso, normal

    // So' checa o pedido de "morrer" DEPOIS de receber, para o abandono ser
    // sempre de uma tarefa concreta -- e' o que o botao Matar promete.
    LMatar := FForm.ConsomeFlagMatar(FNome);
    if LMatar then
    begin
      LDescricao := LEntrada.FieldValue('descricao');
      PostaFim(FForm, FNome, True, LEntrada.Id, LDescricao);
      Exit; // sem XACK: a tarefa fica pendente na PEL deste consumidor
    end;

    PostaLog(FForm, 'Consumidor ' + FNome + ' recebeu ' + LEntrada.Id + ': ' +
      LEntrada.FieldValue('descricao'));
    Sleep(FAtrasoMs); // simula o processamento -- fora de qualquer lock

    if FForm.UsarCliente(LClient) then
    try
      try
        LClient.Streams.XAck(FChave, GRUPO, [LEntrada.Id]);
        PostaLog(FForm, 'Consumidor ' + FNome + ' confirmou ' + LEntrada.Id);
      except
        on E: Exception do
          PostaLog(FForm, 'ERRO ao confirmar ' + LEntrada.Id + ': ' +
            E.ClassName + ': ' + E.Message);
      end;
    finally
      FForm.SoltarCliente;
    end
    else
      PostaLog(FForm, 'sem conexao para confirmar ' + LEntrada.Id);
  end;

  PostaFim(FForm, FNome, False, '', '');
end;

{ TReivindicarWork }

constructor TReivindicarWork.Create(AForm: TfrmFilaTarefas; const AChave: string;
  AMinIdleMs: Int64);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
  FMinIdleMs := AMinIdleMs;
end;

procedure TReivindicarWork.Execute;
var
  LClient: TRedisClient;
  LEntradas: TRedisStreamEntryArray;
  LProximo: string;
  LApagadas: TRedisStringArray;
  I: Integer;
  LNada: Boolean;
begin
  if not FForm.UsarCliente(LClient) then
  begin
    PostaLog(FForm, 'sem conexao');
    Exit;
  end;
  try
    try
      // XAUTOCLAIM = XPENDING + XCLAIM num comando so': varre a PEL do grupo e
      // reivindica de uma vez o que estiver parado ha' pelo menos FMinIdleMs.
      // No Redis 7+ ele tambem PURGA sozinho da PEL qualquer id que ja' nao
      // exista mais no stream -- devolvidos em LApagadas, nao em LEntradas, e
      // sem precisar de XACK (o servidor ja' tirou da PEL).
      LEntradas := LClient.Streams.XAutoClaim(FChave, GRUPO,
        CONSUMIDOR_RECUPERADOR, FMinIdleMs, '0-0', 100, LProximo, LApagadas);
      LNada := True;
      for I := 0 to High(LEntradas) do
      begin
        LNada := False;
        PostaLog(FForm, 'reivindicada ' + LEntradas[I].Id + ': ' +
          LEntradas[I].FieldValue('descricao') + ' -- confirmando');
        LClient.Streams.XAck(FChave, GRUPO, [LEntradas[I].Id]);
      end;
      for I := 0 to High(LApagadas) do
      begin
        LNada := False;
        PostaLog(FForm, 'XAUTOCLAIM tirou ' + LApagadas[I] +
          ' da PEL sozinho -- ja nao existia mais no stream (armadilha da ' +
          'entrada apagada); nao precisa de XACK');
      end;
      if LNada then
        PostaLog(FForm, 'reivindicar: nenhuma pendencia ociosa ha ' +
          IntToStr(FMinIdleMs) + 'ms ou mais');
    except
      on E: Exception do
        PostaLog(FForm, 'ERRO ao reivindicar: ' + E.ClassName + ': ' + E.Message);
    end;
  finally
    FForm.SoltarCliente;
  end;
end;

{ TRetomarWork }

constructor TRetomarWork.Create(AForm: TfrmFilaTarefas;
  const AChave, AConsumidor: string);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
  FConsumidor := AConsumidor;
end;

procedure TRetomarWork.Execute;
var
  LClient: TRedisClient;
  LDados: TRedisStreamDataArray;
  I: Integer;
begin
  if not FForm.UsarCliente(LClient) then
  begin
    PostaLog(FForm, 'sem conexao');
    Exit;
  end;
  try
    try
      // '0' rele' a PEL DESTE consumidor, do inicio -- como ele proprio
      // reiniciando e retomando o que havia recebido e nao confirmado.
      LDados := LClient.Streams.XReadGroup(GRUPO, FConsumidor, [FChave],
        [REDIS_STREAM_PENDING], 50);
      if (Length(LDados) = 0) or (Length(LDados[0].Entries) = 0) then
        PostaLog(FForm, 'retomar (' + FConsumidor +
          '): a propria PEL esta vazia -- nada para retomar')
      else
        for I := 0 to High(LDados[0].Entries) do
        begin
          if LDados[0].Entries[I].IsDeleted then
            // Armadilha 2, vista pelo lado certo: sem campos, so' o id --
            // a entrada foi apagada do stream enquanto ficava pendente.
            PostaLog(FForm, 'retomando ' + LDados[0].Entries[I].Id +
              ' (' + FConsumidor + ') -- SEM CAMPOS (IsDeleted): foi apagada' +
              ' do stream enquanto pendente; confirmando so para tirar da PEL')
          else
            PostaLog(FForm, 'retomando ' + LDados[0].Entries[I].Id + ' (' +
              FConsumidor + '): ' +
              LDados[0].Entries[I].FieldValue('descricao') + ' -- confirmando');
          LClient.Streams.XAck(FChave, GRUPO, [LDados[0].Entries[I].Id]);
        end;
    except
      on E: Exception do
        PostaLog(FForm, 'ERRO ao retomar: ' + E.ClassName + ': ' + E.Message);
    end;
  finally
    FForm.SoltarCliente;
  end;
end;

{ TApagarAbandonadaWork }

constructor TApagarAbandonadaWork.Create(AForm: TfrmFilaTarefas;
  const AChave, AId: string);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
  FId := AId;
end;

procedure TApagarAbandonadaWork.Execute;
var
  LClient: TRedisClient;
  LApagou: Int64;
begin
  if not FForm.UsarCliente(LClient) then
  begin
    PostaLog(FForm, 'sem conexao');
    Exit;
  end;
  try
    try
      // XDEL tira do STREAM, mas NAO da PEL do grupo -- a pendencia continua
      // ali, e' esta a armadilha 2.
      LApagou := LClient.Streams.XDel(FChave, [FId]);
      if LApagou > 0 then
        PostaLog(FForm, 'XDEL apagou ' + FId + ' do STREAM -- continua ' +
          'pendente no grupo, so que sem campos agora')
      else
        PostaLog(FForm, 'XDEL: ' + FId + ' ja nao estava mais no stream');
    except
      on E: Exception do
        PostaLog(FForm, 'ERRO ao apagar: ' + E.ClassName + ': ' + E.Message);
    end;
  finally
    FForm.SoltarCliente;
  end;
end;

{ TAmostraWork }

constructor TAmostraWork.Create(AForm: TfrmFilaTarefas; const AChave: string);
begin
  inherited Create;
  FForm := AForm;
  FChave := AChave;
end;

procedure TAmostraWork.Execute;
var
  LClient: TRedisClient;
  LMarshal: TAmostraMarshal;
  LTamanho: Int64;
  LResumo: TRedisPendingSummary;
begin
  LTamanho := -1;
  LResumo.Count := 0;
  LResumo.MinId := '';
  LResumo.MaxId := '';
  if FForm.UsarCliente(LClient) then
  try
    try
      LTamanho := LClient.Streams.XLen(FChave);
      LResumo := LClient.Streams.XPendingSummary(FChave, GRUPO);
    except
      // Amostragem de tela nao merece caixa de erro nem log a cada tick: se a
      // fila ainda nao foi criada (NOGROUP) ou a conexao caiu, a proxima
      // operacao de verdade vai dizer.
      on E: Exception do
      begin
        LTamanho := -1;
        LResumo.Count := 0;
      end;
    end;
  finally
    FForm.SoltarCliente;
  end;

  LMarshal := TAmostraMarshal.Create;
  LMarshal.Form := FForm;
  LMarshal.Tamanho := LTamanho;
  LMarshal.PendCount := LResumo.Count;
  LMarshal.PendMinId := LResumo.MinId;
  LMarshal.PendMaxId := LResumo.MaxId;
  TThread.Queue(nil, LMarshal.Execute);
end;

{ ---------------------------------------------------------------------------
  TfrmFilaTarefas
  --------------------------------------------------------------------------- }

procedure TfrmFilaTarefas.FormCreate(Sender: TObject);
begin
  FLock := TCriticalSection.Create;
  AtualizaBotoes(False);
  Log('Suba o servidor com: docker compose -f docker/docker-compose.yml up -d');
  Log('Para TLS, use tambem o override docker-compose.tls.yml (porta 6380).');
end;

procedure TfrmFilaTarefas.FormCloseQuery(Sender: TObject; var CanClose: Boolean);
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

  // Mesma logica dos outros samples: espera na propria thread da UI porque a
  // janela ja' esta' indo embora, com teto para nao pendurar a aplicacao. Os
  // consumidores notam FEncerrando e terminam sozinhos (o mais tardar depois
  // de um BLOCK_MS), entao o teto precisa ser folgado o bastante para isso.
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

procedure TfrmFilaTarefas.FormDestroy(Sender: TObject);
begin
  FLock.Free;
end;

function TfrmFilaTarefas.UsarCliente(out AClient: TRedisClient): Boolean;
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

procedure TfrmFilaTarefas.SoltarCliente;
begin
  FLock.Enter;
  try
    Dec(FEmVoo);
  finally
    FLock.Leave;
  end;
end;

function TfrmFilaTarefas.ConsumidorLigado(const ANome: string): Boolean;
begin
  FLock.Enter;
  try
    if ANome = 'A' then
      Result := FLigadoA and not FEncerrando
    else
      Result := FLigadoB and not FEncerrando;
  finally
    FLock.Leave;
  end;
end;

function TfrmFilaTarefas.ConsomeFlagMatar(const ANome: string): Boolean;
begin
  FLock.Enter;
  try
    if ANome = 'A' then
    begin
      Result := FMatarA;
      FMatarA := False;
    end
    else
    begin
      Result := FMatarB;
      FMatarB := False;
    end;
  finally
    FLock.Leave;
  end;
end;

function TfrmFilaTarefas.ChaveFila: string;
var
  LNome: string;
begin
  LNome := Trim(edtFila.Text);
  if LNome = '' then
    LNome := 'pedidos';
  Result := PREFIXO + LNome;
end;

function TfrmFilaTarefas.AtrasoEscolhido(AEdit: TEdit): Integer;
begin
  Result := StrToIntDef(Trim(AEdit.Text), 1000);
  if Result < 0 then
    Result := 0;
end;

procedure TfrmFilaTarefas.Log(const ATexto: string);
begin
  mmLog.Lines.Add(FormatDateTime('hh:nn:ss.zzz', Now) + '  ' + ATexto);
  mmLog.Perform(WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmFilaTarefas.AtualizaBotoes(AConectado: Boolean);
begin
  btnConectar.Enabled := not AConectado;
  btnDesconectar.Enabled := AConectado;
  edtHost.Enabled := not AConectado;
  edtPorta.Enabled := not AConectado;
  edtSenha.Enabled := not AConectado;
  edtDb.Enabled := not AConectado;
  chkTls.Enabled := not AConectado;

  gbFila.Enabled := AConectado;
  gbProducao.Enabled := AConectado;
  gbConsumidorA.Enabled := AConectado;
  gbConsumidorB.Enabled := AConectado;
  gbRecuperacao.Enabled := AConectado;
end;

function TfrmFilaTarefas.LeParams(out AParams: TRedisParams): Boolean;
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
  AParams.ClientName := 'FilaTarefasVcl';
  if AParams.UseTls then
    AParams.TlsVerifyPeer := False;

  if AParams.Host = '' then
  begin
    ShowMessage('Informe o host.');
    Result := False;
  end;
end;

procedure TfrmFilaTarefas.chkTlsClick(Sender: TObject);
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

procedure TfrmFilaTarefas.btnConectarClick(Sender: TObject);
var
  LParams: TRedisParams;
begin
  if not LeParams(LParams) then
    Exit;
  btnConectar.Enabled := False;
  lblStatus.Caption := 'Conectando...';
  RedisPool.Queue(TConectarWork.Create(Self, LParams));
end;

procedure TfrmFilaTarefas.btnDesconectarClick(Sender: TObject);
begin
  btnDesconectar.Enabled := False;
  tmrAmostra.Enabled := False;
  lblStatus.Caption := 'Encerrando as operacoes em voo...';
  RedisPool.Queue(TDesconectarWork.Create(Self));
end;

procedure TfrmFilaTarefas.ConexaoAberta(const AInfo: string);
begin
  AtualizaBotoes(True);
  lblStatus.Caption := 'Conectado a ' + AInfo + '  |  backend TLS: ' +
    RedisTlsBackendName;
  Log('conectado a ' + AInfo);
  tmrAmostra.Enabled := True;
end;

procedure TfrmFilaTarefas.ConexaoFechada(const AMotivo: string);
begin
  AtualizaBotoes(False);
  tmrAmostra.Enabled := False;

  // Reset imediato na tela; os lacos dos consumidores ainda vao confirmar
  // sozinhos por ConsumidorParou daqui a pouco, o que e' inofensivo -- so'
  // reafirma o mesmo estado "parado".
  FLock.Enter;
  try
    FLigadoA := False;
    FLigadoB := False;
    FMatarA := False;
    FMatarB := False;
  finally
    FLock.Leave;
  end;
  chkLigadoA.Checked := False;
  chkLigadoB.Checked := False;
  lblStatusA.Caption := 'Status: parado';
  lblStatusB.Caption := 'Status: parado';
  lblTamanho.Caption := 'Tamanho do stream: -';
  lblPendencias.Caption := 'Pendencias no grupo: -';
  FUltimoAbandonadoId := '';
  FUltimoAbandonadoConsumidor := '';

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

procedure TfrmFilaTarefas.btnCriarFilaClick(Sender: TObject);
begin
  RedisPool.Queue(TCriarFilaWork.Create(Self, ChaveFila));
end;

procedure TfrmFilaTarefas.btnAdicionarClick(Sender: TObject);
var
  LTexto: string;
begin
  LTexto := Trim(edtPayload.Text);
  if LTexto = '' then
    LTexto := 'pedido';
  Inc(FContador);
  RedisPool.Queue(TAdicionarWork.Create(Self, ChaveFila,
    LTexto + ' #' + IntToStr(FContador)));
end;

procedure TfrmFilaTarefas.btnAdicionar5Click(Sender: TObject);
var
  LTexto: string;
  I: Integer;
begin
  LTexto := Trim(edtPayload.Text);
  if LTexto = '' then
    LTexto := 'pedido';
  for I := 1 to 5 do
  begin
    Inc(FContador);
    RedisPool.Queue(TAdicionarWork.Create(Self, ChaveFila,
      LTexto + ' #' + IntToStr(FContador)));
  end;
end;

procedure TfrmFilaTarefas.chkLigadoAClick(Sender: TObject);
var
  LJaRodando: Boolean;
begin
  if chkLigadoA.Checked then
  begin
    FLock.Enter;
    try
      FLigadoA := True;
      LJaRodando := FRodandoA;
      if not LJaRodando then
        FRodandoA := True;
    finally
      FLock.Leave;
    end;
    if not LJaRodando then
    begin
      Log('Consumidor A ligado');
      lblStatusA.Caption := 'Status: rodando';
      RedisPool.Queue(TConsumidorWork.Create(Self, 'A', ChaveFila,
        AtrasoEscolhido(edtAtrasoA)));
    end;
  end
  else
  begin
    FLock.Enter;
    try
      FLigadoA := False;
    finally
      FLock.Leave;
    end;
    Log('Consumidor A desligando (pode levar ate ' + IntToStr(BLOCK_MS) +
      'ms para parar de vez)');
  end;
end;

procedure TfrmFilaTarefas.chkLigadoBClick(Sender: TObject);
var
  LJaRodando: Boolean;
begin
  if chkLigadoB.Checked then
  begin
    FLock.Enter;
    try
      FLigadoB := True;
      LJaRodando := FRodandoB;
      if not LJaRodando then
        FRodandoB := True;
    finally
      FLock.Leave;
    end;
    if not LJaRodando then
    begin
      Log('Consumidor B ligado');
      lblStatusB.Caption := 'Status: rodando';
      RedisPool.Queue(TConsumidorWork.Create(Self, 'B', ChaveFila,
        AtrasoEscolhido(edtAtrasoB)));
    end;
  end
  else
  begin
    FLock.Enter;
    try
      FLigadoB := False;
    finally
      FLock.Leave;
    end;
    Log('Consumidor B desligando (pode levar ate ' + IntToStr(BLOCK_MS) +
      'ms para parar de vez)');
  end;
end;

procedure TfrmFilaTarefas.btnMatarAClick(Sender: TObject);
var
  LLigado: Boolean;
begin
  FLock.Enter;
  try
    LLigado := FLigadoA;
    if LLigado then
      FMatarA := True;
  finally
    FLock.Leave;
  end;
  if not LLigado then
    ShowMessage('Consumidor A nao esta ligado.')
  else
    Log('Consumidor A vai morrer na proxima tarefa que receber, sem confirmar');
end;

procedure TfrmFilaTarefas.btnMatarBClick(Sender: TObject);
var
  LLigado: Boolean;
begin
  FLock.Enter;
  try
    LLigado := FLigadoB;
    if LLigado then
      FMatarB := True;
  finally
    FLock.Leave;
  end;
  if not LLigado then
    ShowMessage('Consumidor B nao esta ligado.')
  else
    Log('Consumidor B vai morrer na proxima tarefa que receber, sem confirmar');
end;

procedure TfrmFilaTarefas.ConsumidorParou(const ANome: string; AMorreu: Boolean;
  const AIdAbandonado, ADescricao: string);
begin
  FLock.Enter;
  try
    if ANome = 'A' then
    begin
      FLigadoA := False;
      FRodandoA := False;
    end
    else
    begin
      FLigadoB := False;
      FRodandoB := False;
    end;
  finally
    FLock.Leave;
  end;

  if AMorreu then
  begin
    FUltimoAbandonadoId := AIdAbandonado;
    FUltimoAbandonadoConsumidor := ANome;
    Log('Consumidor ' + ANome + ' MORREU antes de confirmar ' + AIdAbandonado +
      ': ' + ADescricao + ' -- fica pendente no grupo');
  end
  else
    Log('Consumidor ' + ANome + ' parou');

  if ANome = 'A' then
  begin
    chkLigadoA.Checked := False;
    if AMorreu then
      lblStatusA.Caption := 'Status: MORTO (nao confirmou ' + AIdAbandonado + ')'
    else
      lblStatusA.Caption := 'Status: parado';
  end
  else
  begin
    chkLigadoB.Checked := False;
    if AMorreu then
      lblStatusB.Caption := 'Status: MORTO (nao confirmou ' + AIdAbandonado + ')'
    else
      lblStatusB.Caption := 'Status: parado';
  end;
end;

procedure TfrmFilaTarefas.btnReivindicarClick(Sender: TObject);
var
  LMinIdle: Int64;
begin
  LMinIdle := StrToIntDef(Trim(edtIdleMin.Text), 3000);
  if LMinIdle < 0 then
    LMinIdle := 0;
  RedisPool.Queue(TReivindicarWork.Create(Self, ChaveFila, LMinIdle));
end;

procedure TfrmFilaTarefas.btnApagarAbandonadaClick(Sender: TObject);
begin
  if FUltimoAbandonadoId = '' then
  begin
    ShowMessage('Nenhuma tarefa abandonada ainda. Ligue um consumidor, ' +
      'adicione uma tarefa e clique Matar antes dele confirmar.');
    Exit;
  end;
  RedisPool.Queue(TApagarAbandonadaWork.Create(Self, ChaveFila,
    FUltimoAbandonadoId));
  FUltimoAbandonadoId := '';
end;

procedure TfrmFilaTarefas.btnRetomarClick(Sender: TObject);
begin
  if FUltimoAbandonadoConsumidor = '' then
  begin
    ShowMessage('Nenhum consumidor morreu ainda. Ligue um, adicione uma ' +
      'tarefa e clique Matar antes dele confirmar.');
    Exit;
  end;
  RedisPool.Queue(TRetomarWork.Create(Self, ChaveFila,
    FUltimoAbandonadoConsumidor));
end;

procedure TfrmFilaTarefas.tmrAmostraTimer(Sender: TObject);
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
  RedisPool.Queue(TAmostraWork.Create(Self, ChaveFila));
end;

procedure TfrmFilaTarefas.MostraAmostra(ATamanho, APendCount: Int64;
  const APendMinId, APendMaxId: string);
begin
  FLock.Enter;
  try
    FAmostraEmVoo := False;
  finally
    FLock.Leave;
  end;

  if ATamanho < 0 then
    lblTamanho.Caption := 'Tamanho do stream: -'
  else
    lblTamanho.Caption := 'Tamanho do stream: ' + IntToStr(ATamanho);

  if ATamanho < 0 then
    lblPendencias.Caption := 'Pendencias no grupo: -'
  else if APendCount = 0 then
    lblPendencias.Caption := 'Pendencias no grupo: 0'
  else
    lblPendencias.Caption := 'Pendencias no grupo: ' + IntToStr(APendCount) +
      ' (' + APendMinId + ' .. ' + APendMaxId + ')';
end;

procedure TfrmFilaTarefas.btnLimparClick(Sender: TObject);
begin
  mmLog.Lines.Clear;
end;

end.
