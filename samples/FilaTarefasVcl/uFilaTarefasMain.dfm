object frmFilaTarefas: TfrmFilaTarefas
  Left = 0
  Top = 0
  Caption = 'Fila de tarefas com Streams  -  pascal-redis-faa'
  ClientHeight = 830
  ClientWidth = 780
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  Position = poScreenCenter
  OnCloseQuery = FormCloseQuery
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  TextHeight = 15
  object gbConexao: TGroupBox
    Left = 8
    Top = 8
    Width = 764
    Height = 80
    Caption = ' Conex'#227'o '
    TabOrder = 0
    object lblHost: TLabel
      Left = 16
      Top = 25
      Width = 26
      Height = 15
      Caption = 'Host'
    end
    object lblPorta: TLabel
      Left = 200
      Top = 25
      Width = 31
      Height = 15
      Caption = 'Porta'
    end
    object lblSenha: TLabel
      Left = 306
      Top = 25
      Width = 36
      Height = 15
      Caption = 'Senha'
    end
    object lblDb: TLabel
      Left = 468
      Top = 25
      Width = 17
      Height = 15
      Caption = 'DB'
    end
    object lblStatus: TLabel
      Left = 16
      Top = 54
      Width = 70
      Height = 15
      Caption = 'Desconectado.'
    end
    object edtHost: TEdit
      Left = 50
      Top = 21
      Width = 140
      Height = 23
      TabOrder = 0
      Text = 'localhost'
    end
    object edtPorta: TEdit
      Left = 237
      Top = 21
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '6379'
    end
    object edtSenha: TEdit
      Left = 348
      Top = 21
      Width = 110
      Height = 23
      PasswordChar = '*'
      TabOrder = 2
    end
    object edtDb: TEdit
      Left = 491
      Top = 21
      Width = 45
      Height = 23
      TabOrder = 3
      Text = '0'
    end
    object chkTls: TCheckBox
      Left = 548
      Top = 23
      Width = 55
      Height = 17
      Caption = 'TLS'
      TabOrder = 4
      OnClick = chkTlsClick
    end
    object btnConectar: TButton
      Left = 610
      Top = 19
      Width = 72
      Height = 25
      Caption = 'Conectar'
      TabOrder = 5
      OnClick = btnConectarClick
    end
    object btnDesconectar: TButton
      Left = 688
      Top = 19
      Width = 72
      Height = 25
      Caption = 'Desconectar'
      TabOrder = 6
      OnClick = btnDesconectarClick
    end
  end
  object gbFila: TGroupBox
    Left = 8
    Top = 94
    Width = 764
    Height = 110
    Caption = ' Fila (stream + grupo consumidor) '
    TabOrder = 1
    object lblFila: TLabel
      Left = 16
      Top = 25
      Width = 22
      Height = 15
      Caption = 'Fila'
    end
    object lblFilaInfo: TLabel
      Left = 16
      Top = 54
      Width = 730
      Height = 15
      Caption =
        'Crie a fila antes de ligar os consumidores -- senao XREADGROUP' +
        ' falha com NOGROUP.'
    end
    object lblTamanho: TLabel
      Left = 16
      Top = 80
      Width = 116
      Height = 15
      Caption = 'Tamanho do stream: -'
    end
    object lblPendencias: TLabel
      Left = 230
      Top = 80
      Width = 128
      Height = 15
      Caption = 'Pendencias no grupo: -'
    end
    object edtFila: TEdit
      Left = 64
      Top = 21
      Width = 160
      Height = 23
      TabOrder = 0
      Text = 'pedidos'
    end
    object btnCriarFila: TButton
      Left = 240
      Top = 19
      Width = 190
      Height = 25
      Caption = 'Criar fila (XGROUP CREATE)'
      TabOrder = 1
      OnClick = btnCriarFilaClick
    end
  end
  object gbProducao: TGroupBox
    Left = 8
    Top = 210
    Width = 764
    Height = 70
    Caption = ' Producao de tarefas (XADD) '
    TabOrder = 2
    object lblPayload: TLabel
      Left = 16
      Top = 25
      Width = 55
      Height = 15
      Caption = 'Descricao'
    end
    object edtPayload: TEdit
      Left = 112
      Top = 21
      Width = 160
      Height = 23
      TabOrder = 0
      Text = 'pedido'
    end
    object btnAdicionar: TButton
      Left = 284
      Top = 19
      Width = 140
      Height = 25
      Caption = 'Adicionar tarefa'
      TabOrder = 1
      OnClick = btnAdicionarClick
    end
    object btnAdicionar5: TButton
      Left = 434
      Top = 19
      Width = 170
      Height = 25
      Caption = 'Adicionar 5 tarefas'
      TabOrder = 2
      OnClick = btnAdicionar5Click
    end
  end
  object gbConsumidorA: TGroupBox
    Left = 8
    Top = 286
    Width = 764
    Height = 90
    Caption = ' Consumidor A (XREADGROUP BLOCK) '
    TabOrder = 3
    object lblAtrasoA: TLabel
      Left = 120
      Top = 27
      Width = 158
      Height = 15
      Caption = 'Tempo de processamento (ms)'
    end
    object lblStatusA: TLabel
      Left = 16
      Top = 57
      Width = 78
      Height = 15
      Caption = 'Status: parado'
    end
    object chkLigadoA: TCheckBox
      Left = 16
      Top = 25
      Width = 90
      Height = 17
      Caption = 'Ligado'
      TabOrder = 0
      OnClick = chkLigadoAClick
    end
    object edtAtrasoA: TEdit
      Left = 284
      Top = 23
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '1000'
    end
    object btnMatarA: TButton
      Left = 360
      Top = 22
      Width = 280
      Height = 25
      Caption = 'Matar (nao confirma a proxima tarefa)'
      TabOrder = 2
      OnClick = btnMatarAClick
    end
  end
  object gbConsumidorB: TGroupBox
    Left = 8
    Top = 382
    Width = 764
    Height = 90
    Caption = ' Consumidor B (XREADGROUP BLOCK) '
    TabOrder = 4
    object lblAtrasoB: TLabel
      Left = 120
      Top = 27
      Width = 158
      Height = 15
      Caption = 'Tempo de processamento (ms)'
    end
    object lblStatusB: TLabel
      Left = 16
      Top = 57
      Width = 78
      Height = 15
      Caption = 'Status: parado'
    end
    object chkLigadoB: TCheckBox
      Left = 16
      Top = 25
      Width = 90
      Height = 17
      Caption = 'Ligado'
      TabOrder = 0
      OnClick = chkLigadoBClick
    end
    object edtAtrasoB: TEdit
      Left = 284
      Top = 23
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '1000'
    end
    object btnMatarB: TButton
      Left = 360
      Top = 22
      Width = 280
      Height = 25
      Caption = 'Matar (nao confirma a proxima tarefa)'
      TabOrder = 2
      OnClick = btnMatarBClick
    end
  end
  object gbRecuperacao: TGroupBox
    Left = 8
    Top = 478
    Width = 764
    Height = 130
    Caption = ' Recuperacao de pendencias (armadilhas) '
    TabOrder = 5
    object lblIdleMin: TLabel
      Left = 16
      Top = 25
      Width = 148
      Height = 15
      Caption = 'Ocioso ha pelo menos (ms)'
    end
    object lblArmadilha: TLabel
      Left = 16
      Top = 88
      Width = 730
      Height = 15
      Caption =
        'XAUTOCLAIM (Redis 7+) purga sozinho a pendencia ja apagada do s' +
        'tream; para ver IsDeleted de verdade, use Retomar (mesmo consu' +
        'midor rele a propria PEL).'
    end
    object edtIdleMin: TEdit
      Left = 190
      Top = 21
      Width = 70
      Height = 23
      TabOrder = 0
      Text = '3000'
    end
    object btnReivindicar: TButton
      Left = 280
      Top = 19
      Width = 230
      Height = 25
      Caption = 'Reivindicar pendencias (XAUTOCLAIM)'
      TabOrder = 1
      OnClick = btnReivindicarClick
    end
    object btnRetomar: TButton
      Left = 16
      Top = 52
      Width = 280
      Height = 25
      Caption = 'Retomar minha PEL (mesmo consumidor)'
      TabOrder = 2
      OnClick = btnRetomarClick
    end
    object btnApagarAbandonada: TButton
      Left = 310
      Top = 52
      Width = 280
      Height = 25
      Caption = 'Apagar ultima abandonada (XDEL)'
      TabOrder = 3
      OnClick = btnApagarAbandonadaClick
    end
  end
  object mmLog: TMemo
    Left = 8
    Top = 614
    Width = 764
    Height = 170
    Anchors = [akLeft, akTop, akRight, akBottom]
    Font.Charset = DEFAULT_CHARSET
    Font.Color = clWindowText
    Font.Height = -12
    Font.Name = 'Consolas'
    Font.Style = []
    ParentFont = False
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 6
    WordWrap = False
  end
  object btnLimpar: TButton
    Left = 8
    Top = 792
    Width = 100
    Height = 25
    Anchors = [akLeft, akBottom]
    Caption = 'Limpar log'
    TabOrder = 7
    OnClick = btnLimparClick
  end
  object tmrAmostra: TTimer
    Enabled = False
    Interval = 700
    OnTimer = tmrAmostraTimer
    Left = 700
    Top = 790
  end
end
