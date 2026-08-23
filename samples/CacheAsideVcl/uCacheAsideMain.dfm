object frmCacheAside: TfrmCacheAside
  Left = 0
  Top = 0
  Caption = 'Cache-aside com Redis  -  pascal-redis-faa'
  ClientHeight = 652
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
  object gbConsulta: TGroupBox
    Left = 8
    Top = 94
    Width = 764
    Height = 118
    Caption = ' Consulta com cache-aside '
    TabOrder = 1
    object lblCodigo: TLabel
      Left = 16
      Top = 27
      Width = 40
      Height = 15
      Caption = 'C'#243'digo'
    end
    object lblAtraso: TLabel
      Left = 556
      Top = 27
      Width = 92
      Height = 15
      Caption = 'Fonte lenta (ms)'
    end
    object lblResultado: TLabel
      Left = 16
      Top = 58
      Width = 68
      Height = 15
      Caption = 'Resultado: -'
    end
    object lblOrigem: TLabel
      Left = 16
      Top = 84
      Width = 55
      Height = 15
      Caption = 'Origem: -'
    end
    object lblPlacar: TLabel
      Left = 380
      Top = 84
      Width = 190
      Height = 15
      Caption = 'Hits: 0   Misses: 0   Taxa de acerto: -'
    end
    object edtCodigo: TEdit
      Left = 64
      Top = 23
      Width = 120
      Height = 23
      TabOrder = 0
      Text = '1001'
    end
    object btnConsultar: TButton
      Left = 196
      Top = 22
      Width = 150
      Height = 25
      Caption = 'Consultar'
      TabOrder = 1
      OnClick = btnConsultarClick
    end
    object btnConsultar5: TButton
      Left = 352
      Top = 22
      Width = 190
      Height = 25
      Caption = 'Consultar 5x ao mesmo tempo'
      TabOrder = 2
      OnClick = btnConsultar5Click
    end
    object edtAtraso: TEdit
      Left = 656
      Top = 23
      Width = 64
      Height = 23
      TabOrder = 3
      Text = '800'
    end
  end
  object gbCache: TGroupBox
    Left = 8
    Top = 218
    Width = 764
    Height = 120
    Caption = ' Ciclo de vida da chave '
    TabOrder = 2
    object lblTtl: TLabel
      Left = 16
      Top = 27
      Width = 40
      Height = 15
      Caption = 'TTL (s)'
    end
    object lblTtlRestante: TLabel
      Left = 310
      Top = 27
      Width = 84
      Height = 15
      Caption = 'TTL restante: -'
    end
    object lblArmadilha: TLabel
      Left = 16
      Top = 88
      Width = 660
      Height = 15
      Caption =
        'Um SET simples apaga o TTL: o cache vira vazamento permanente. Ve' +
        'ja o r'#243'tulo acima mudar para SEM PRAZO.'
    end
    object edtTtl: TEdit
      Left = 64
      Top = 23
      Width = 50
      Height = 23
      TabOrder = 0
      Text = '60'
    end
    object chkJitter: TCheckBox
      Left = 126
      Top = 25
      Width = 170
      Height = 17
      Caption = 'TTL com jitter (+/- 20%)'
      TabOrder = 1
    end
    object btnRegravarSem: TButton
      Left = 16
      Top = 54
      Width = 240
      Height = 25
      Caption = 'Regravar SEM KEEPTTL (armadilha)'
      TabOrder = 2
      OnClick = btnRegravarSemClick
    end
    object btnRegravarCom: TButton
      Left = 264
      Top = 54
      Width = 210
      Height = 25
      Caption = 'Regravar COM KEEPTTL'
      TabOrder = 3
      OnClick = btnRegravarComClick
    end
    object btnDel: TButton
      Left = 482
      Top = 54
      Width = 130
      Height = 25
      Caption = 'Invalidar (DEL)'
      TabOrder = 4
      OnClick = btnDelClick
    end
    object btnUnlink: TButton
      Left = 620
      Top = 54
      Width = 130
      Height = 25
      Caption = 'Invalidar (UNLINK)'
      TabOrder = 5
      OnClick = btnUnlinkClick
    end
  end
  object gbLote: TGroupBox
    Left = 8
    Top = 344
    Width = 764
    Height = 92
    Caption = ' Expira'#231#227'o em massa: TTL fixo x com jitter '
    TabOrder = 3
    object lblLoteTtl: TLabel
      Left = 16
      Top = 27
      Width = 84
      Height = 15
      Caption = 'TTL do lote (s)'
    end
    object lblSobreviventes: TLabel
      Left = 568
      Top = 27
      Width = 66
      Height = 15
      Caption = 'No cache: -'
    end
    object edtLoteTtl: TEdit
      Left = 110
      Top = 23
      Width = 50
      Height = 23
      TabOrder = 0
      Text = '15'
    end
    object btnLoteFixo: TButton
      Left = 172
      Top = 22
      Width = 190
      Height = 25
      Caption = 'Aquecer 20 chaves (TTL fixo)'
      TabOrder = 1
      OnClick = btnLoteFixoClick
    end
    object btnLoteJitter: TButton
      Left = 368
      Top = 22
      Width = 190
      Height = 25
      Caption = 'Aquecer 20 chaves (com jitter)'
      TabOrder = 2
      OnClick = btnLoteJitterClick
    end
    object pbSobreviventes: TProgressBar
      Left = 16
      Top = 56
      Width = 732
      Height = 18
      Max = 20
      Smooth = True
      TabOrder = 3
    end
  end
  object mmLog: TMemo
    Left = 8
    Top = 442
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
    TabOrder = 4
    WordWrap = False
  end
  object btnLimpar: TButton
    Left = 8
    Top = 618
    Width = 100
    Height = 25
    Anchors = [akLeft, akBottom]
    Caption = 'Limpar log'
    TabOrder = 5
    OnClick = btnLimparClick
  end
  object tmrAmostra: TTimer
    Enabled = False
    Interval = 500
    OnTimer = tmrAmostraTimer
    Left = 700
    Top = 600
  end
end
