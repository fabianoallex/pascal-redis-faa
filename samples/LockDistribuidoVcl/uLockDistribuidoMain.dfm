object frmLockDistribuido: TfrmLockDistribuido
  Left = 0
  Top = 0
  Caption = 'Lock distribu'#237'do (inst'#226'ncia '#250'nica) com Redis  -  pascal-redis-faa'
  ClientHeight = 750
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
  object gbLock: TGroupBox
    Left = 8
    Top = 94
    Width = 764
    Height = 140
    Caption = ' Aquisicao do lock (SET NX PX) '
    TabOrder = 1
    object lblAviso: TLabel
      Left = 16
      Top = 22
      Width = 730
      Height = 15
      Caption =
        'AVISO: lock de INSTANCIA UNICA (um Redis so) -- NAO e Redlock.' +
        ' Dono que cai antes do TTL perde a garantia.'
    end
    object lblRecurso: TLabel
      Left = 16
      Top = 52
      Width = 46
      Height = 15
      Caption = 'Recurso'
    end
    object lblTtl: TLabel
      Left = 228
      Top = 52
      Width = 47
      Height = 15
      Caption = 'TTL (ms)'
    end
    object lblMeuToken: TLabel
      Left = 16
      Top = 84
      Width = 68
      Height = 15
      Caption = 'Meu token: -'
    end
    object lblDono: TLabel
      Left = 16
      Top = 108
      Width = 138
      Height = 15
      Caption = 'Dono atual no servidor: -'
    end
    object lblTtlRestante: TLabel
      Left = 410
      Top = 108
      Width = 84
      Height = 15
      Caption = 'TTL restante: -'
    end
    object edtRecurso: TEdit
      Left = 72
      Top = 48
      Width = 140
      Height = 23
      TabOrder = 0
      Text = 'pedido:42'
    end
    object edtTtl: TEdit
      Left = 288
      Top = 48
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '4000'
    end
    object chkComToken: TCheckBox
      Left = 364
      Top = 50
      Width = 230
      Height = 17
      Caption = 'Gerar token de posse (recomendado)'
      Checked = True
      State = cbChecked
      TabOrder = 2
      OnClick = chkComTokenClick
    end
    object btnAdquirir: TButton
      Left = 606
      Top = 47
      Width = 150
      Height = 25
      Caption = 'Adquirir (SET NX PX)'
      TabOrder = 3
      OnClick = btnAdquirirClick
    end
  end
  object gbConcorrente: TGroupBox
    Left = 8
    Top = 240
    Width = 764
    Height = 90
    Caption = ' Concorrente simulado (outro processo) '
    TabOrder = 2
    object lblConcorrenteToken: TLabel
      Left = 330
      Top = 27
      Width = 133
      Height = 15
      Caption = 'Token do concorrente: -'
    end
    object lblConcorrenteInfo: TLabel
      Left = 16
      Top = 55
      Width = 730
      Height = 15
      Caption =
        'Ao ficar livre, o concorrente tenta adquirir a cada 1s, com o ' +
        'proprio token.'
    end
    object chkConcorrente: TCheckBox
      Left = 16
      Top = 25
      Width = 300
      Height = 17
      Caption = 'Simular concorrente tentando o lock'
      TabOrder = 0
      OnClick = chkConcorrenteClick
    end
  end
  object gbLiberacao: TGroupBox
    Left = 8
    Top = 336
    Width = 764
    Height = 100
    Caption = ' Liberacao e a armadilha '
    TabOrder = 3
    object lblArmadilha: TLabel
      Left = 16
      Top = 62
      Width = 730
      Height = 15
      Caption =
        'DEL apaga o que estiver na chave AGORA, seja seu ou de outro d' +
        'ono. Deixe expirar, ligue o concorrente, e teste aqui.'
    end
    object btnLiberarSeguro: TButton
      Left = 16
      Top = 25
      Width = 280
      Height = 25
      Caption = 'Liberar com script (compare-and-delete)'
      TabOrder = 0
      OnClick = btnLiberarSeguroClick
    end
    object btnLiberarArmadilha: TButton
      Left = 310
      Top = 25
      Width = 260
      Height = 25
      Caption = 'Liberar com DEL direto (ARMADILHA)'
      TabOrder = 1
      OnClick = btnLiberarArmadilhaClick
    end
  end
  object gbRenovacao: TGroupBox
    Left = 8
    Top = 442
    Width = 764
    Height = 90
    Caption = ' Renovacao de posse (PEXPIRE via Lua) '
    TabOrder = 4
    object lblRenovacaoInfo: TLabel
      Left = 16
      Top = 55
      Width = 730
      Height = 15
      Caption =
        'A cada 1s roda um script que so estende o TTL se o token no se' +
        'rvidor ainda for o meu.'
    end
    object chkRenovarAuto: TCheckBox
      Left = 16
      Top = 25
      Width = 360
      Height = 17
      Caption = 'Renovar automaticamente enquanto eu for o dono'
      TabOrder = 0
      OnClick = chkRenovarAutoClick
    end
  end
  object mmLog: TMemo
    Left = 8
    Top = 538
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
    TabOrder = 5
    WordWrap = False
  end
  object btnLimpar: TButton
    Left = 8
    Top = 716
    Width = 100
    Height = 25
    Anchors = [akLeft, akBottom]
    Caption = 'Limpar log'
    TabOrder = 6
    OnClick = btnLimparClick
  end
  object tmrAmostra: TTimer
    Enabled = False
    Interval = 500
    OnTimer = tmrAmostraTimer
    Left = 620
    Top = 680
  end
  object tmrConcorrente: TTimer
    Enabled = False
    Interval = 1000
    OnTimer = tmrConcorrenteTimer
    Left = 660
    Top = 680
  end
  object tmrRenovar: TTimer
    Enabled = False
    Interval = 1000
    OnTimer = tmrRenovarTimer
    Left = 700
    Top = 680
  end
end
