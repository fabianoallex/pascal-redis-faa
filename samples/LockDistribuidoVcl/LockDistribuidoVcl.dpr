program LockDistribuidoVcl;

{ Lock distribuido (GUI): SET NX PX adquire, um script Lua compare-and-delete
  libera com seguranca, e a armadilha do DEL direto mostra por que o token
  importa. Mostra tambem a renovacao de posse via PEXPIRE em Lua.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild LockDistribuidoVcl.lpi
    Delphi: abrir LockDistribuidoVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uLockDistribuidoMain in 'uLockDistribuidoMain.pas' {frmLockDistribuido};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmLockDistribuido, frmLockDistribuido);
  Application.Run;
end.
