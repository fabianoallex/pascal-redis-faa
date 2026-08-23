program CacheAsideVcl;

{ Cache-aside (GUI): consulta o cache primeiro, no miss vai a' fonte lenta e
  grava com TTL. Mostra a armadilha do SET sem KEEPTTL (que apaga o prazo) e a
  diferenca entre expirar o lote inteiro junto ou com jitter.

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild CacheAsideVcl.lpi
    Delphi: abrir CacheAsideVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uCacheAsideMain in 'uCacheAsideMain.pas' {frmCacheAside};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmCacheAside, frmCacheAside);
  Application.Run;
end.
