program FilaTarefasVcl;

{ Fila de tarefas (GUI): stream + consumer group, XADD produz, XREADGROUP
  BLOCK consome, XACK confirma. Mostra a pendencia sobrevivendo a um worker
  que morre sem confirmar, e a reivindicacao por XAUTOCLAIM -- inclusive o
  caso da entrada apagada do stream (IsDeleted).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    lazbuild FilaTarefasVcl.lpi
    Delphi: abrir FilaTarefasVcl.dproj no IDE }

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads, // threads reais no Unix: sem isso os eventos/condvars da lib falham em runtime
    {$ENDIF}
  Interfaces,
  {$ENDIF}
  Forms,
  uFilaTarefasMain in 'uFilaTarefasMain.pas' {frmFilaTarefas};

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmFilaTarefas, frmFilaTarefas);
  Application.Run;
end.
