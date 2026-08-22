program RedisUnitTestsFpc;

{ Runner FPCUnit dos testes unitarios (mesma cobertura do tests\Unit\*.pas
  DUnitX/Delphi, portada para FPCUnit). Nao precisa de servidor Redis: o codec
  e' testado sobre TRedisBytesSource, em memoria.

  Console (saida de texto), quando chamado com qualquer parametro:
    .\RedisUnitTestsFpc.exe --all --format=plain
  GUI (janela com arvore de testes + barra verde/vermelha), sem parametros:
    .\RedisUnitTestsFpc.exe
  Fora do Windows roda sempre em modo console (sem LCL/widgetset). }

{$mode delphi}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  {$IFDEF MSWINDOWS}
  Interfaces, Forms, GuiTestRunner,
  {$ENDIF}
  Classes, consoletestrunner, testregistry,
  Redis.TypesTests,
  Redis.RespTests,
  Redis.ConnectionTests;

var
  ConsoleApp: TTestRunner;
begin
  // Console FPC puro (fora de app Lazarus): DefaultSystemCodePage nao e' UTF-8
  // por padrao, entao os literais acentuados dos testes (RoundTrip_ComAcentos,
  // Comando_ArgumentoComAcentos etc.) seriam transcodificados errado e a suite
  // falharia por motivo que nao tem nada a ver com Redis. Ver CLAUDE.md.
  SetMultiByteConversionCodePage(CP_UTF8);

  {$IFDEF MSWINDOWS}
  if ParamCount = 0 then
  begin
    Application.Initialize;
    Application.CreateForm(TGUITestRunner, TestRunner);
    Application.Run;
  end
  else
  {$ENDIF}
  begin
    DefaultFormat := fPlain;
    DefaultRunAllTests := True;
    ConsoleApp := TTestRunner.Create(nil);
    try
      ConsoleApp.Initialize;
      ConsoleApp.Title := 'pascal-redis-faa - testes unitarios (FPCUnit)';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  end;
end.
