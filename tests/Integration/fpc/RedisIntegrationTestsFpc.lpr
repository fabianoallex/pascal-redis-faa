program RedisIntegrationTestsFpc;

{ Runner FPCUnit dos testes de INTEGRACAO (mesma cobertura do
  tests\Integration\*.pas DUnitX/Delphi, portada para FPCUnit).

  PRECISA de um Redis em localhost:6379 — suba com docker/docker-compose.yml
  antes de rodar. Sem servidor, todos os testes falham por conexao recusada.

  Console (saida de texto), quando chamado com qualquer parametro:
    .\RedisIntegrationTestsFpc.exe --all --format=plain
  GUI (janela com arvore de testes + barra verde/vermelha), sem parametros:
    .\RedisIntegrationTestsFpc.exe
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
  Redis.IntegrationTests;

var
  ConsoleApp: TTestRunner;
begin
  // Console FPC puro (fora de app Lazarus): DefaultSystemCodePage nao e' UTF-8
  // por padrao, entao os literais acentuados dos testes seriam transcodificados
  // errado e a suite falharia por motivo alheio ao Redis — aqui o estrago seria
  // pior, porque o valor errado iria PARA o servidor. Ver CLAUDE.md.
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
      ConsoleApp.Title := 'pascal-redis-faa - testes de integracao (FPCUnit)';
      ConsoleApp.Run;
    finally
      ConsoleApp.Free;
    end;
  end;
end.
