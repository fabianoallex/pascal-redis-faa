program Redis.IntegrationSuite;

{ Runner DUnitX dos testes de INTEGRACAO.

  PRECISA de um Redis em localhost:6379 — suba com docker/docker-compose.yml
  antes de rodar. Sem servidor, todos os testes falham por conexao recusada.

  A suite irma em FPCUnit fica em tests\Integration\fpc — as duas tem a MESMA
  cobertura e o corpo dos testes e' identico (e' para isso que existe o
  Redis.DUnitXCompat). Toda mudanca de um lado vai para o outro na mesma
  sessao.

  O nome do programa nao e' Redis.IntegrationTests porque ja' existe a UNIT com
  esse nome, e Delphi nao aceita projeto e unit homonimos no mesmo escopo. }

{$APPTYPE CONSOLE}
{$STRONGLINKTYPES ON}

uses
  System.SysUtils,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  DUnitX.TestFramework,
  Redis.Types in '..\..\src\Redis.Types.pas',
  Redis.Resp in '..\..\src\Redis.Resp.pas',
  Redis.Threading in '..\..\src\Redis.Threading.pas',
  Redis.Transport in '..\..\src\Redis.Transport.pas',
  Redis.Connection in '..\..\src\Redis.Connection.pas',
  Redis.Pool in '..\..\src\Redis.Pool.pas',
  Redis.Commands in '..\..\src\Redis.Commands.pas',
  Redis.Commands.Keys in '..\..\src\Redis.Commands.Keys.pas',
  Redis.Commands.Strings in '..\..\src\Redis.Commands.Strings.pas',
  Redis.Commands.Hashes in '..\..\src\Redis.Commands.Hashes.pas',
  Redis.Commands.Lists in '..\..\src\Redis.Commands.Lists.pas',
  Redis.Commands.Sets in '..\..\src\Redis.Commands.Sets.pas',
  Redis.Commands.ZSets in '..\..\src\Redis.Commands.ZSets.pas',
  Redis.Client in '..\..\src\Redis.Client.pas',
  Redis.DUnitXCompat in '..\Unit\Redis.DUnitXCompat.pas',
  Redis.IntegrationTests in 'Redis.IntegrationTests.pas';

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
  nunitLogger: ITestLogger;
begin
  // Conexoes e sockets de verdade indo e voltando do pool: um vazamento aqui
  // e' conexao que ninguem fechou, e o servidor sente isso antes do cliente.
  ReportMemoryLeaksOnShutdown := True;
  try
    TDUnitX.CheckCommandLine;

    if TDUnitX.Options.Include = '' then
      TDUnitX.Options.Include := '.';

    runner := TDUnitX.CreateRunner;
    runner.UseRTTI := True;
    runner.FailsOnNoAsserts := False;

    if TDUnitX.Options.ConsoleMode <> TDunitXConsoleMode.Off then
    begin
      logger := TDUnitXConsoleLogger.Create(
        TDUnitX.Options.ConsoleMode = TDunitXConsoleMode.Quiet);
      runner.AddLogger(logger);
    end;

    nunitLogger := TDUnitXXMLNUnitFileLogger.Create(TDUnitX.Options.XMLOutputFile);
    runner.AddLogger(nunitLogger);

    results := runner.Execute;

    if not results.AllPassed then
      System.ExitCode := EXIT_ERRORS;

    if (TDUnitX.Options.ExitBehavior = TDUnitXExitBehavior.Pause) and IsConsole then
    begin
      System.Write('Done.. press <Enter> key to quit.');
      System.Readln;
    end;
  except
    on E: Exception do
      System.Writeln(E.ClassName, ': ', E.Message);
  end;
end.
