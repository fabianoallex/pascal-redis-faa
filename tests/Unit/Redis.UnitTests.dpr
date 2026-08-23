program Redis.UnitTests;

{ Runner DUnitX dos testes unitarios. Nao precisa de servidor Redis: o codec e'
  exercitado sobre TRedisBytesSource, em memoria.

  A suite irma em FPCUnit fica em tests\Unit\fpc — as duas tem a MESMA cobertura
  e o corpo dos testes e' identico (o Redis.DUnitXCompat existe justamente para
  isso). Toda mudanca de um lado vai para o outro na mesma sessao. }

{$APPTYPE CONSOLE}
{$STRONGLINKTYPES ON}

uses
  System.SysUtils,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  DUnitX.TestFramework,
  Redis.Types in '..\..\src\Redis.Types.pas',
  Redis.Resp in '..\..\src\Redis.Resp.pas',
  Redis.Transport in '..\..\src\Redis.Transport.pas',
  Redis.Transport.Tls in '..\..\src\Redis.Transport.Tls.pas',
  Redis.Connection in '..\..\src\Redis.Connection.pas',
  Redis.Pool in '..\..\src\Redis.Pool.pas',
  Redis.Commands in '..\..\src\Redis.Commands.pas',
  Redis.Commands.Keys in '..\..\src\Redis.Commands.Keys.pas',
  Redis.Commands.Strings in '..\..\src\Redis.Commands.Strings.pas',
  Redis.Commands.Hashes in '..\..\src\Redis.Commands.Hashes.pas',
  Redis.Commands.Lists in '..\..\src\Redis.Commands.Lists.pas',
  Redis.Commands.Sets in '..\..\src\Redis.Commands.Sets.pas',
  Redis.Commands.ZSets in '..\..\src\Redis.Commands.ZSets.pas',
  Redis.Commands.Scripting in '..\..\src\Redis.Commands.Scripting.pas',
  Redis.Transaction in '..\..\src\Redis.Transaction.pas',
  Redis.Commands.PubSub in '..\..\src\Redis.Commands.PubSub.pas',
  Redis.PubSub in '..\..\src\Redis.PubSub.pas',
  Redis.Client in '..\..\src\Redis.Client.pas',
  Redis.DUnitXCompat in 'Redis.DUnitXCompat.pas',
  Redis.TypesTests in 'Redis.TypesTests.pas',
  Redis.RespTests in 'Redis.RespTests.pas',
  Redis.ConnectionTests in 'Redis.ConnectionTests.pas',
  Redis.PoolTests in 'Redis.PoolTests.pas',
  Redis.CommandsTests in 'Redis.CommandsTests.pas',
  Redis.TransactionTests in 'Redis.TransactionTests.pas',
  Redis.PubSubTests in 'Redis.PubSubTests.pas';

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
  nunitLogger: ITestLogger;
begin
  // A arvore de respostas e' toda por interface, entao um vazamento aqui
  // significaria ciclo de referencia — coisa que o M1 nao pode deixar passar
  // para o pool do M3, onde conexoes vao e voltam o tempo todo.
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
