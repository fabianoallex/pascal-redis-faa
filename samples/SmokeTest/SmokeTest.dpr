program SmokeTest;

{ Smoke test da pascal-redis-faa contra um Redis real (docker/docker-compose.yml).

  Compila nos dois mundos a partir do MESMO fonte:
    FPC:    fpc -Fu..\..\src -Fi..\..\src SmokeTest.dpr
    Delphi: dcc32 -NSSystem;Winapi -U..\..\src -I..\..\src SmokeTest.dpr

  ESTADO (M0): a lib ainda so tem transporte + threading. Este smoke test
  monta os frames RESP na mao (ASCII, sem binary-safety) so para provar a
  cadeia: pacote compila, socket conecta, servidor responde. A partir do M2
  ele passa a usar TRedisConnection/Redis.Resp e cresce para SET/GET/DEL,
  pipeline, MULTI/EXEC, pub/sub e streams; o argumento --tls entra no M5.

  Sai com exit code 0 se tudo passou; 1 se algo falhou. }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ELSE}
  {$APPTYPE CONSOLE}
{$ENDIF}

uses
  {$IFDEF FPC}
    {$IFDEF UNIX}
  cthreads,
    {$ENDIF}
  {$ENDIF}
  SysUtils,
  Redis.Threading,
  Redis.Transport;

const
  HOST = 'localhost';
  PORT = 6379;

var
  GFalhas: Integer = 0;

procedure Passo(const ANome: string; AOk: Boolean; const ADetalhe: string = '');
begin
  if AOk then
    WriteLn('  [PASS] ', ANome, ADetalhe)
  else
  begin
    WriteLn('  [FAIL] ', ANome, ADetalhe);
    Inc(GFalhas);
  end;
end;

{ Acrescenta texto ASCII a um buffer de bytes. Provisorio: a partir do M1 o
  Redis.Resp codifica TBytes de verdade (bulk strings sao binario-seguras). }
procedure Acrescenta(var ABuf: TBytes; const AText: string);
var
  I, N: Integer;
begin
  N := Length(ABuf);
  SetLength(ABuf, N + Length(AText));
  for I := 1 to Length(AText) do
    ABuf[N + I - 1] := Byte(Ord(AText[I]));
end;

{ Comando no "unified request protocol": array de bulk strings. E' o unico
  formato que a lib vai emitir (comando inline nao e' binario-seguro). }
function CodificaComando(const AArgs: array of string): TBytes;
var
  I: Integer;
begin
  Result := nil;
  Acrescenta(Result, '*' + IntToStr(Length(AArgs)) + #13#10);
  for I := 0 to High(AArgs) do
  begin
    Acrescenta(Result, '$' + IntToStr(Length(AArgs[I])) + #13#10);
    Acrescenta(Result, AArgs[I] + #13#10);
  end;
end;

procedure Envia(ASock: TRedisTcpSocket; const AArgs: array of string);
var
  LBuf: TBytes;
  LEnviados: Integer;
begin
  LBuf := CodificaComando(AArgs);
  LEnviados := ASock.Send(LBuf[0], Length(LBuf));
  if LEnviados <> Length(LBuf) then
    raise ERedisTransport.CreateFmt('envio parcial: %d de %d bytes',
      [LEnviados, Length(LBuf)]);
end;

{ Le uma linha terminada em CRLF, byte a byte. Provisorio: o Redis.Resp do M1
  le em buffer (byte a byte custa uma syscall por caractere). }
function LeLinha(ASock: TRedisTcpSocket; out ALinha: string): Boolean;
var
  LByte, LAnterior: Byte;
begin
  ALinha := '';
  LAnterior := 0;
  repeat
    if ASock.Receive(LByte, 1) <= 0 then
      Exit(False);
    if (LByte = 10) and (LAnterior = 13) then
    begin
      SetLength(ALinha, Length(ALinha) - 1);  // descarta o CR
      Exit(True);
    end;
    ALinha := ALinha + Chr(LByte);
    LAnterior := LByte;
  until False;
end;

procedure Executa;
var
  LSock: TRedisTcpSocket;
  LLinha, LCorpo: string;
  LOk: Boolean;
begin
  WriteLn('pascal-redis-faa :: smoke test M0');
  WriteLn('  alvo ......... ', HOST, ':', PORT);
  WriteLn('  compilador ... ', {$IFDEF FPC} 'FPC ' + {$I %FPCVERSION%} {$ELSE} 'Delphi' {$ENDIF});
  WriteLn('  backend TLS .. ', RedisTlsBackendName, ' (nao exercitado ate o M5)');
  WriteLn;

  LSock := TRedisTcpSocket.Create;
  try
    LSock.Connect(HOST, PORT);
    Passo('conecta no servidor', True);

    { A leitura vem ANTES da chamada a Passo, e nao embutida nela. Os
      argumentos de uma chamada nao sao avaliados da esquerda para a direita:
      o ' -> ' + LLinha do ultimo parametro era montado antes de LeLinha
      preencher LLinha, e o detalhe saia sempre vazio — justo no caso de falha,
      em que ele e' a unica pista. }
    Envia(LSock, ['PING']);
    LOk := LeLinha(LSock, LLinha);
    Passo('PING responde +PONG', LOk and (LLinha = '+PONG'), ' -> ' + LLinha);

    Envia(LSock, ['ECHO', 'pascal-redis-faa']);
    LCorpo := '';
    LOk := LeLinha(LSock, LLinha) and (LLinha = '$16') and
           LeLinha(LSock, LCorpo);
    Passo('ECHO devolve bulk string', LOk and (LCorpo = 'pascal-redis-faa'),
      ' -> ' + LLinha + ' ' + LCorpo);

    LSock.Close;
    Passo('encerra a conexao', True);
  finally
    LSock.Free;
  end;
end;

begin
  {$IFNDEF FPC}
  ReportMemoryLeaksOnShutdown := True;
  {$ENDIF}
  try
    Executa;
  except
    on E: Exception do
    begin
      WriteLn('  [FAIL] excecao ', E.ClassName, ': ', E.Message);
      Inc(GFalhas);
    end;
  end;
  WriteLn;
  if GFalhas = 0 then
    WriteLn('RESULTADO: PASS')
  else
    WriteLn('RESULTADO: FAIL (', GFalhas, ' passo(s))');
  ExitCode := Ord(GFalhas <> 0);
end.
