{ Diretivas de compilacao compartilhadas (dual Delphi / Free Pascal).

  Cada unit da lib inclui este arquivo logo apos o 'unit ...;'. No Delphi ele
  nao tem efeito; no FPC ativa o mode Delphi (strings longas, advanced records,
  generics estilo Delphi, type helpers).

  REDIS_WINDOWS: definido quando o alvo e' Windows, em qualquer compilador
  (Delphi define MSWINDOWS; FPC define WINDOWS). Usado para o transporte TLS
  via SChannel, que e' exclusivo de Windows.

  REDIS_OPENSSL: opt-in do usuario (-dREDIS_OPENSSL). Troca o backend TLS para
  OpenSSL em QUALQUER plataforma; tem precedencia sobre o SChannel se ambos
  estiverem definidos. Fora do Windows e' a UNICA forma de ter TLS. }

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
{$ENDIF}

{$IFDEF MSWINDOWS}
  {$DEFINE REDIS_WINDOWS}
{$ENDIF}
{$IFDEF WINDOWS}
  {$IFNDEF REDIS_WINDOWS}
    {$DEFINE REDIS_WINDOWS}
  {$ENDIF}
{$ENDIF}
