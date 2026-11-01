{ Diretivas de compilacao compartilhadas (dual Delphi / Free Pascal).

  Cada unit da lib inclui este arquivo logo apos o 'unit ...;'. No Delphi ele
  nao tem efeito; no FPC ativa o mode Delphi (strings longas, advanced records,
  generics estilo Delphi, type helpers).

  REDIS_WINDOWS: definido quando o alvo e' Windows, em qualquer compilador
  (Delphi define MSWINDOWS; FPC define WINDOWS).

  REDIS_OPENSSL: opt-in do usuario (-dREDIS_OPENSSL). Desde a 0.4.0 so' muda
  o backend TLS PADRAO para OpenSSL (TRedisParams.TlsBackend = tbDefault). Os
  dois backends vem da pascal-common-faa e sao escolhidos em runtime: no
  Windows os dois estao em todo build, e fora dele o OpenSSL e' o unico, com
  ou sem a diretiva. Ver RedisDefaultTlsBackend na Redis.Transport. }

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
