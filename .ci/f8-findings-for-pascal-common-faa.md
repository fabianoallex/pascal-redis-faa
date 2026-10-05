# F8 (pascal-redis-faa) — findings for pascal-common-faa

Migration of pascal-redis-faa to pascal-common-faa v1.1.0, done 2026-10-04 — the last library
of F8. Nothing in pascal-common-faa was changed; these are the points to bring back. Most
important first.

## 1. GUI apps: forms are freed BEFORE any unit finalization, so `PcPool` runs leftovers after them

**What `migrating.md` says.** "Your unit is finalized before `PascalCommon.ThreadPool`, so
`PcPool` is still alive and still running your items when your finalization frees things."
True, and it frames the hazard as "your finalization". In a GUI application there is an earlier
point that the guide doesn't mention: the forms (and anything else owned by `Application`) are
freed before **any** unit finalization runs.

**Measured** (FPC 3.2.2 / LCL, Windows x64; a probe with the same `uses` shape as redis's GUI
samples: `Interfaces, Forms` in the program, then a form unit that uses `Forms` before
`PascalCommon.ThreadPool`; the form is created with `Application` as owner, queues 3 items that
sleep 300/600/900 ms and then log whether the form is alive; the main block ends right away):

```
[main  ] programa principal: fim (as finalizacoes comecam)
[main  ] TProbeForm.Destroy (a form sai aqui)
[main  ] uProbeForm: finalizacao
[worker] item 1: form viva = False, Application <> nil = True
[worker] item 2: form viva = False, Application <> nil = True
[worker] item 3: form viva = False, Application <> nil = True
```

The form is destroyed before the first unit finalization (even before its own unit's), and all
three items run afterwards, against a freed form.

**Cause, in the UI libraries' source.** LCL: `TApplication.Create` calls
`AddExitProc(@BeforeFinalization)` (`lcl/include/application.inc`), which frees the
application's components. VCL (Delphi 12 source): `Vcl.Forms` calls `AddExitProc(DoneApplication)`,
and `DoneApplication` calls `Application.DestroyComponents`. Exit procs run before unit
finalization. The VCL side was read in the source, not run.

**Not a regression.** The donors' pools were also freed in a unit finalization (`RedisPool` in
`Redis.Threading`'s), so they ran their leftovers after the forms too. redis's GUI samples had
a use-after-free at shutdown because of it: their `FormCloseQuery` waited only for the
operations between `UsarCliente`/`SoltarCliente`. Measured in `FilaTarefasVcl` (FPC, an
instrumented scratch copy, a stream consumer sleeping 4 s in "processing" when the window is
closed): `FormDestroy` at 39.489, the consumer waking at 42.137 and calling into the freed
form. Silent: exit code 0, 0 unfreed blocks (heaptrc, even with `keepreleased`, doesn't flag a
use after free).

**Fixed in redis with the pipes pattern** (`docs/DECISOES.md` §50): every work item derives
from a base that counts it on the form from its constructor (UI thread, before `Queue`) to its
destructor; `FormCloseQuery` waits for the counter to reach 0. Two things beyond the pipes
pattern were needed in a GUI:

- **Pump `TThread.Queue` while waiting** (`CheckSynchronize(10)` instead of `Sleep(10)`). The
  items post marshals as they finish; left for after the message loop, nobody runs them, and on
  Delphi they leak — `System.Classes`' `DoneThreadSynchronization` doesn't free what is left in
  the queue (read in the Delphi 12 source). So the wait belongs in `OnCloseQuery`, while the form
  is whole, not in `FormDestroy`.
- **A work item must not read a control.** On LCL, reading `TEdit.Text` from a worker is a
  cross-thread `SendMessage` to the UI thread — which is the thread waiting for that item.

After the fix, the same script: consumer waking at 48.695, `FormDestroy` at 48.705.

**Suggestion** for `migrating.md` ("Behavior to know about") and the `PascalCommon.ThreadPool`
header: in a VCL/LCL application, a form or data module that queues work on `PcPool` must wait
for its own items in `OnCloseQuery`, pumping `CheckSynchronize`, because `Application` frees it
in an exit proc, before every unit finalization — so "wait in your finalization" is already too
late for it.

## 2. `migrating.md` step 7: say "check that heaptrc is actually on"

pascal-redis-faa's FPC test `.lpi` never had `UseHeaptrc`: every "0 leaks" in its history was
DUnitX's `Tests Leaked: 0`; FPC leaks were never measured. Turning it on during the migration
gave 0 unfreed blocks on Windows and Linux (so nothing was hiding), but nothing in the steps
would have caught a missing heaptrc. Together with gotcha 5 (silent heaptrc on Debian), a
suggestion for step 7: "0 leaks means the `0 unfreed memory blocks` line was seen — in the
heaptrc **file** (`HEAPTRC=log=`) — not that no leak line appeared."

## 3. lazbuild drops a `packagefiles.xml` in the current directory on a broken dependency

Building `pascal_redis_faa.lpk` alone, with `pascal_common_faa` not registered, fails with
`Broken dependency: pascal_redis_faa 0.1->pascal_common_faa (>=1.0)` (correct: that is the
by-name rule) **and** writes a copy of the user's Lazarus `packagefiles.xml` into the current
directory. Reproduced in an empty directory; building a `.lpi` that resolves everything doesn't
write it. pipes and amqp already list `packagefiles.xml` in `.gitignore`; redis now does too.
Since every consumer `.lpk` now has a dependency that is unresolved until the application
registers it, this will happen to every library author who runs `lazbuild <lib>.lpk` from the
repository root. Worth one line in `migrating.md` step 3 (or a gotcha): register
`pascal_common_faa.lpk` before building the library's `.lpk` alone, and ignore
`/packagefiles.xml`.

## 4. Linux integration suites against a server: one server per run

For `CPUS=1` with several runners at once, redis's integration suite can't share one server
(it uses `CLIENT KILL` and counts subscribers with `PUBSUB NUMSUB`). `tools/test_fpc_docker.sh`
starts a `redis:7.2-alpine` per run and runs the FPC container with
`--network container:<that redis>`, so the suite's hardcoded `localhost:6379` is that server and
nothing in the tests changed. 8 runners × 5 runs of each suite at `--cpus=1`: all green. Not a
change for pascal-common-faa; possibly a line for the `dual-compiler-delphi-lazarus` skill, for
libraries whose suites need a server.

## 5. Things that worked as documented (no action)

- **No unit left behind.** Everything in `Redis.Threading` had moved (it was the plain copy), so
  the unit was deleted instead of kept as a shell; the version check went to `Redis.Types`, the
  base unit every protocol unit uses (including `Redis.Pool` and `Redis.PubSub`, the only
  consumers of pascal-common-faa), right after the `uses` that brings `PascalCommon.Version`.
  Raising the minimum to 10200 stops FPC 3.2.2 with
  `Redis.Types.pas(34,4) Fatal: User defined: pascal-redis-faa precisa da pascal-common-faa 1.0.0 ou mais nova`.
- `.lpk` requiring `pascal_common_faa` by name with `MinVersion Major="1"`; 2 test and 4 sample
  `.lpi` listing it first with `DefaultFilename` into `external/` and `Prefer="True"`: lazbuild
  built all six against the submodule (`-Fu...\external\pascal-common-faa\packages\lib\x86_64-win64`
  in the build logs).
- **Whole-word rename** with `perl -pi` (`\b`): 134 lines changed, LF kept. `TRedisPool` (the
  connection pool), `TRedisPoolParams` and `TRedisPoolWorkerThread` (an integration-test type)
  kept their counts (60, 29, 12) before and after.
- **Monitor contract.** `Wait`/`PulseAll` bodies compared with names normalized: identical to
  redis's. The three `Wait` call sites (two in `Redis.PubSub`, one in `Redis.Pool`) already loop
  on their condition under the lock with a deadline.
- **64-bit atomics:** not used by redis outside the deleted unit, so the overload question
  didn't arise.
- **`Destroy` runs the queue:** no redis document claimed otherwise, so nothing to correct.
- **Results:** unit 436/436 and integration 69/69, 0 unfreed blocks, on FPC Windows x64 (both
  TLS backends, SChannel and OpenSSL) and FPC Linux x86_64; 8 parallel Linux runners at
  `--cpus=1`, 40 runs of each suite, all green; SmokeTest 151 plain and 160 `--tls` (SChannel
  and OpenSSL).

## 6. Delphi confirmation

Delphi 12 CE, built in the IDE (Debug) and run from the command line, 2026-10-04: Win32 and
Win64, unit 436/436 and integration 69/69, `Tests Leaked: 0`, no failures or errors; SmokeTest
151 plain and 160 `--tls` (SChannel) on both. "Build all" of `Redis.groupproj` ok on Win32. The
three GUI samples (Win32; their projects have no Win64 platform) open, connect and close with
exit code 0 and no `ReportMemoryLeaksOnShutdown` box, including the close with a consumer asleep
in processing.
