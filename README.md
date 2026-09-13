# otp_rails_beam

An Elixir sidecar supervisor for Rails (or arbitrary) OS processes — the
`beam` repo from the [otp-rails](https://github.com/shishi-odoshi) project
(DESIGN §2). It supervises external commands through native OTP supervision
(each child is a GenServer owning a Port), consumes the DESIGN §5 health
protocol over a Unix socket, and emits the DESIGN §6 telemetry events.

It depends only on the §5/§6 protocols — never on the Ruby gem's code. This
is Phase 4 step 1 (ports-based supervision); the shared job queue and Phoenix
channels come later.

## Usage

```elixir
{:ok, sup} =
  OtpRailsBeam.start_link(
    socket_path: "tmp/otp-rails.sock",
    strategy: :one_for_one,          # or :rest_for_one
    max_restarts: 5,                 # native OTP restart intensity
    max_seconds: 60,
    children: [
      %{id: "web",  cmd: ["bin/rails", "server"], shutdown_ms: 30_000},
      %{id: "jobs", cmd: ["bin/jobs"], health_interval_ms: 5_000}
    ]
  )
```

Child spec fields: `id` (string), `cmd` (argv list), `restart`
(`:permanent` default | `:transient` | `:temporary`), `shutdown_ms`
(SIGTERM → wait → SIGKILL drain budget, default 30s), `health_interval_ms`
(heartbeat aging tick, default 5s). Strategies and restart intensity are
plain OTP — exceeding `max_restarts`/`max_seconds` collapses the tree
(exit reason `:shutdown`), leaving the platform as the final supervisor.

Children inherit `OTP_RAILS_SOCK` and `OTP_RAILS_TOKEN` in their environment.
A child that heartbeats is judged by its heartbeats; one that never does is
judged by OS process aliveness (the Port reports its death immediately).

## Contract

The wire protocol is frozen (otp-rails DESIGN §5/§9, hard rule 4): one JSON
object per line over a Unix socket, mode 0600, per-boot token. No MessagePack,
no length prefixes, no protocol versions, no acks, no replies — ever.

Heartbeat (child → supervisor):

```json
{"id":"jobs","state":"healthy","ts":1757700000,"token":"<OTP_RAILS_TOKEN>","meta":{}}
```

Control (anything holding the token → supervisor):

```json
{"cmd":"restart","id":"jobs","token":"<OTP_RAILS_TOKEN>"}
```

Rules, matching the Ruby reference implementation exactly:

- A line with a missing or wrong `token` is silently dropped. Malformed JSON
  is dropped. No reply either way.
- Max line length is 64 KiB (content + newline). Longer lines are malformed:
  dropped without unbounded buffering, resyncing at the next newline.
- `token`, `cmd`, `id`, and `state` must be JSON strings; a non-string value
  in any of them drops the line, same as a bad token.
- A message with a `cmd` key is control-shaped (a non-string `cmd` drops the
  line — it never falls through to the heartbeat branch); otherwise string
  `id` + `state` make a heartbeat. Unknown commands and unknown ids are
  ignored; heartbeats for unknown child ids are dropped at intake.
- `state` is one of `"starting" | "healthy" | "degraded" | "dead"`; anything
  else is treated as healthy (reference behavior).
- Freshness is measured from receipt time on the supervisor's monotonic
  clock; the `ts` field is accepted but not used for aging.
- Missing 3 × `health_interval` ⇒ degraded (telemetry only); 6 ⇒ dead ⇒ the
  child is drained (SIGTERM, `shutdown_ms`, SIGKILL) and restarted by the
  native strategy. A fresh heartbeat reporting `"dead"` also triggers this.
- Heartbeats are cleared when a child is (re)spawned, so a replaced process
  cannot vouch for its successor.
- `{"cmd":"restart"}` drains and replaces the child gracefully; it does not
  count toward restart intensity.

## Telemetry

The §6 names, verbatim (see `OtpRailsBeam.Telemetry.events/0`):

```
[:otp_rails, :supervisor, :start]
[:otp_rails, :supervisor, :stop]
[:otp_rails, :supervisor, :escalate]       # intensity exceeded
[:otp_rails, :child, :spawn]
[:otp_rails, :child, :healthy]
[:otp_rails, :child, :degraded]
[:otp_rails, :child, :exit]                # measurements: {exit_code, uptime_ms}
[:otp_rails, :child, :restart]             # metadata:     {attempt, backoff_ms, strategy}
[:otp_rails, :child, :drain]
[:otp_rails, :child, :kill]                # drain timed out
```

Native OTP restarts immediately, so `backoff_ms` is always 0 here. Native
supervisors expose no hook at the instant intensity is exceeded, so
`:escalate` is emitted by a monitor when the tree exits with reason
`:shutdown` (a clean `OtpRailsBeam.stop/1` emits only `:stop`).

## Dependencies

Kept to the minimum the contract itself demands:

- **`:telemetry`** — DESIGN §6 defines event names that "mirror Elixir
  `:telemetry` so the sidecar can forward them unchanged"; this is that
  library, the BEAM ecosystem standard.
- **`:jason`** — JSON codec for the §5 NDJSON wire format. Elixir has no
  built-in JSON until 1.18/OTP 27 and this project supports Elixir 1.15+.

Everything else is stdlib/OTP: `:gen_tcp` for the Unix socket, `Port` for
process ownership, `Supervisor` for strategies and restart intensity,
`:crypto` for the per-boot token.

## Tests

```
mix deps.get
mix test
```

The tests mirror the Ruby gem's `test/socket_test.rb` with real OS processes
(fixture children are Elixir scripts under `test/fixtures/`, run with
`elixir fixture.exs` so no other runtime is required): heartbeat aging into
degraded/dead + restart, bad-token drops, control restart, and OS-level kill
recovery.

### Contract tests (Ruby ⇄ Elixir interop)

`test/contract/` proves both §5 implementations against the OTHER side's
processes, in both directions:

- this supervisor supervising RUBY children that heartbeat with the real
  `OtpRails::Heartbeat` helper from the published gem (plus a Ruby control
  client sending `{"cmd":"restart"}`);
- the RUBY supervisor (`otp-rails run`, unmodified) supervising an ELIXIR
  stdlib heartbeater, asserted through its logger telemetry.

They need `ruby` and the gem on PATH and are excluded from plain `mix test`:

```
gem install otp-rails
mix test --only contract      # just the interop suite
mix test --include contract   # everything
```

## CI

GitHub Actions on `ubuntu-latest` via `erlef/setup-beam`
(`.github/workflows/ci.yml`). Two jobs: `test` (pure Elixir) and `contract`
(adds `ruby/setup-ruby` + the otp-rails gem) — kept separate so a contract
failure is immediately distinguishable from an Elixir regression. macOS CI is
skipped because `erlef/setup-beam` does not support macOS runners; both suites
pass locally on macOS.
