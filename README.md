# otp_rails_beam

An Elixir sidecar supervisor for Rails (or arbitrary) OS processes — the
`beam` repo from the [otp-rails](https://github.com/shishi-odoshi) project
(DESIGN §2). It supervises external commands through native OTP supervision
(each child is a GenServer owning a Port), consumes the DESIGN §5 health
protocol over a Unix socket, and emits the DESIGN §6 telemetry events.

It depends only on the §5/§6 protocols and the DB schema — never on the Ruby
gem's code. Phase 4 step 1 is the ports-based supervision below; step 2 is
the [shared Solid Queue worker](#shared-job-queue-otprailsbeamqueue); Phoenix
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

## Shared job queue (`OtpRailsBeam.Queue`)

Phase 4 step 2: beam as an alternate job runner, consuming the SAME Solid
Queue Postgres schema the Rails app writes (decision: Solid Queue schema, not
GoodJob, not a custom table; Postgres-only for v1). beam executes ONLY jobs
routed to designated queue(s) with registered Elixir handlers — Ruby workers
keep everything else, and both kinds of worker plus Solid Queue's own
supervisor run concurrently against the same tables.

```elixir
defmodule MyApp.HardJob do
  @behaviour OtpRailsBeam.Queue.Handler

  @impl true
  def perform([user_id, options]) do
    # plain JSON-typed args from the ActiveJob envelope
    :ok
  end
end

{:ok, queue} =
  OtpRailsBeam.Queue.start_link(
    db: [hostname: "localhost", database: "app_production", username: "app", password: "..."],
    queues: ["elixir"],                        # exact names only, no wildcards
    handlers: %{"HardJob" => MyApp.HardJob}    # ActiveJob class_name => module
  )
```

Rails routes work to it the normal ActiveJob way — `queue_as :elixir` (or
`SomeJob.set(queue: "elixir")`); nothing on the Ruby side knows or cares that
the worker is a BEAM process.

### Mirrored Solid Queue semantics

The solid_queue gem (1.7.x) is the schema and semantics authority; the SQL in
`OtpRailsBeam.Queue.Store` mirrors the Ruby worker exactly:

- **Claiming** (`ReadyExecution.claim`): per queue in configured order, minus
  paused queues (`solid_queue_pauses`), `SELECT … ORDER BY priority ASC,
  job_id ASC LIMIT n FOR UPDATE SKIP LOCKED`, then the
  `solid_queue_claimed_executions` insert and ready-row delete in the same
  transaction — safe against concurrent Ruby workers by construction.
- **Success** (`ClaimedExecution#finished`): lock the claimed row, set
  `solid_queue_jobs.finished_at`, delete the claimed row; skip silently if
  the row is already gone (finalized or pruned by someone else).
- **Failure** (`ClaimedExecution#failed_with`): same finalize dance writing a
  `solid_queue_failed_executions` row whose `error` JSON
  (`exception_class`/`message`/`backtrace`) loads cleanly into
  `SolidQueue::FailedExecution` for Mission Control retry/discard.
- **Process registry**: registers in `solid_queue_processes` (kind
  `"Worker"`, `worker-<hex>` name, OS pid, hostname, metadata) and heartbeats
  `last_heartbeat_at` on Solid Queue's cadence (60s default), so a live beam
  worker never looks prunable — and a SIGKILLed one is cleaned up by Solid
  Queue's supervisor exactly like a dead Ruby worker: after
  `process_alive_threshold` its claimed executions are failed with
  `ProcessPrunedError` and its process row deleted. Clean shutdown
  deregisters and releases still-claimed executions back to ready
  (`after_destroy :release_all_claimed_executions`).

### Retries, and other v1 boundaries

- **Elixir handler failures are NOT ActiveJob retries.** `retry_on` /
  `discard_on` run inside a Ruby worker; a failing handler sends the job
  straight to `failed_executions`, where Solid Queue's normal tooling
  retries or discards it. A job class with no registered handler on a
  designated queue fails the same loud way (`UnknownJobClassError`) rather
  than dangling or being silently skipped.
- **Arguments are plain JSON types.** The ActiveJob envelope
  (`job_class`/`job_id`/`queue_name`/`arguments`) is decoded; ActiveJob's
  hash markers (`_aj_symbol_keys`, `_aj_hash_with_indifferent_access`,
  `_aj_ruby2_keywords`) are stripped. GlobalID references and
  custom-serialized objects fail with a `DeserializationError`-shaped row —
  route jobs carrying them to Ruby queues (v1).
- **Concurrency controls / batches**: jobs with `concurrency_key` should stay
  on Ruby queues — beam doesn't release the semaphore on completion (the
  Ruby dispatcher's maintenance recovers expired ones); batch progress
  callbacks don't fire for beam-finished jobs. Finished jobs are always
  preserved (`preserve_finished_jobs`).
- Scheduled/recurring dispatch stays with Solid Queue's Ruby dispatcher;
  beam only works `solid_queue_ready_executions`, so `wait:`/recurring jobs
  flow through the dispatcher and get picked up once ready.
- v1 executes jobs sequentially per queue process (`:batch_size` bounds the
  claim, like the Ruby pool's capacity).

### Queue telemetry

Beam-local events (the §6 supervision contract is frozen and untouched):

```
[:otp_rails_beam, :job, :start]     %{system_time}    %{job_id, active_job_id, class_name, queue_name}
[:otp_rails_beam, :job, :finish]    %{duration_ms}    same metadata
[:otp_rails_beam, :job, :failure]   %{duration_ms}    metadata + %{error: %{exception_class, message, backtrace}}
```

## Dependencies

Kept to the minimum the contract itself demands:

- **`:telemetry`** — DESIGN §6 defines event names that "mirror Elixir
  `:telemetry` so the sidecar can forward them unchanged"; this is that
  library, the BEAM ecosystem standard.
- **`:jason`** — JSON codec for the §5 NDJSON wire format. Elixir has no
  built-in JSON until 1.18/OTP 27 and this project supports Elixir 1.15+.

- **`:postgrex`** — the PostgreSQL driver for the shared Solid Queue tables
  (Phase 4 step 2 is Postgres-only by decision). The SQL is hand-rolled, not
  Ecto: the queue surface is eight fixed statements against a schema owned
  by the Rails side, where the exact locking clauses (`FOR UPDATE SKIP
  LOCKED`, transaction boundaries) ARE the contract — a query builder and
  schema/migration layer would add three dependencies and a second source of
  truth for tables beam must never define, while making the mirrored
  statements harder to compare against the Ruby originals.

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

### Shared-queue interop tests (Solid Queue ⇄ beam)

`test/queue/` (tagged `:queue`, excluded from plain `mix test`) proves the
shared queue against the REAL solid_queue gem — no mocks. The Ruby side is a
test-only bundle under `test/fixtures/solid_queue/` (activerecord +
solid_queue ~> 1.7 + pg) that loads the gem's own schema, enqueues
ActiveJob-enveloped jobs, runs a genuine `SolidQueue::Worker`, and runs the
supervisor's `SolidQueue::Process.prune` maintenance call. Covered:

- beam claims/executes ONLY elixir-queue jobs (side effects + `finished_at`
  + cleaned ready/claimed rows); default-queue jobs untouched;
- 50-job bursts on each side with beam and a Ruby worker polling
  concurrently: zero cross-claims, zero double-executions;
- a failing Elixir handler produces a `failed_executions` row that
  `SolidQueue::FailedExecution` loads on the Ruby side;
- `kill -9` mid-claim: once the heartbeat goes stale, Solid Queue's own
  pruning fails the orphaned claims with `ProcessPrunedError` and removes
  the dead process row.

They need docker Postgres and the fixture bundle:

```
docker run -d --name otp-rails-beam-queue-pg \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=otp_rails_beam_queue_test \
  -p 55433:5432 postgres:16
(cd test/fixtures/solid_queue && bundle install)
mix test --only queue
```

Connection defaults match that container; override with `SOLID_QUEUE_PG_HOST`
/ `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE`.

## CI

GitHub Actions on `ubuntu-latest` via `erlef/setup-beam`
(`.github/workflows/ci.yml`). Three jobs: `test` (pure Elixir), `contract`
(adds `ruby/setup-ruby` + the otp-rails gem), and `queue` (adds a
`postgres:16` service + the solid_queue fixture bundle) — kept separate so a
contract or queue failure is immediately distinguishable from an Elixir
regression. macOS CI is
skipped because `erlef/setup-beam` does not support macOS runners; both suites
pass locally on macOS.
