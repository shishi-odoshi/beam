# otp_rails_beam

An Elixir sidecar supervisor for Rails (or arbitrary) OS processes — the
`beam` repo from the [otp-rails](https://github.com/shishi-odoshi) project
(DESIGN §2). It supervises external commands through native OTP supervision
(each child is a GenServer owning a Port), consumes the DESIGN §5 health
protocol over a Unix socket, and emits the DESIGN §6 telemetry events.

It depends only on the §5/§6 protocols and the DB schema — never on the Ruby
gem's code. Phase 4 step 1 is the ports-based supervision below; step 2 is
the [shared Solid Queue worker](#shared-job-queue-otprailsbeamqueue); step 3
is the [ActionCable-compatible cable](#actioncable-compatible-cable-otprailsbeamcable).

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

## ActionCable-compatible cable (`OtpRailsBeam.Cable`)

Phase 4 step 3 (decision log 2026-09-13): beam's realtime layer is
ACTIONCABLE-COMPATIBLE — it serves the ActionCable v1 JSON wire protocol
over WebSockets, sourcing broadcasts from the SOLID CABLE schema (Rails 8's
default cable adapter). Existing Turbo Streams / `@rails/actioncable`
clients repoint their cable URL at beam with zero Rails-side code changes;
Rails keeps broadcasting exactly as before (`Turbo::Broadcastable`,
`ActionCable.server.broadcast` → `solid_cable_messages`). Native Phoenix
channel semantics are explicitly deferred.

```elixir
{:ok, cable} =
  OtpRailsBeam.Cable.start_link(
    port: 28080,
    db: [hostname: "localhost", database: "app_production_cable", username: "app", password: "..."],
    secret_key_base: System.fetch_env!("OTP_RAILS_CABLE_SECRET")
  )
```

Clients connect to `ws://host:28080/cable`. Options (`:path`,
`:polling_interval_ms`, `:allowed_channels`, `:key_digest`, ...) are
documented on the module.

### Wire protocol

The actioncable gem (8.0.x) is the authority; beam mirrors it frame for
frame:

- Subprotocol negotiation from `ActionCable::INTERNAL[:protocols]`
  (`actioncable-v1-json` first; a client offering none still connects).
- `{"type":"welcome"}` on open; `{"type":"ping","message":<unix seconds>}`
  every 3s (`Server::Connections::BEAT_INTERVAL`).
- `subscribe` → `confirm_subscription` / `reject_subscription` with the
  identifier echoed verbatim; duplicate subscribes silently ignored
  (`Subscriptions#add`); `unsubscribe` → no reply frame.
- Broadcasts as `{"identifier":...,"message":<JSON-decoded payload>}`, the
  exact output of Action Cable's default stream handler.

One deliberate divergence: Ruby Action Cable handles a subscribe for an
unknown channel class by logging "Subscription class not found" and sending
nothing; beam sends an explicit `reject_subscription` so clients aren't
left hanging.

### Auth (v1 boundary)

A subscription is accepted iff:

- its channel is `Turbo::StreamsChannel` and its `signed_stream_name`
  verifies — beam re-derives Turbo's verifier key from the shared Rails
  secret exactly like `Rails.application.key_generator`:
  PBKDF2-HMAC-SHA256(`secret_key_base`, salt
  `"turbo/signed_stream_verifier_key"`, 1000 iterations, 64 bytes), then
  checks the `base64(JSON)--HMAC-SHA256-hexdigest` signature
  (`ActiveSupport::MessageVerifier`, `digest: "SHA256", serializer: JSON`).
  The secret comes from `:secret_key_base` /`OTP_RAILS_CABLE_SECRET` /
  `SECRET_KEY_BASE` — Turbo keys off `secret_key_base` itself, so sharing
  it is what makes Rails-signed stream names verify in beam. Apps that set
  `config.turbo.signed_stream_verifier_key` pass it via
  `:signed_stream_verifier_key`; apps on pre-7.0 framework defaults pass
  `key_digest: :sha1`.
- OR its channel name is in `:allowed_channels` (default EMPTY), a map of
  channel class name to a `params -> stream | [streams] | nil` function —
  the beam-side stand-in for that channel's Ruby `subscribed` method, since
  beam runs no Ruby channel code.

Everything else is rejected. There is NO cookie/session authentication
(`identified_by :current_user`-style connection auth needs the Rails
session beam never sees) and NO origin checking: authorization lives
entirely in the unguessable signed stream name. Don't allowlist channels
whose streams carry data the signed-name ceremony was protecting.

### Broadcast sourcing and replay-avoidance

Solid Cable (3.0.x) is a POLLING adapter, and beam consumes it exactly the
way its own Ruby listener does (`lib/action_cable/subscription_adapter/
solid_cable.rb`):

- poll every `polling_interval` (default 0.1s) with
  `WHERE channel_hash = ANY(...) AND id > cursor ORDER BY id` —
  `Message.broadcastable`; `channel_hash` is the first 8 bytes of
  SHA-256(channel) as a signed big-endian 64-bit integer
  (`Message.channel_hash_for`), with hash collisions resolved by exact
  channel bytes at fan-out;
- the global cursor starts at `MAX(id)` on boot and each stream's baseline
  is `MAX(id)` at first-subscribe (`Listener#add_channel`), so reconnects
  never replay messages broadcast before the new subscription — the same
  guarantee the Ruby listener gives;
- beam only ever READS `solid_cable_messages`: retention stays Rails-owned
  (Solid Cable's autotrim `TrimJob` deletes rows older than
  `message_retention`), and trimmed history is naturally invisible to the
  `id > cursor` predicate.

### Cable telemetry

Beam-local events (the §6 supervision contract is frozen and untouched):

```
[:otp_rails_beam, :cable, :connect]    %{system_time}  %{}
[:otp_rails_beam, :cable, :subscribe]  %{system_time}  %{identifier, streams}
[:otp_rails_beam, :cable, :reject]     %{system_time}  %{identifier, reason}
[:otp_rails_beam, :cable, :broadcast]  %{subscribers}  %{channel, message_id}
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

- **`:bandit` + `:websock_adapter`** — the websocket stack for the cable
  endpoint (Phase 4 step 3). The decision log explicitly allows serving the
  ActionCable protocol with a minimal Elixir websocket stack rather than
  full Phoenix, and full Phoenix would be the wrong tool here: beam serves
  ONE fixed wire protocol at one path — no channels DSL (ActionCable
  semantics are hand-mirrored from the gem source, the way the queue
  mirrors solid_queue), no PubSub (broadcasts come from Postgres polling,
  Solid Cable's own model), no endpoint/router/PubSub/code-reloading
  machinery. Bandit is the pure-Elixir HTTP server Phoenix itself defaults
  to (maintained, HTTP/1.1+HTTP/2, built on Plug), and `websock_adapter` is
  the thin standard shim that upgrades a Plug request into a `WebSock`
  handler — the same pair Phoenix uses underneath, minus the framework.
  Cowboy would have worked too, but its Erlang-flavored handler API and
  ranch supervision integrate less cleanly with a Plug-shaped endpoint and
  per-connection process semantics we test against.

Everything else is stdlib/OTP: `:gen_tcp` for the Unix socket, `Port` for
process ownership, `Supervisor` for strategies and restart intensity,
`:crypto` for the per-boot token, PBKDF2/HMAC key material, and channel
hashing.

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

### Cable interop tests (Solid Cable / Turbo ⇄ beam)

`test/cable/cable_interop_test.exs` (tagged `:cable`, excluded from plain
`mix test`) proves the cable against the REAL Ruby stack — no mocks. The
Ruby side is a test-only bundle under `test/fixtures/solid_cable/` (rails +
actioncable + solid_cable ~> 3.0 + turbo-rails ~> 2.0 + pg) that loads the
gem's own cable schema, signs stream names through
`Turbo::Streams::StreamName` with the key the turbo engine initializer
derives, and broadcasts through Action Cable's `server.broadcast` → Solid
Cable pubsub adapter. The Elixir side is a hand-rolled RFC 6455 test client
(`test/support/cable_ws_client.ex`) speaking to a real
`OtpRailsBeam.Cable`. Covered:

- welcome frame + `actioncable-v1-json` subprotocol negotiation (and the
  no-subprotocol case);
- ping cadence and unix-time payloads;
- Ruby-signed Turbo subscription confirmed, Ruby-broadcast payloads
  (raw turbo-stream HTML strings and structured objects) delivered
  verbatim as broadcast frames;
- tampered signature and non-allowlisted channels rejected; allowlisted
  channel subscribing via its params-to-stream function;
- stream isolation, unsubscribe semantics, duplicate-subscribe
  suppression, no replay on reconnect, and exactly-one-copy delivery to
  concurrent subscribers.

Pure-Elixir unit tests (`test/cable/signed_stream_name_test.exs`, no tag)
pin the verifier and `channel_hash` against Ruby-produced vectors in the
default suite. The interop suite needs the same docker Postgres as the
queue suite (it creates its own `otp_rails_beam_cable_test` database) plus
the fixture bundle:

```
(cd test/fixtures/solid_cable && bundle install)
mix test --only cable
```

Override connection settings with `SOLID_CABLE_PG_HOST` / `_PORT` / `_USER`
/ `_PASSWORD` / `_DATABASE`.

## CI

GitHub Actions on `ubuntu-latest` via `erlef/setup-beam`
(`.github/workflows/ci.yml`). Four jobs: `test` (pure Elixir), `contract`
(adds `ruby/setup-ruby` + the otp-rails gem), `queue` (adds a `postgres:16`
service + the solid_queue fixture bundle), and `cable` (postgres service +
the solid_cable fixture bundle) — kept separate so a contract, queue, or
cable failure is immediately distinguishable from an Elixir regression.
macOS CI is
skipped because `erlef/setup-beam` does not support macOS runners; both suites
pass locally on macOS.
