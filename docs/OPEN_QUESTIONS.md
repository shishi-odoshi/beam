# Open questions / contract notes

Where DESIGN.md left room for interpretation, the Ruby reference
implementation (`lib/otp_rails/socket_server.rb`, `lib/otp_rails/supervisor.rb`)
was treated as the tie-breaker and mirrored exactly. Nothing here blocked
implementation; these are recorded so the ambiguity is visible rather than
silently absorbed.

## 1. DESIGN §5's heartbeat example omits `token`

§5 shows `{"id","state","ts","meta"}`, but §9, PLAN 1.4, and the Ruby
implementation all require a `token` field (wrong/missing ⇒ dropped).

- **Options:** (a) token required, per the implementation; (b) token optional,
  per the §5 example.
- **Chosen:** (a) — the conservative reading; a tokenless heartbeat is
  silently dropped, exactly like the Ruby socket layer.
- **Recommendation:** update the §5 example to include `"token"` so the doc
  alone is sufficient (the §8 Phase 0 acceptance bar says it should be).

## 2. `ts` is not used for freshness

The Ruby supervisor ages heartbeats from *receipt time* on its monotonic
clock and ignores `ts` entirely. Mirrored. Using `ts` would introduce clock
coupling between child and supervisor — rejected as a protocol extension.

## 3. Unknown `state` strings count as healthy

Ruby's `HEARTBEAT_STATES.fetch(hb[:state], :healthy)` maps anything outside
`starting/healthy/degraded/dead` to healthy. Mirrored, though "unknown ⇒
degraded" would arguably be safer. Changing it would diverge from the
reference, so it wasn't.

## 4. §6 event fidelity under native OTP supervision (design feedback)

Native OTP supervisors expose no callback at the moment they restart a child
or give up (intensity exceeded), so two events are reconstructed rather than
emitted in-line:

- `[:otp_rails, :child, :restart]` is emitted from the *replacement* child's
  init (spawn counter in ETS), with `backoff_ms: 0` — native OTP has no
  backoff. The `attempt`/`strategy` metadata is preserved.
- `[:otp_rails, :supervisor, :escalate]` is emitted by an external monitor
  when the tree exits with reason `:shutdown` (what a native supervisor exits
  with after exceeding intensity); measurements like Ruby's `{restarts}` /
  `{within}` are not observable from outside and are omitted.

If §6 ever grows guaranteed measurement fields for these two events, the
Elixir side will need a hand-rolled supervisor loop instead of `Supervisor` —
worth knowing before freezing measurements.

---

# Shared queue (Phase 4 step 2) — conservative readings

The solid_queue gem source (1.7.0) was the authority wherever DESIGN/PLAN
left room; the queue-side notes below record where its behavior differed
from the plan's wording or forced a v1 scope call.

## 5. "Releases claimed jobs" actually means "fails them with ProcessPrunedError"

The plan describes SQ's supervisor as *releasing* a dead worker's claimed
executions. Solid Queue 1.x's prune path
(`Process::Prunable#prune` → `fail_all_claimed_executions_with`) does NOT
re-dispatch them to ready: it converts them to `solid_queue_failed_executions`
rows with `SolidQueue::Processes::ProcessPrunedError` and deletes the process
row, leaving retry/discard to the normal failed-job tooling. (True release —
back to ready — happens only on *clean* deregistration via the
`after_destroy` callback, which beam mirrors on graceful shutdown.) The
orphan interop test asserts the failed-with-ProcessPrunedError behavior,
because that is the real contract.

## 6. Concurrency-controlled and batched jobs are Ruby-only (v1)

`ClaimedExecution#finished` runs `job.unblock_next_blocked_job` (semaphore
release + blocked-execution promotion) and batch-progress callbacks. beam
does not implement either; jobs with `concurrency_key` or `batch_id` should
not be routed to beam queues. Degradation is graceful, not silent corruption:
an unreleased semaphore expires after `concurrency_duration` and the Ruby
dispatcher's concurrency maintenance dispatches the blocked job.
- **Options:** (a) document the routing rule; (b) implement semaphore
  release in SQL (subtle: `Semaphore::Proxy` + blocked-execution promotion
  ordering). **Chosen:** (a) for v1.

## 7. Unregistered job class on a designated queue ⇒ loud failed_execution

If a job lands on a beam queue with no registered handler, beam fails it
(`OtpRailsBeam.Queue.UnknownJobClassError`) instead of skipping it. Skipping
would either leave it claimed forever (blocks pruning heuristics) or
silently starve it — a routing bug should be visible in Mission Control and
retriable after registering the handler. Filtering the claim query by
class_name was rejected: it diverges from `ReadyExecution.claim`'s SQL shape,
and "designated queue" is the routing contract.

## 8. Handler failures are not ActiveJob retries

`retry_on`/`discard_on` execute inside a Ruby worker's ActiveJob layer.
beam failures go straight to `failed_executions` with `executions`
unchanged; SQ-side tooling owns retry/discard. Documented in the README and
the `Handler` moduledoc rather than emulating ActiveJob's retry bookkeeping
(which would mean re-serializing envelopes and re-scheduling — Ruby-side
semantics beam must not fork).

## 9. `preserve_finished_jobs` is assumed true

beam always writes `finished_at` (mirroring `Job#finished!` with the
default `SolidQueue.preserve_finished_jobs = true`). If an app sets it to
false, Ruby workers destroy finished jobs while beam preserves them — the
`clear_finished_jobs_after` dispatcher cleanup still reaps beam's rows, so
the divergence is cosmetic; revisit only if someone actually runs
`preserve_finished_jobs = false`.
