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
