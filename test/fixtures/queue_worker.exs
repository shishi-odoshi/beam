# Kill-target for the orphan-release interop test: a standalone OS process
# running OdoshiBeam.Queue with a handler that never returns, so its
# claimed executions stay claimed until the process is SIGKILLed and Solid
# Queue's Ruby-side pruning cleans up after it.
#
# Run with the compiled test build on the code path:
#   elixir -pa _build/test/lib/*/ebin test/fixtures/queue_worker.exs

{:ok, _} = Application.ensure_all_started(:postgrex)
{:ok, _} = Application.ensure_all_started(:telemetry)
{:ok, _} = Application.ensure_all_started(:jason)

defmodule KillTarget.StuckHandler do
  @behaviour OdoshiBeam.Queue.Handler

  @impl true
  def perform(_args) do
    Process.sleep(:infinity)
  end
end

db = [
  hostname: System.get_env("SOLID_QUEUE_PG_HOST", "127.0.0.1"),
  port: String.to_integer(System.get_env("SOLID_QUEUE_PG_PORT", "55433")),
  username: System.get_env("SOLID_QUEUE_PG_USER", "postgres"),
  password: System.get_env("SOLID_QUEUE_PG_PASSWORD", "postgres"),
  database: System.get_env("SOLID_QUEUE_PG_DATABASE", "odoshi_beam_queue_test")
]

{:ok, _queue} =
  OdoshiBeam.Queue.start_link(
    db: db,
    queues: ["elixir"],
    handlers: %{"MarkerJob" => KillTarget.StuckHandler},
    polling_interval_ms: 50,
    heartbeat_interval_ms: 500
  )

IO.puts("QUEUE_WORKER_UP #{System.pid()}")
Process.sleep(:infinity)
