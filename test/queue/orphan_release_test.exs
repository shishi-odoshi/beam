defmodule OtpRailsBeam.Queue.OrphanReleaseTest do
  @moduledoc """
  The orphan contract, proven with a real SIGKILL: a beam worker that dies
  without deregistering must be cleaned up by Solid Queue's OWN Ruby-side
  pruning exactly like a dead Ruby worker — claimed executions failed with
  `SolidQueue::Processes::ProcessPrunedError` (retriable through Solid
  Queue's normal tooling) and the stale process row deleted, once its
  heartbeat ages past `process_alive_threshold`.
  """

  use ExUnit.Case, async: false

  import OtpRailsBeam.QueueHelpers

  @moduletag :queue
  @moduletag timeout: 120_000

  setup do
    setup_db!()
    {:ok, conn: connect!(), dir: tmp_dir!("orphan")}
  end

  test "kill -9 mid-claim: SQ's pruning releases the dead worker's claimed jobs",
       %{conn: conn, dir: dir} do
    log = Path.join(dir, "log")
    ruby!("enqueue.rb", ["elixir", "2", "k", log])

    # A separate OS process running OtpRailsBeam.Queue with a stuck handler
    # (claims both jobs, then sleeps in perform forever).
    port = spawn_kill_target()
    os_pid = await_line(port, "QUEUE_WORKER_UP")

    wait_until(fn ->
      one!(conn, "SELECT count(*) FROM solid_queue_claimed_executions") == 2
    end)

    assert one!(conn, "SELECT count(*) FROM solid_queue_processes") == 1

    # SIGKILL: no terminate/2, no deregister — the rows are orphaned.
    {_, 0} = System.cmd("kill", ["-9", os_pid])
    await_exit(port)

    # Rows survive the death untouched: nothing but the heartbeat going stale
    # marks this worker dead.
    assert one!(conn, "SELECT count(*) FROM solid_queue_claimed_executions") == 2
    assert one!(conn, "SELECT count(*) FROM solid_queue_processes") == 1

    # Let the last heartbeat (500ms cadence) age past the shortened 1s
    # threshold, then run Solid Queue's own maintenance prune.
    Process.sleep(1_600)
    assert ruby!("prune.rb", ["1"]) =~ "PRUNED processes=0 claimed=0 failed=2"

    # Claimed executions became failed executions with ProcessPrunedError…
    rows = rows!(conn, "SELECT error FROM solid_queue_failed_executions ORDER BY job_id")
    assert length(rows) == 2

    for [error_json] <- rows do
      error = Jason.decode!(error_json)
      assert error["exception_class"] == "SolidQueue::Processes::ProcessPrunedError"
      assert error["message"] =~ "found dead and pruned"
    end

    # …the stale process row is gone, and the jobs are retriable, not finished.
    assert one!(conn, "SELECT count(*) FROM solid_queue_processes") == 0
    assert one!(conn, "SELECT count(*) FROM solid_queue_jobs WHERE finished_at IS NOT NULL") == 0
  end

  defp spawn_kill_target do
    elixir = System.find_executable("elixir") || flunk("elixir not on PATH")

    pa_args =
      Mix.Project.build_path()
      |> Path.join("lib/*/ebin")
      |> Path.wildcard()
      |> Enum.flat_map(&["-pa", &1])

    script = Path.expand("../fixtures/queue_worker.exs", __DIR__)

    env =
      for {k, v} <- ruby_env(), String.starts_with?(k, "SOLID_QUEUE_") do
        {String.to_charlist(k), String.to_charlist(v)}
      end

    Port.open({:spawn_executable, elixir}, [
      :binary,
      :exit_status,
      {:line, 4096},
      args: pa_args ++ [script],
      env: env
    ])
  end

  defp await_line(port, prefix) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        case String.split(line) do
          [^prefix, os_pid] -> os_pid
          _ -> await_line(port, prefix)
        end

      {^port, {:exit_status, status}} ->
        flunk("kill target exited early with status #{status}")
    after
      30_000 -> flunk("kill target did not report #{prefix}")
    end
  end

  defp await_exit(port) do
    receive do
      {^port, {:exit_status, _}} -> :ok
      {^port, {:data, _}} -> await_exit(port)
    after
      10_000 -> flunk("kill target did not exit after SIGKILL")
    end
  end
end
