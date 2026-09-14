defmodule OdoshiBeam.Contract.RubySupervisorTest do
  @moduledoc """
  Cross-implementation contract tests, direction 2: the RUBY supervisor
  (`odoshi run` from the published gem, unmodified) supervising an ELIXIR
  child that heartbeats §5 NDJSON with nothing but the Elixir stdlib
  (`test/fixtures/heartbeater.exs` — the same fixture the pure-Elixir suite
  uses against the beam supervisor).

  The Ruby supervisor is driven as a subprocess via a temp
  `config/supervisor.rb`; assertions read its default logger telemetry
  (`[odoshi] odoshi.child.* ...` lines on stderr):

  * `child.spawn` — the Elixir child started;
  * `child.degraded` after the fixture goes silent — proves the supervisor
    had registered the Elixir heartbeats as ACTIVE health (a passive
    :command child with a live PID never degrades) and ages them by
    freshness, 3 missed intervals ⇒ degraded;
  * a second `child.spawn` + `child.restart` — 6 missed ⇒ dead ⇒ restart.

  Excluded from plain `mix test`; run with `mix test --include contract`.
  """

  use ExUnit.Case, async: false

  @moduletag :contract
  @moduletag timeout: 120_000

  @heartbeater Path.expand("../fixtures/heartbeater.exs", __DIR__)
  @wait_ms 30_000

  setup do
    # Unix socket paths are capped at ~104 bytes on macOS: short scratch dir.
    dir = Path.join(System.tmp_dir!(), "otpr-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{tmp_dir: dir}
  end

  test "elixir child under the ruby supervisor: active, degraded on silence, restarted",
       %{tmp_dir: dir} do
    exe =
      System.find_executable("odoshi") ||
        raise "contract tests need the odoshi gem's CLI on PATH (gem install odoshi)"

    flag = Path.join(dir, "stop.flag")
    sock = Path.join(dir, "rs.sock")
    config = Path.join(dir, "supervisor.rb")

    # Plain-Ruby DSL, evaluated by the gem without Rails (DESIGN §9).
    File.write!(config, """
    socket "#{sock}"
    max_restarts 10, within: 60
    backoff :none
    child :ex, adapter: :command, shutdown: 2, start_timeout: 20, health_interval: 0.2,
          cmd: "elixir #{@heartbeater} 0.1 #{flag} ex"
    """)

    port =
      Port.open({:spawn_executable, exe}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["run", config]
      ])

    os_pid =
      case Port.info(port, :os_pid) do
        {:os_pid, pid} -> pid
        nil -> nil
      end

    on_exit(fn ->
      # Belt and braces: if the assertion path failed before the clean TERM,
      # don't leave a supervisor (and its children) running.
      if os_pid,
        do: System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
    end)

    {:ok, buf} = await(port, "", fn buf -> spawns(buf) >= 1 end)
    assert spawns(buf) >= 1, "ruby supervisor should spawn the elixir child\n#{buf}"

    # Let the stdlib heartbeater connect and register several beats (0.1s
    # interval vs 0.2s health interval) before wedging it.
    Process.sleep(1_500)
    File.touch!(flag)

    {:ok, buf} = await(port, buf, fn buf -> buf =~ "odoshi.child.degraded" end)

    assert buf =~ "odoshi.child.degraded",
           "silence after active §5 heartbeats should age into :degraded\n#{buf}"

    {:ok, buf} = await(port, buf, fn buf -> spawns(buf) >= 2 end)
    assert spawns(buf) >= 2, "6 missed intervals should get the elixir child restarted\n#{buf}"

    assert buf =~ "odoshi.child.restart",
           "the replacement should be a strategy restart, not a silent respawn\n#{buf}"

    # Clean shutdown: TERM is trapped by the CLI and drains the tree.
    {_out, 0} = System.cmd("kill", ["-TERM", Integer.to_string(os_pid)])
    assert {:ok, status, _buf} = await_exit(port, buf), "ruby supervisor should exit on TERM"
    assert status == 0, "ruby supervisor should shut down cleanly (got exit #{status})"
  end

  defp spawns(buf) do
    buf |> String.split("odoshi.child.spawn") |> length() |> Kernel.-(1)
  end

  defp await(port, buf, pred, timeout \\ @wait_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await(port, buf, pred, deadline)
  end

  defp do_await(port, buf, pred, deadline) do
    if pred.(buf) do
      {:ok, buf}
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        flunk("timed out waiting for supervisor output; got:\n#{buf}")
      else
        receive do
          {^port, {:data, chunk}} -> do_await(port, buf <> chunk, pred, deadline)
          {^port, {:exit_status, status}} -> flunk("supervisor exited #{status} early:\n#{buf}")
        after
          remaining -> flunk("timed out waiting for supervisor output; got:\n#{buf}")
        end
      end
    end
  end

  defp await_exit(port, buf, timeout \\ 15_000) do
    receive do
      {^port, {:exit_status, status}} -> {:ok, status, buf}
      {^port, {:data, chunk}} -> await_exit(port, buf <> chunk, timeout)
    after
      timeout -> :timeout
    end
  end
end
