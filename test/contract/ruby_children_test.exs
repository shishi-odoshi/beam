defmodule OdoshiBeam.Contract.RubyChildrenTest do
  @moduledoc """
  Cross-implementation contract tests, direction 1: the BEAM supervisor
  supervising RUBY children that speak §5 with the real `odoshi` gem
  (`Odoshi::Heartbeat`), plus a Ruby-side §9 control client.

  Excluded from plain `mix test` (they need ruby + the odoshi gem);
  run with `mix test --include contract` or `mix test --only contract`.
  """

  use ExUnit.Case, async: false

  alias OdoshiBeam.TestEvents

  @moduletag :contract
  @moduletag capture_log: true
  @moduletag timeout: 60_000

  @fixtures Path.expand("fixtures", __DIR__)
  @wait_ms 20_000

  setup_all do
    System.find_executable("ruby") || raise "contract tests need ruby on PATH"

    {_out, 0} =
      System.cmd("ruby", ["-e", ~s(require "odoshi/heartbeat")], stderr_to_stdout: true)

    :ok
  rescue
    e in MatchError ->
      reraise "contract tests need the odoshi gem (gem install odoshi): #{inspect(e)}",
              __STACKTRACE__
  end

  setup do
    handle = {agent, _id} = TestEvents.attach()

    # Unix socket paths are capped at ~104 bytes on macOS: short scratch dir.
    dir = Path.join(System.tmp_dir!(), "otpc-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      TestEvents.detach(handle)
      File.rm_rf!(dir)
    end)

    %{agent: agent, tmp_dir: dir}
  end

  test "ruby child using Odoshi::Heartbeat registers active, degrades on silence, is restarted",
       %{tmp_dir: dir, agent: agent} do
    flag = Path.join(dir, "stop.flag")
    sock = Path.join(dir, "s.sock")
    cmd = ["ruby", Path.join(@fixtures, "ruby_heartbeater.rb"), "0.1", flag]

    {:ok, sup} = start_sup(sock, cmd)
    assert wait_until(fn -> spawns(agent) >= 1 end), "ruby child should start"

    assert wait_until(fn -> OdoshiBeam.heartbeated?(sup, "hb") end),
           "the gem's Heartbeat helper should register as active with the beam supervisor"

    File.touch!(flag)

    assert wait_until(fn -> TestEvents.named(agent, :degraded, "hb") != [] end),
           "3 missed intervals of gem-format heartbeats should emit :degraded"

    assert wait_until(fn -> spawns(agent) >= 2 end),
           "6 missed intervals should count as :dead and restart the ruby child"

    OdoshiBeam.stop(sup)
  end

  test "ruby child heartbeating with a bad token is ignored, never treated as active",
       %{tmp_dir: dir, agent: agent} do
    marker = Path.join(dir, "sent.marker")
    sock = Path.join(dir, "s.sock")
    cmd = ["ruby", Path.join(@fixtures, "ruby_bad_heartbeater.rb"), marker]

    {:ok, sup} = start_sup(sock, cmd)
    assert wait_until(fn -> spawns(agent) >= 1 end), "ruby child should start"

    assert wait_until(fn -> File.exists?(marker) end),
           "fixture should have sent its bad-token heartbeats"

    # Twelve health intervals — plenty for a wrongly-accepted heartbeat to age
    # through :degraded (3 missed) into :dead (6) and force a restart.
    Process.sleep(2_500)

    assert spawns(agent) == 1, "a bad-token heartbeat must not make the child active"

    assert TestEvents.named(agent, :degraded, "hb") == [],
           "a dropped heartbeat must not produce degraded reports"

    refute OdoshiBeam.heartbeated?(sup, "hb"), "the heartbeat must not be recorded"

    OdoshiBeam.stop(sup)
  end

  test "ruby control client restarts the child; a bad token is ignored",
       %{tmp_dir: dir, agent: agent} do
    sock = Path.join(dir, "s.sock")
    control = Path.join(@fixtures, "ruby_control.rb")

    {:ok, sup} = start_sup(sock, ["sleep", "30"])
    assert wait_until(fn -> spawns(agent) >= 1 end), "child should start"

    {_out, 0} = System.cmd("ruby", [control, sock, "hb", "wrong-token"], stderr_to_stdout: true)
    Process.sleep(500)
    assert spawns(agent) == 1, "a bad-token control message must be ignored"

    {_out, 0} =
      System.cmd("ruby", [control, sock, "hb", OdoshiBeam.token(sup)], stderr_to_stdout: true)

    assert wait_until(fn -> spawns(agent) >= 2 end),
           "the ruby restart command should replace the child"

    assert TestEvents.named(agent, :drain, "hb") != [], "the old child should have been drained"

    OdoshiBeam.stop(sup)
  end

  defp start_sup(sock, cmd) do
    OdoshiBeam.start_link(
      socket_path: sock,
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 60,
      children: [
        %{id: "hb", cmd: cmd, shutdown_ms: 2_000, health_interval_ms: 200}
      ]
    )
  end

  defp spawns(agent), do: length(TestEvents.named(agent, :spawn, "hb"))

  defp wait_until(fun, timeout \\ @wait_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(fun, deadline)
  end

  defp do_wait(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) > deadline ->
        false

      true ->
        Process.sleep(50)
        do_wait(fun, deadline)
    end
  end
end
