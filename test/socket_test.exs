defmodule OtpRailsBeam.SocketTest do
  @moduledoc """
  Mirrors test/socket_test.rb in the Ruby gem — real processes, no mocks,
  generous timing:

  1. a child that heartbeats then goes silent (flag file) ⇒ degraded, then
     killed and restarted (3/6 missed-interval aging);
  2. heartbeats with a bad token are dropped — child stays up, no restart;
  3. `{"cmd":"restart"}` on the socket replaces the child; a bad token is
     ignored;
  4. killing the child's OS process ⇒ native OTP restart.
  """

  use ExUnit.Case, async: false

  import Bitwise

  alias OtpRailsBeam.TestEvents

  @moduletag capture_log: true
  @moduletag timeout: 60_000

  @fixtures Path.expand("fixtures", __DIR__)
  @wait_ms 20_000

  setup do
    handle = {agent, _id} = TestEvents.attach()

    # Unix socket paths are capped at ~104 bytes on macOS, so use a short
    # scratch dir rather than ExUnit's deeply nested :tmp_dir.
    dir = Path.join(System.tmp_dir!(), "otpb-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      TestEvents.detach(handle)
      File.rm_rf!(dir)
    end)

    %{agent: agent, tmp_dir: dir}
  end

  test "child that stops heartbeating is degraded then restarted", %{tmp_dir: dir, agent: agent} do
    flag = Path.join(dir, "stop.flag")
    sock = Path.join(dir, "s.sock")
    cmd = ["elixir", Path.join(@fixtures, "heartbeater.exs"), "0.1", flag]

    {:ok, sup} = start_sup(sock, cmd)

    assert (File.stat!(sock).mode &&& 0o777) == 0o600, "socket must be mode 0600"
    assert wait_until(fn -> spawns(agent) >= 1 end), "child should start"

    assert wait_until(fn -> OtpRailsBeam.heartbeated?(sup, "hb") end),
           "supervisor should have recorded a heartbeat"

    File.touch!(flag)

    assert wait_until(fn -> TestEvents.named(agent, :degraded, "hb") != [] end),
           "3 missed intervals should emit :degraded"

    assert wait_until(fn -> spawns(agent) >= 2 end),
           "6 missed intervals should count as :dead and restart the child"

    OtpRailsBeam.stop(sup)
  end

  test "heartbeats with a bad token are dropped", %{tmp_dir: dir, agent: agent} do
    marker = Path.join(dir, "sent.marker")
    sock = Path.join(dir, "s.sock")
    cmd = ["elixir", Path.join(@fixtures, "bad_heartbeater.exs"), marker]

    {:ok, sup} = start_sup(sock, cmd)

    assert wait_until(fn -> spawns(agent) >= 1 end), "child should start"
    assert wait_until(fn -> File.exists?(marker) end), "fixture should have sent its heartbeat"

    # Twelve health intervals — plenty for a wrongly-accepted heartbeat to age
    # through :degraded (3) into :dead (6) and force a restart.
    Process.sleep(2_500)

    assert spawns(agent) == 1, "a bad-token heartbeat must not make the child active"

    assert TestEvents.named(agent, :degraded, "hb") == [],
           "a dropped heartbeat must not produce degraded reports"

    refute OtpRailsBeam.heartbeated?(sup, "hb"), "the heartbeat must not be recorded"

    OtpRailsBeam.stop(sup)
  end

  test "control restart replaces the child and a bad token is ignored", %{
    tmp_dir: dir,
    agent: agent
  } do
    sock = Path.join(dir, "s.sock")

    {:ok, sup} = start_sup(sock, ["sleep", "30"])
    assert wait_until(fn -> spawns(agent) >= 1 end), "child should start"

    {:ok, conn} =
      :gen_tcp.connect({:local, String.to_charlist(sock)}, 0, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        conn,
        Jason.encode!(%{cmd: "restart", id: "hb", token: "wrong-token"}) <> "\n"
      )

    Process.sleep(500)
    assert spawns(agent) == 1, "a bad-token control message must be ignored"

    :ok =
      :gen_tcp.send(
        conn,
        Jason.encode!(%{cmd: "restart", id: "hb", token: OtpRailsBeam.token(sup)}) <> "\n"
      )

    assert wait_until(fn -> spawns(agent) >= 2 end), "restart command should replace the child"
    assert TestEvents.named(agent, :drain, "hb") != [], "the old child should have been drained"

    :gen_tcp.close(conn)
    OtpRailsBeam.stop(sup)
  end

  test "killing the child's OS process triggers a restart", %{tmp_dir: dir, agent: agent} do
    sock = Path.join(dir, "s.sock")

    {:ok, sup} = start_sup(sock, ["sleep", "30"])
    assert wait_until(fn -> spawns(agent) >= 1 end), "child should start"

    pid1 = OtpRailsBeam.child_os_pid(sup, "hb")
    assert is_integer(pid1)

    {_out, 0} = System.cmd("kill", ["-KILL", Integer.to_string(pid1)])

    assert wait_until(fn -> spawns(agent) >= 2 end), "killed child should be restarted"
    assert TestEvents.named(agent, :exit, "hb") != [], "the OS death should emit child.exit"

    assert TestEvents.named(agent, :restart, "hb") != [],
           "the crash respawn should emit child.restart"

    assert wait_until(fn ->
             pid2 = OtpRailsBeam.child_os_pid(sup, "hb")
             is_integer(pid2) and pid2 != pid1
           end),
           "the replacement child should have a fresh OS pid"

    OtpRailsBeam.stop(sup)
  end

  defp start_sup(sock, cmd) do
    OtpRailsBeam.start_link(
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
