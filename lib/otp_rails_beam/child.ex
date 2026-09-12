defmodule OtpRailsBeam.Child do
  @moduledoc """
  One GenServer per supervised OS process, owning its Port.

  Health model (DESIGN §5, mirroring the Ruby `effective_health`):

  * A child that has heartbeated is judged by heartbeats — its own reported
    state, aged by freshness: 3 missed `health_interval`s ⇒ degraded
    (telemetry only), 6 ⇒ dead ⇒ drain (SIGTERM, wait `shutdown_ms`, SIGKILL)
    and stop so native OTP supervision restarts it.
  * A child that never heartbeats is judged by OS process aliveness: the Port
    delivers `{:exit_status, code}` the moment the process dies, which stops
    this GenServer and triggers the native restart strategy.

  Heartbeats are cleared on (re)spawn so a replaced child can't vouch for its
  successor — same rule as the Ruby supervisor.
  """

  use GenServer

  alias OtpRailsBeam.Telemetry

  @hb_states %{
    "starting" => :starting,
    "healthy" => :healthy,
    "degraded" => :degraded,
    "dead" => :dead
  }

  def start_link({ctx, spec}), do: GenServer.start_link(__MODULE__, {ctx, spec})

  @impl true
  def init({ctx, spec}) do
    Process.flag(:trap_exit, true)

    # A replaced child's heartbeats must not vouch for its successor.
    :ets.delete(ctx.table, {:hb, spec.id})

    spawn_count = :ets.update_counter(ctx.table, {:count, spec.id}, 1, {{:count, spec.id}, 0})
    manual? = :ets.take(ctx.table, {:manual, spec.id}) != []

    exe =
      System.find_executable(hd(spec.cmd)) ||
        raise ArgumentError, "executable not found: #{hd(spec.cmd)}"

    port =
      Port.open({:spawn_executable, exe}, [
        :binary,
        :exit_status,
        args: tl(spec.cmd),
        env: [
          {~c"OTP_RAILS_SOCK", String.to_charlist(ctx.socket_path)},
          {~c"OTP_RAILS_TOKEN", String.to_charlist(ctx.token)}
        ]
      ])

    os_pid =
      case Port.info(port, :os_pid) do
        {:os_pid, pid} -> pid
        nil -> nil
      end

    # A crash-driven respawn is a restart in §6 terms; a control-command
    # replacement (manual flag) emits only drain + spawn, like the Ruby
    # supervisor's restart!. Native OTP restarts immediately: backoff_ms is 0.
    if spawn_count > 1 and not manual? do
      Telemetry.emit([:otp_rails, :child, :restart], %{backoff_ms: 0}, %{
        id: spec.id,
        attempt: spawn_count - 1,
        strategy: ctx.strategy
      })
    end

    Telemetry.emit([:otp_rails, :child, :spawn], %{}, %{id: spec.id, pid: os_pid})
    # No probes in step 1: like the Ruby :command adapter without a probe,
    # a live OS process is healthy.
    Telemetry.emit([:otp_rails, :child, :healthy], %{}, %{id: spec.id})

    state = %{
      ctx: ctx,
      spec: spec,
      port: port,
      os_pid: os_pid,
      started_at: System.monotonic_time(:millisecond),
      degraded: 0,
      exited: false
    }

    schedule_tick(spec)
    {:ok, state}
  end

  @impl true
  def handle_call(:os_pid, _from, state), do: {:reply, state.os_pid, state}

  @impl true
  def handle_info(:tick, %{exited: true} = state), do: {:noreply, state}

  def handle_info(:tick, state) do
    %{ctx: ctx, spec: spec} = state

    case effective_health(ctx, spec) do
      :dead ->
        state = drain(state)
        emit_exit(state, nil)
        {:stop, {:shutdown, :heartbeat_dead}, state}

      :degraded ->
        n = state.degraded + 1
        Telemetry.emit([:otp_rails, :child, :degraded], %{consecutive: n}, %{id: spec.id})
        schedule_tick(spec)
        {:noreply, %{state | degraded: n}}

      :healthy ->
        schedule_tick(spec)
        {:noreply, %{state | degraded: 0}}

      _starting_or_passive ->
        schedule_tick(spec)
        {:noreply, state}
    end
  end

  def handle_info({port, {:exit_status, code}}, %{port: port} = state) do
    state = %{state | exited: true}
    emit_exit(state, code)
    reason = if code == 0, do: :normal, else: {:exit_status, code}
    {:stop, reason, state}
  end

  def handle_info({port, {:data, _out}}, %{port: port} = state) do
    # Child stdout/stderr is not the supervisor's business in step 1.
    {:noreply, state}
  end

  def handle_info({:EXIT, port, _reason}, %{port: port} = state), do: {:noreply, state}
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Deliberate shutdown (tree stop or control restart): drain without
    # emitting child.exit, mirroring the Ruby @stopping suppression.
    _ = if not state.exited, do: drain(state)
    :ok
  end

  # DESIGN §5 / Ruby effective_health: heartbeat-active children are judged
  # by freshness + reported state; passive children by OS aliveness (which
  # the Port reports asynchronously, so the tick has nothing to do).
  defp effective_health(ctx, spec) do
    case :ets.lookup(ctx.table, {:hb, spec.id}) do
      [] ->
        :passive

      [{_key, at_ms, hb_state}] ->
        missed = (System.monotonic_time(:millisecond) - at_ms) / spec.health_interval_ms

        cond do
          missed >= 6 -> :dead
          missed >= 3 -> :degraded
          true -> Map.get(@hb_states, hb_state, :healthy)
        end
    end
  end

  defp drain(%{exited: true} = state), do: state

  defp drain(state) do
    %{spec: spec, port: port, os_pid: os_pid} = state
    Telemetry.emit([:otp_rails, :child, :drain], %{}, %{id: spec.id})
    signal(os_pid, "TERM")

    case await_exit(port, spec.shutdown_ms) do
      {:ok, _code} ->
        %{state | exited: true}

      :timeout ->
        Telemetry.emit([:otp_rails, :child, :kill], %{}, %{id: spec.id})
        signal(os_pid, "KILL")

        case await_exit(port, 2_000) do
          {:ok, _code} -> :ok
          :timeout -> if port_alive?(port), do: Port.close(port)
        end

        %{state | exited: true}
    end
  end

  defp await_exit(port, timeout) do
    receive do
      {^port, {:exit_status, code}} -> {:ok, code}
    after
      timeout -> :timeout
    end
  end

  defp emit_exit(state, code) do
    uptime_ms = System.monotonic_time(:millisecond) - state.started_at

    Telemetry.emit([:otp_rails, :child, :exit], %{exit_code: code, uptime_ms: uptime_ms}, %{
      id: state.spec.id
    })
  end

  defp signal(nil, _sig), do: :ok

  defp signal(os_pid, sig) do
    _ = System.cmd("kill", ["-" <> sig, Integer.to_string(os_pid)], stderr_to_stdout: true)
    :ok
  end

  defp port_alive?(port), do: Port.info(port) != nil

  defp schedule_tick(spec), do: Process.send_after(self(), :tick, spec.health_interval_ms)
end
