defmodule OtpRailsBeam do
  @moduledoc """
  Elixir sidecar supervisor for Rails (or arbitrary) processes, speaking the
  otp-rails DESIGN §5 health protocol and emitting the §6 telemetry events.

      {:ok, sup} =
        OtpRailsBeam.start_link(
          socket_path: "tmp/otp-rails.sock",
          strategy: :one_for_one,
          max_restarts: 5,
          max_seconds: 60,
          children: [
            %{id: "web",  cmd: ["bin/rails", "server"], shutdown_ms: 30_000},
            %{id: "jobs", cmd: ["bin/jobs"], health_interval_ms: 5_000}
          ]
        )

  Children inherit `OTP_RAILS_SOCK` / `OTP_RAILS_TOKEN` and may send NDJSON
  heartbeats; `{"cmd":"restart","id":...,"token":...}` on the same socket
  replaces a child. See the README Contract section.
  """

  alias OtpRailsBeam.{Root, Telemetry}

  @doc """
  Start a supervision tree. Options:

  * `:socket_path` (required) — Unix socket path for §5 heartbeats/control.
  * `:children` (required) — list of `OtpRailsBeam.ChildSpec` attrs.
  * `:strategy` — `:one_for_one` (default) or `:rest_for_one`.
  * `:max_restarts` / `:max_seconds` — native OTP restart intensity.
  """
  def start_link(opts) do
    case Root.start_link(opts) do
      {:ok, pid} ->
        watch_lifecycle(pid)
        {:ok, pid}

      other ->
        other
    end
  end

  @doc "Stop the tree cleanly (drains children in reverse start order)."
  def stop(sup, timeout \\ :infinity), do: Supervisor.stop(sup, :normal, timeout)

  @doc "The per-boot heartbeat token (exported to children as OTP_RAILS_TOKEN)."
  def token(sup), do: ctx(sup).token

  @doc "Gracefully replace one child, same as the socket restart command."
  def restart_child(sup, id), do: Root.restart_child(ctx(sup), id)

  @doc "Debug/test hook: has this child's heartbeat been recorded?"
  def heartbeated?(sup, id), do: :ets.lookup(ctx(sup).table, {:hb, id}) != []

  @doc "Debug/test hook: the child's current OS pid (nil if not running)."
  def child_os_pid(sup, id) do
    with sup_pid when is_pid(sup_pid) <- Root.children_sup(sup),
         {_id, pid, _type, _mods} when is_pid(pid) <-
           sup_pid |> Supervisor.which_children() |> Enum.find(fn {i, _, _, _} -> i == id end) do
      GenServer.call(pid, :os_pid)
    else
      _ -> nil
    end
  end

  defp ctx(sup) do
    sup |> Root.socket_server() |> GenServer.call(:ctx)
  end

  # Native OTP supervisors expose no hook at the moment intensity is exceeded;
  # observationally, a supervisor that gives up exits with reason :shutdown
  # while a clean Supervisor.stop/3 exits :normal. An external monitor turns
  # that into the §6 escalate/stop events.
  defp watch_lifecycle(pid) do
    spawn(fn ->
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, reason} ->
          if reason == :shutdown do
            Telemetry.emit([:otp_rails, :supervisor, :escalate], %{}, %{})
          end

          Telemetry.emit([:otp_rails, :supervisor, :stop], %{}, %{})
      end
    end)
  end
end
