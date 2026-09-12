defmodule OtpRailsBeam.ChildrenSupervisor do
  @moduledoc """
  The native OTP supervisor over the external children. Strategy and restart
  intensity are plain OTP: `strategy` (one_for_one | rest_for_one),
  `max_restarts` / `max_seconds`. Exceeding intensity shuts this supervisor
  down, which collapses the root (max_restarts: 0 there) — the platform is the
  final supervisor, per DESIGN §3.3.
  """

  use Supervisor

  alias OtpRailsBeam.Child

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg)

  @impl true
  def init(%{ctx: ctx, specs: specs, strategy: strategy, max_restarts: mr, max_seconds: ms}) do
    children =
      Enum.map(specs, fn spec ->
        %{
          id: spec.id,
          start: {Child, :start_link, [{ctx, spec}]},
          restart: spec.restart,
          # Room for the Child's own TERM → wait(shutdown_ms) → KILL drain.
          shutdown: spec.shutdown_ms + 3_000
        }
      end)

    Supervisor.init(children, strategy: strategy, max_restarts: mr, max_seconds: ms)
  end
end
