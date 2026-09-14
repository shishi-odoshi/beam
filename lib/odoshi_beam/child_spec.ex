defmodule OdoshiBeam.ChildSpec do
  @moduledoc """
  DESIGN §3.2, trimmed to Phase 4 step 1: a child is an external OS command.

  * `id` — string, unique within the tree; this is the id children echo in
    heartbeats and the id `{"cmd":"restart"}` targets.
  * `cmd` — argv list, e.g. `["ruby", "worker.rb"]`. The first element is
    resolved with `System.find_executable/1` at spawn time.
  * `restart` — `:permanent` (default) | `:transient` | `:temporary`,
    mapped straight onto native OTP restart values.
  * `shutdown_ms` — drain budget: SIGTERM, wait this long, then SIGKILL.
  * `health_interval_ms` — heartbeat aging tick (DESIGN §5): 3 missed
    intervals ⇒ degraded, 6 ⇒ dead.
  """

  @enforce_keys [:id, :cmd]
  defstruct [:id, :cmd, restart: :permanent, shutdown_ms: 30_000, health_interval_ms: 5_000]

  @restart_kinds [:permanent, :transient, :temporary]

  def new(%__MODULE__{} = spec), do: validate(spec)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    validate(struct!(__MODULE__, attrs))
  end

  defp validate(%__MODULE__{} = spec) do
    if not is_binary(spec.id) or spec.id == "" do
      raise ArgumentError, "child id must be a non-empty string, got: #{inspect(spec.id)}"
    end

    if not (is_list(spec.cmd) and spec.cmd != [] and Enum.all?(spec.cmd, &is_binary/1)) do
      raise ArgumentError,
            "cmd must be a non-empty argv list of strings, got: #{inspect(spec.cmd)}"
    end

    if spec.restart not in @restart_kinds do
      raise ArgumentError, "restart must be one of #{inspect(@restart_kinds)}"
    end

    if not (is_integer(spec.shutdown_ms) and spec.shutdown_ms > 0) do
      raise ArgumentError, "shutdown_ms must be a positive integer"
    end

    if not (is_integer(spec.health_interval_ms) and spec.health_interval_ms > 0) do
      raise ArgumentError, "health_interval_ms must be a positive integer"
    end

    spec
  end
end
