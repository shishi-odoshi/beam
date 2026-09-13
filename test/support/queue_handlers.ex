defmodule OtpRailsBeam.QueueHandlers do
  @moduledoc "Elixir handlers backing the fixture ActiveJob classes in tests."

  defmodule Marker do
    @moduledoc "Elixir twin of the Ruby MarkerJob: appends `beam <id>` to the shared log."
    @behaviour OtpRailsBeam.Queue.Handler

    @impl true
    def perform([log_path, id]) do
      File.write!(log_path, "beam #{id}\n", [:append])
      :ok
    end
  end

  defmodule Failing do
    @moduledoc "Raises, so the job lands in solid_queue_failed_executions."
    @behaviour OtpRailsBeam.Queue.Handler

    @impl true
    def perform(_args) do
      raise ArgumentError, "boom from the beam handler"
    end
  end
end
