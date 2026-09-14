defmodule OdoshiBeam.TestEvents do
  @moduledoc """
  Telemetry capture for tests: attaches to every §6 event and accumulates them
  in an Agent, so assertions can poll instead of racing the mailbox.
  """

  def attach do
    # Unlinked: on_exit callbacks run after the test process (and anything
    # linked to it) is gone, and detach/1 still needs the agent then.
    {:ok, agent} = Agent.start(fn -> [] end)
    handler_id = "test-events-#{inspect(agent)}"

    :ok =
      :telemetry.attach_many(
        handler_id,
        OdoshiBeam.Telemetry.events(),
        &__MODULE__.handle/4,
        %{agent: agent}
      )

    {agent, handler_id}
  end

  def detach({agent, handler_id}) do
    :telemetry.detach(handler_id)
    Agent.stop(agent)
  end

  def handle(event, measurements, metadata, %{agent: agent}) do
    entry = %{event: event, measurements: measurements, metadata: metadata}
    Agent.update(agent, &[entry | &1])
  end

  def all(agent), do: agent |> Agent.get(& &1) |> Enum.reverse()

  def named(agent, event_suffix, id \\ nil) do
    Enum.filter(all(agent), fn e ->
      List.last(e.event) == event_suffix and (id == nil or e.metadata[:id] == id)
    end)
  end
end
