defmodule OtpRailsBeam.Queue.Registration do
  @moduledoc """
  Owns this worker's `solid_queue_processes` row: registers it on init,
  touches `last_heartbeat_at` every `:heartbeat_interval_ms` (default 60s —
  `SolidQueue.process_heartbeat_interval`), and deregisters on clean
  shutdown, releasing any still-claimed executions back to ready.

  The heartbeat is what keeps Solid Queue's supervisor from pruning us: its
  maintenance task fails the claimed executions of any process whose
  heartbeat is older than `SolidQueue.process_alive_threshold` (default 5
  minutes) and deletes the process row. If that happens while we're alive
  (our row vanished under us), this GenServer stops abnormally so the queue
  supervisor re-registers a fresh process — the same recovery a supervised
  Ruby worker gets by exiting and being re-forked.
  """

  use GenServer
  require Logger

  alias OtpRailsBeam.Queue.Store

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:name])

  @doc "The `solid_queue_processes.id` claimed executions are attributed to."
  def process_id(server), do: GenServer.call(server, :process_id)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    db = Keyword.fetch!(opts, :db)
    interval = Keyword.get(opts, :heartbeat_interval_ms, 60_000)

    # Mirror SolidQueue::Processes::Base: name "worker-<hex(10)>", OS pid,
    # hostname, and informational metadata.
    attrs = %{
      name: "worker-" <> Base.encode16(:crypto.strong_rand_bytes(10), case: :lower),
      pid: String.to_integer(System.pid()),
      hostname: hostname(),
      metadata: Keyword.get(opts, :metadata, %{})
    }

    {:ok, id} = Store.register_process(db, attrs)
    Process.send_after(self(), :heartbeat, interval)

    {:ok, %{db: db, id: id, name: attrs.name, interval: interval, pruned: false}}
  end

  @impl true
  def handle_call(:process_id, _from, state), do: {:reply, state.id, state}

  @impl true
  def handle_info(:heartbeat, state) do
    case safe_heartbeat(state) do
      :ok ->
        Process.send_after(self(), :heartbeat, state.interval)
        {:noreply, state}

      :pruned ->
        Logger.warning(
          "otp_rails_beam.queue: process row #{state.id} (#{state.name}) was pruned by " <>
            "Solid Queue's supervisor; restarting to re-register"
        )

        {:stop, :process_pruned, %{state | pruned: true}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Clean shutdown mirrors SolidQueue::Process#deregister. If we were
    # pruned the row (and our claimed executions) are already gone.
    unless state.pruned do
      try do
        Store.deregister_process(state.db, state.id)
      catch
        kind, reason ->
          Logger.warning(
            "otp_rails_beam.queue: deregister failed: #{Exception.format_banner(kind, reason)}"
          )
      end
    end

    :ok
  end

  # A transient DB error must not look like a prune: Solid Queue only
  # punishes staleness after process_alive_threshold (5 minutes), so we log
  # and try again next interval — same as Ruby's TimerTask observer.
  defp safe_heartbeat(state) do
    Store.heartbeat(state.db, state.id)
  catch
    kind, reason ->
      Logger.warning(
        "otp_rails_beam.queue: heartbeat failed (will retry): " <>
          Exception.format_banner(kind, reason)
      )

      :ok
  end

  defp hostname do
    {:ok, host} = :inet.gethostname()
    List.to_string(host)
  end
end
