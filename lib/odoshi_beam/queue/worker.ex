defmodule OdoshiBeam.Queue.Worker do
  @moduledoc """
  The poll → claim → execute loop, mirroring `SolidQueue::Worker`:

  * polls the designated queues on `:polling_interval_ms`, claiming up to
    `:batch_size` executions per poll with `ReadyExecution.claim` semantics
    (see `OdoshiBeam.Queue.Store.claim/4`);
  * executes each claimed job through its registered
    `OdoshiBeam.Queue.Handler` (v1 runs jobs sequentially in this process —
    the claim batch is the analog of the Ruby pool's available capacity);
  * finishes or fails each execution exactly like `ClaimedExecution#perform`.

  Emits beam-local telemetry (never the frozen §6 contract events):

      [:odoshi_beam, :job, :start]    %{system_time}   %{job_id, active_job_id, class_name, queue_name}
      [:odoshi_beam, :job, :finish]   %{duration_ms}   same metadata
      [:odoshi_beam, :job, :failure]  %{duration_ms}   metadata + %{error: %{exception_class, message, backtrace}}
  """

  use GenServer
  require Logger

  alias OdoshiBeam.Queue.{ActiveJob, Registration, Store}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:name])

  @impl true
  def init(opts) do
    state = %{
      db: Keyword.fetch!(opts, :db),
      registration: Keyword.fetch!(opts, :registration),
      queues: Keyword.fetch!(opts, :queues),
      handlers: Keyword.fetch!(opts, :handlers),
      batch_size: Keyword.get(opts, :batch_size, 3),
      polling_interval_ms: Keyword.get(opts, :polling_interval_ms, 100)
    }

    {:ok, state, {:continue, :poll}}
  end

  @impl true
  def handle_continue(:poll, state), do: {:noreply, poll(state)}

  @impl true
  def handle_info(:poll, state), do: {:noreply, poll(state)}
  def handle_info(_msg, state), do: {:noreply, state}

  defp poll(state) do
    process_id = Registration.process_id(state.registration)

    case try_claim(state, process_id) do
      {:ok, []} ->
        Process.send_after(self(), :poll, state.polling_interval_ms)

      {:ok, claimed} ->
        Enum.each(claimed, &execute(&1, state))
        send(self(), :poll)

      # A DB blip must not burn through the supervisor's restart intensity at
      # polling speed; back off one interval and try again. Anything that
      # isn't a connection error crashes (crash-only).
      {:error, %DBConnection.ConnectionError{} = err} ->
        Logger.warning("odoshi_beam.queue: claim failed (will retry): #{err.message}")
        Process.send_after(self(), :poll, state.polling_interval_ms * 10)
    end

    state
  end

  defp try_claim(state, process_id) do
    {:ok, Store.claim(state.db, state.queues, state.batch_size, process_id)}
  rescue
    err in [DBConnection.ConnectionError] -> {:error, err}
  end

  defp execute(%{claimed_id: claimed_id, job_id: job_id}, state) do
    case Store.fetch_job(state.db, job_id) do
      {:ok, job} ->
        meta = %{
          job_id: job_id,
          active_job_id: job.active_job_id,
          class_name: job.class_name,
          queue_name: job.queue_name
        }

        :telemetry.execute(
          [:odoshi_beam, :job, :start],
          %{system_time: System.system_time()},
          meta
        )

        started = System.monotonic_time(:millisecond)
        result = decode_and_run(job, state.handlers)
        duration_ms = System.monotonic_time(:millisecond) - started

        case result do
          :ok ->
            Store.finish(state.db, claimed_id, job_id)

            :telemetry.execute(
              [:odoshi_beam, :job, :finish],
              %{duration_ms: duration_ms},
              meta
            )

          {:failed, error} ->
            Store.fail(state.db, claimed_id, job_id, error)

            :telemetry.execute(
              [:odoshi_beam, :job, :failure],
              %{duration_ms: duration_ms},
              Map.put(meta, :error, error)
            )
        end

      # FK-impossible in practice, but never crash the loop over a vanished job.
      :not_found ->
        Logger.warning("odoshi_beam.queue: claimed job #{job_id} no longer exists")

        Store.fail(state.db, claimed_id, job_id, %{
          exception_class: "OdoshiBeam.Queue.MissingJobError",
          message: "solid_queue_jobs row #{job_id} not found for claimed execution",
          backtrace: []
        })
    end
  end

  defp decode_and_run(job, handlers) do
    case ActiveJob.decode(job.arguments) do
      {:ok, %{job_class: job_class, arguments: args}} ->
        case Map.fetch(handlers, job_class) do
          {:ok, module} ->
            run_handler(module, args)

          :error ->
            # This queue was designated for Elixir execution, so an
            # unregistered class is a routing/config bug: fail loudly where
            # Solid Queue's tooling can see (and retry) it, instead of
            # claiming it forever or silently skipping it.
            {:failed,
             %{
               exception_class: "OdoshiBeam.Queue.UnknownJobClassError",
               message:
                 "no Elixir handler registered for ActiveJob class #{inspect(job_class)} " <>
                   "on queue #{inspect(job.queue_name)}",
               backtrace: []
             }}
        end

      {:error, message} ->
        {:failed,
         %{
           exception_class: "OdoshiBeam.Queue.DeserializationError",
           message: message,
           backtrace: []
         }}
    end
  end

  defp run_handler(module, args) do
    case module.perform(args) do
      :ok ->
        :ok

      {:error, reason} ->
        {:failed,
         %{
           exception_class: "OdoshiBeam.Queue.HandlerError",
           message: "handler #{inspect(module)} returned {:error, #{inspect(reason)}}",
           backtrace: []
         }}

      other ->
        {:failed,
         %{
           exception_class: "OdoshiBeam.Queue.HandlerError",
           message:
             "handler #{inspect(module)} returned #{inspect(other)} " <>
               "(expected :ok or {:error, term})",
           backtrace: []
         }}
    end
  rescue
    e ->
      {:failed,
       %{
         exception_class: exception_class(e),
         message: Exception.message(e),
         backtrace: backtrace(__STACKTRACE__)
       }}
  catch
    kind, reason ->
      {:failed,
       %{
         exception_class: "OdoshiBeam.Queue.HandlerExit",
         message: Exception.format_banner(kind, reason),
         backtrace: backtrace(__STACKTRACE__)
       }}
  end

  # "ArgumentError" / "MyApp.SomeError" — inspect/1 drops the Elixir. prefix,
  # giving the Ruby side a readable class name in FailedExecution#exception_class.
  defp exception_class(%module{}), do: inspect(module)

  defp backtrace(stacktrace) do
    Enum.map(stacktrace, &Exception.format_stacktrace_entry/1)
  end
end
