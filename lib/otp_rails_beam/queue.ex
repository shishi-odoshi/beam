defmodule OtpRailsBeam.Queue do
  @moduledoc """
  A Solid Queue worker on the BEAM (otp-rails Phase 4 step 2): consumes the
  same Postgres tables a Rails app's Solid Queue writes, executing ONLY jobs
  routed to designated queue(s) through registered Elixir handlers. Ruby
  workers keep every other queue, and both kinds of worker — plus Solid
  Queue's own supervisor — run concurrently against the same schema.

      {:ok, queue} =
        OtpRailsBeam.Queue.start_link(
          db: [
            hostname: "localhost",
            port: 5432,
            username: "app",
            password: "...",
            database: "app_production"   # the queue database Solid Queue uses
          ],
          queues: ["elixir"],
          handlers: %{"HardJob" => MyApp.HardJob}
        )

  Options:

  * `:db` (required) — `Postgrex.start_link/1` options for the Solid Queue
    database (Postgres only, v1).
  * `:queues` — exact queue names to work, default `["elixir"]`. Wildcards
    are rejected on purpose: this worker only ever executes jobs explicitly
    routed to it. **The mirror image is on you**: every RUBY worker's
    `config/queue.yml` must exclude these queues — a stock `queues: "*"`
    Ruby worker polls everything and will silently race beam for
    designated-queue jobs, executing them as plain Ruby jobs (whoever polls
    first wins; no error anywhere). Solid Queue has no exclusion syntax, so
    enumerate the Ruby side explicitly (e.g. `queues: [default, mailers]`).
    See the README warning and beam#10.
  * `:handlers` (required) — map of ActiveJob `class_name` to a module
    implementing `OtpRailsBeam.Queue.Handler`.
  * `:batch_size` — max executions claimed per poll (default 3, the analog
    of Solid Queue's worker thread count).
  * `:polling_interval_ms` — default 100 (Solid Queue's 0.1s).
  * `:heartbeat_interval_ms` — default 60_000
    (`SolidQueue.process_heartbeat_interval`).
  * `:pool_size` — Postgrex pool size, default 2 (worker + heartbeat).
  * `:name` — supervisor name, default `OtpRailsBeam.Queue`.

  The tree is `rest_for_one`: DB pool → process registration (heartbeat) →
  worker loop, so a lost registration (e.g. Solid Queue's supervisor pruned
  our process row) restarts the worker with a fresh `process_id` too.
  """

  use Supervisor

  alias OtpRailsBeam.Queue.{Registration, Worker}

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, Keyword.put(opts, :name, name), name: name)
  end

  @doc "The `solid_queue_processes.id` this queue's executions are claimed under."
  def process_id(name \\ __MODULE__), do: Registration.process_id(registration_name(name))

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    db_opts = Keyword.fetch!(opts, :db)
    queues = opts |> Keyword.get(:queues, ["elixir"]) |> validate_queues!()
    handlers = opts |> Keyword.fetch!(:handlers) |> validate_handlers!()

    db_name = :"#{name}.DB"
    polling_interval_ms = Keyword.get(opts, :polling_interval_ms, 100)

    db_spec = %{
      id: :db,
      start:
        {Postgrex, :start_link,
         [
           Keyword.merge(db_opts,
             name: db_name,
             pool_size: Keyword.get(opts, :pool_size, 2)
           )
         ]}
    }

    registration_spec = %{
      id: :registration,
      start:
        {Registration, :start_link,
         [
           [
             name: registration_name(name),
             db: db_name,
             heartbeat_interval_ms: Keyword.get(opts, :heartbeat_interval_ms, 60_000),
             metadata: %{
               queues: Enum.join(queues, ","),
               polling_interval: polling_interval_ms / 1000,
               runtime: "beam (otp_rails_beam)"
             }
           ]
         ]},
      # Give deregistration (release claimed + delete process row) time to run.
      shutdown: 10_000
    }

    worker_spec = %{
      id: :worker,
      start:
        {Worker, :start_link,
         [
           [
             name: :"#{name}.Worker",
             db: db_name,
             registration: registration_name(name),
             queues: queues,
             handlers: handlers,
             batch_size: Keyword.get(opts, :batch_size, 3),
             polling_interval_ms: polling_interval_ms
           ]
         ]},
      # An in-flight handler gets this long to finish before the job is
      # released back to ready by Registration's deregister.
      shutdown: Keyword.get(opts, :drain_timeout_ms, 30_000)
    }

    Supervisor.init([db_spec, registration_spec, worker_spec],
      strategy: :rest_for_one,
      max_restarts: Keyword.get(opts, :max_restarts, 10),
      max_seconds: Keyword.get(opts, :max_seconds, 60)
    )
  end

  defp registration_name(name), do: :"#{name}.Registration"

  defp validate_queues!(queues) do
    with true <- is_list(queues) and queues != [],
         true <-
           Enum.all?(queues, &(is_binary(&1) and &1 != "" and not String.contains?(&1, "*"))) do
      queues
    else
      _ ->
        raise ArgumentError,
              ":queues must be a non-empty list of exact queue names (no wildcards — " <>
                "this worker only executes jobs explicitly routed to it), got: #{inspect(queues)}"
    end
  end

  defp validate_handlers!(handlers) do
    with true <- is_map(handlers) and map_size(handlers) > 0,
         true <-
           Enum.all?(handlers, fn {class, module} -> is_binary(class) and is_atom(module) end) do
      handlers
    else
      _ ->
        raise ArgumentError,
              ":handlers must be a non-empty map of ActiveJob class name => handler module, " <>
                "got: #{inspect(handlers)}"
    end
  end
end
