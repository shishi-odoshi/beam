defmodule OdoshiBeam.Queue.Store do
  @moduledoc """
  Hand-rolled SQL mirror of Solid Queue's worker-side semantics (gem version
  1.7.x is the authority — `app/models/solid_queue/{ready_execution,
  claimed_execution,process}.rb`). Ruby workers and Solid Queue's supervisor
  run concurrently against the same tables, so every statement here matches
  the Ruby implementation's locking and transaction boundaries:

  * **claim** — `ReadyExecution.claim`: per queue (in configured order, minus
    paused queues), `SELECT id, job_id ... ORDER BY priority ASC, job_id ASC
    LIMIT n FOR UPDATE SKIP LOCKED`, then the `solid_queue_claimed_executions`
    insert and the ready-row delete inside the same transaction.
  * **finish** — `ClaimedExecution#finished`: lock the claimed row
    (plain `FOR UPDATE`; skip silently if it's gone — someone else finalized
    or pruned it), set `solid_queue_jobs.finished_at`, delete the claimed row.
  * **fail** — `ClaimedExecution#failed_with`: same finalize dance, inserting
    a `solid_queue_failed_executions` row whose `error` column is the JSON
    Solid Queue writes (`exception_class` / `message` / `backtrace`), with
    the `ON CONFLICT` upsert mirroring the Ruby `RecordNotUnique` rescue.
  * **register/heartbeat/deregister** — `SolidQueue::Process`: workers
    register with kind `"Worker"` and touch `last_heartbeat_at` on the same
    cadence Ruby workers do, so Solid Queue's supervisor prunes a dead beam
    worker exactly like a dead Ruby one — and never a live one. Deregistering
    releases still-claimed executions back to ready (the Ruby `after_destroy`
    callback).

  Not mirrored (documented v1 limits): concurrency-control semaphore release
  (`job.unblock_next_blocked_job` — expired semaphores are recovered by the
  Ruby dispatcher's concurrency maintenance), batch progress callbacks, and
  `preserve_finished_jobs = false` (finished jobs are always preserved).
  """

  @typedoc "A claimed execution: the claimed row id plus its job id."
  @type claimed :: %{claimed_id: integer(), job_id: integer()}

  ## Process registry (solid_queue_processes)

  @doc """
  Insert this worker's `solid_queue_processes` row (kind "Worker", like
  `SolidQueue::Process.register`). Returns `{:ok, process_id}`.
  """
  def register_process(db, attrs) do
    now = utc_now()

    %{rows: [[id]]} =
      Postgrex.query!(
        db,
        """
        INSERT INTO solid_queue_processes
          (kind, last_heartbeat_at, supervisor_id, pid, hostname, metadata, created_at, name)
        VALUES ('Worker', $1, NULL, $2, $3, $4, $1, $5)
        RETURNING id
        """,
        [now, attrs.pid, attrs.hostname, Jason.encode!(attrs.metadata), attrs.name]
      )

    {:ok, id}
  end

  @doc """
  Touch `last_heartbeat_at` (the atomic equivalent of Ruby's
  `with_lock { touch(:last_heartbeat_at) }`). Returns `:ok`, or `:pruned`
  when the row no longer exists — Solid Queue's supervisor pruned us and
  failed our claimed executions, so the caller must re-register, not carry on.
  """
  def heartbeat(db, process_id) do
    case Postgrex.query!(
           db,
           "UPDATE solid_queue_processes SET last_heartbeat_at = $1 WHERE id = $2",
           [utc_now(), process_id]
         ) do
      %{num_rows: 1} -> :ok
      %{num_rows: 0} -> :pruned
    end
  end

  @doc """
  Delete our process row, first releasing any still-claimed executions back
  to ready — mirroring `SolidQueue::Process#deregister` and its
  `after_destroy :release_all_claimed_executions` callback
  (`ClaimedExecution#release` → ready `create_or_find_by` + claimed delete).
  """
  def deregister_process(db, process_id) do
    {:ok, _} =
      Postgrex.transaction(db, fn conn ->
        Postgrex.query!(
          conn,
          """
          INSERT INTO solid_queue_ready_executions (job_id, queue_name, priority, created_at)
          SELECT j.id, j.queue_name, j.priority, $2
          FROM solid_queue_claimed_executions ce
          JOIN solid_queue_jobs j ON j.id = ce.job_id
          WHERE ce.process_id = $1
          ON CONFLICT (job_id) DO NOTHING
          """,
          [process_id, utc_now()]
        )

        Postgrex.query!(
          conn,
          "DELETE FROM solid_queue_claimed_executions WHERE process_id = $1",
          [process_id]
        )

        Postgrex.query!(conn, "DELETE FROM solid_queue_processes WHERE id = $1", [process_id])
      end)

    :ok
  end

  ## Claiming (solid_queue_ready_executions -> solid_queue_claimed_executions)

  @doc """
  Claim up to `limit` ready executions from `queues` (exact names, in order),
  skipping paused queues — `SolidQueue::ReadyExecution.claim` semantics.
  """
  @spec claim(pid() | atom(), [binary()], pos_integer(), integer()) :: [claimed()]
  def claim(db, queues, limit, process_id) do
    paused = paused_queues(db)

    queues
    |> Enum.reject(&(&1 in paused))
    |> Enum.reduce({[], limit}, fn queue, {acc, remaining} ->
      if remaining <= 0 do
        {acc, remaining}
      else
        rows = claim_from_queue(db, queue, remaining, process_id)
        {acc ++ rows, remaining - length(rows)}
      end
    end)
    |> elem(0)
  end

  defp claim_from_queue(db, queue, limit, process_id) do
    {:ok, rows} =
      Postgrex.transaction(db, fn conn ->
        # select_candidates: executed eagerly, FOR UPDATE SKIP LOCKED so
        # concurrent Ruby workers claiming the same queue never block or
        # double-claim.
        %{rows: candidates} =
          Postgrex.query!(
            conn,
            """
            SELECT id, job_id FROM solid_queue_ready_executions
            WHERE queue_name = $1
            ORDER BY priority ASC, job_id ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            """,
            [queue, limit]
          )

        case candidates do
          [] ->
            []

          _ ->
            ready_ids = Enum.map(candidates, fn [id, _job_id] -> id end)
            job_ids = Enum.map(candidates, fn [_id, job_id] -> job_id end)

            # ClaimedExecution.claiming: insert claimed rows, then delete the
            # ready rows, all inside the claim transaction.
            %{rows: claimed} =
              Postgrex.query!(
                conn,
                """
                INSERT INTO solid_queue_claimed_executions (job_id, process_id, created_at)
                SELECT u, $2, $3 FROM unnest($1::bigint[]) AS u
                RETURNING id, job_id
                """,
                [job_ids, process_id, utc_now()]
              )

            Postgrex.query!(
              conn,
              "DELETE FROM solid_queue_ready_executions WHERE id = ANY($1)",
              [ready_ids]
            )

            Enum.map(claimed, fn [cid, jid] -> %{claimed_id: cid, job_id: jid} end)
        end
      end)

    rows
  end

  @doc "Queue names currently paused (`solid_queue_pauses`), checked per poll like `QueueSelector`."
  def paused_queues(db) do
    %{rows: rows} = Postgrex.query!(db, "SELECT queue_name FROM solid_queue_pauses", [])
    Enum.map(rows, fn [name] -> name end)
  end

  ## Job rows

  @doc "Fetch the job row backing a claimed execution."
  def fetch_job(db, job_id) do
    case Postgrex.query!(
           db,
           """
           SELECT class_name, arguments, queue_name, active_job_id
           FROM solid_queue_jobs WHERE id = $1
           """,
           [job_id]
         ) do
      %{rows: [[class_name, arguments, queue_name, active_job_id]]} ->
        {:ok,
         %{
           id: job_id,
           class_name: class_name,
           arguments: arguments,
           queue_name: queue_name,
           active_job_id: active_job_id
         }}

      %{rows: []} ->
        :not_found
    end
  end

  ## Finalization (ClaimedExecution#finished / #failed_with)

  @doc """
  Mark a job finished: lock the claimed row, set `finished_at`, delete the
  claimed row. Returns `:ok`, or `:already_finalized` when the claimed row is
  gone (pruned or finalized by another actor) — in which case nothing is
  written, exactly like Ruby's `unless_already_finalized`.
  """
  def finish(db, claimed_id, job_id) do
    finalize(db, claimed_id, fn conn ->
      now = utc_now()

      Postgrex.query!(
        conn,
        "UPDATE solid_queue_jobs SET finished_at = $1, updated_at = $1 WHERE id = $2",
        [now, job_id]
      )
    end)
  end

  @doc """
  Record a failure: lock the claimed row, insert the
  `solid_queue_failed_executions` row, delete the claimed row. `error` is a
  map with `:exception_class`, `:message` and `:backtrace` (list of strings),
  stored as the same JSON Solid Queue's `FailedExecution` serializes and
  reads back (`exception_class` / `message` / `backtrace` accessors).
  """
  def fail(db, claimed_id, job_id, error) do
    error_json =
      Jason.encode!(%{
        exception_class: error.exception_class,
        message: error.message,
        backtrace: error.backtrace
      })

    finalize(db, claimed_id, fn conn ->
      Postgrex.query!(
        conn,
        """
        INSERT INTO solid_queue_failed_executions (job_id, error, created_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (job_id) DO UPDATE SET error = EXCLUDED.error
        """,
        [job_id, error_json, utc_now()]
      )
    end)
  end

  defp finalize(db, claimed_id, fun) do
    {:ok, result} =
      Postgrex.transaction(db, fn conn ->
        %{rows: locked} =
          Postgrex.query!(
            conn,
            "SELECT id FROM solid_queue_claimed_executions WHERE id = $1 FOR UPDATE",
            [claimed_id]
          )

        case locked do
          [] ->
            :already_finalized

          [_] ->
            fun.(conn)

            Postgrex.query!(
              conn,
              "DELETE FROM solid_queue_claimed_executions WHERE id = $1",
              [claimed_id]
            )

            :ok
        end
      end)

    result
  end

  # Rails writes UTC wall-clock values into these timestamp-without-time-zone
  # columns; NaiveDateTime.utc_now() is the same thing.
  defp utc_now, do: NaiveDateTime.utc_now()
end
