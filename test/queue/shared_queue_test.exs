defmodule OdoshiBeam.Queue.SharedQueueTest do
  @moduledoc """
  Interop acceptance suite for the shared Solid Queue (Phase 4 step 2): the
  Ruby side is the REAL solid_queue gem (test/fixtures/solid_queue/), the
  Elixir side is `OdoshiBeam.Queue`, and both run against the same Postgres
  schema. Requires the docker Postgres from the README (`:queue` tag).
  """

  use ExUnit.Case, async: false

  import OdoshiBeam.QueueHelpers

  alias OdoshiBeam.QueueHandlers

  @moduletag :queue
  @moduletag timeout: 120_000

  setup do
    setup_db!()
    {:ok, conn: connect!(), dir: tmp_dir!("shared")}
  end

  defp start_queue!(opts) do
    name = :"queue_under_test_#{System.unique_integer([:positive])}"

    defaults = [
      name: name,
      db: db_opts(),
      queues: ["elixir"],
      handlers: %{
        "MarkerJob" => QueueHandlers.Marker,
        "FailingJob" => QueueHandlers.Failing
      },
      polling_interval_ms: 50,
      heartbeat_interval_ms: 200
    ]

    pid = start_supervised!({OdoshiBeam.Queue, Keyword.merge(defaults, opts)}, id: name)
    {name, pid}
  end

  defp log_lines(log) do
    case File.read(log) do
      {:ok, content} -> content |> String.split("\n", trim: true) |> Enum.sort()
      {:error, :enoent} -> []
    end
  end

  test "beam claims and executes ONLY elixir-queue jobs; heartbeat spares it from pruning",
       %{conn: conn, dir: dir} do
    log = Path.join(dir, "log")
    ruby!("enqueue.rb", ["elixir", "3", "e", log])
    ruby!("enqueue.rb", ["default", "2", "d", log])

    capture_job_events()
    {name, pid} = start_queue!([])

    wait_until(fn -> length(log_lines(log)) == 3 end)
    assert log_lines(log) == ["beam e0", "beam e1", "beam e2"]

    # elixir-queue jobs: finished_at set, ready + claimed rows cleaned.
    wait_until(fn ->
      one!(
        conn,
        "SELECT count(*) FROM solid_queue_jobs WHERE queue_name = 'elixir' AND finished_at IS NOT NULL"
      ) == 3
    end)

    assert one!(
             conn,
             "SELECT count(*) FROM solid_queue_ready_executions WHERE queue_name = 'elixir'"
           ) == 0

    assert one!(conn, "SELECT count(*) FROM solid_queue_claimed_executions") == 0

    # default-queue jobs: untouched — still ready, not finished.
    assert one!(
             conn,
             "SELECT count(*) FROM solid_queue_ready_executions WHERE queue_name = 'default'"
           ) == 2

    assert one!(
             conn,
             "SELECT count(*) FROM solid_queue_jobs WHERE queue_name = 'default' AND finished_at IS NOT NULL"
           ) == 0

    # Registered like a Ruby worker: kind Worker, live heartbeat, our OS pid.
    assert [[kind, reg_pid]] = rows!(conn, "SELECT kind, pid FROM solid_queue_processes")
    assert kind == "Worker"
    assert reg_pid == String.to_integer(System.pid())

    # Solid Queue's own pruning (threshold 2s) must spare a live beam worker:
    # its 200ms heartbeat keeps the row fresh.
    assert ruby!("prune.rb", ["2"]) =~ "PRUNED processes=1"
    assert one!(conn, "SELECT count(*) FROM solid_queue_processes") == 1

    # beam-local telemetry fired for each executed job; §6 names never used.
    assert_receive {:job_event, [:odoshi_beam, :job, :start], _, %{class_name: "MarkerJob"}}
    assert_receive {:job_event, [:odoshi_beam, :job, :finish], %{duration_ms: _}, _}

    # Clean shutdown deregisters (row deleted), like SolidQueue::Process#deregister.
    :ok = stop_supervised(name)
    refute Process.alive?(pid)
    assert one!(conn, "SELECT count(*) FROM solid_queue_processes") == 0
  end

  test "burst: 50 jobs each side, beam + real Ruby worker concurrently, zero cross-claims",
       %{conn: conn, dir: dir} do
    log = Path.join(dir, "log")
    ready = Path.join(dir, "ruby_worker_ready")

    {_name, _pid} = start_queue!(batch_size: 5)

    ruby_worker = Task.async(fn -> ruby("run_worker.rb", ["default", "15", ready]) end)
    wait_until(30_000, fn -> File.exists?(ready) end)

    # Enqueue both bursts while BOTH workers are polling.
    enqueues = [
      Task.async(fn -> ruby("enqueue.rb", ["elixir", "50", "e", log]) end),
      Task.async(fn -> ruby("enqueue.rb", ["default", "50", "d", log]) end)
    ]

    for task <- enqueues do
      assert {_out, 0} = Task.await(task, 60_000)
    end

    wait_until(30_000, fn -> length(log_lines(log)) >= 100 end)
    # Give any hypothetical double-execution a beat to append, then snapshot.
    Process.sleep(500)
    lines = log_lines(log)

    # Exactly one execution per job, each on its designated side.
    assert length(lines) == 100
    assert lines == Enum.sort(Enum.map(0..49, &"beam e#{&1}") ++ Enum.map(0..49, &"ruby d#{&1}"))

    wait_until(fn ->
      one!(conn, "SELECT count(*) FROM solid_queue_jobs WHERE finished_at IS NOT NULL") == 100
    end)

    assert one!(conn, "SELECT count(*) FROM solid_queue_ready_executions") == 0
    assert one!(conn, "SELECT count(*) FROM solid_queue_claimed_executions") == 0
    assert one!(conn, "SELECT count(*) FROM solid_queue_failed_executions") == 0

    assert {out, 0} = Task.await(ruby_worker, 60_000)
    assert out =~ "WORKER_DONE"
  end

  test "a failing Elixir handler produces a failed_executions row the Ruby side loads",
       %{conn: conn, dir: dir} do
    log = Path.join(dir, "log")
    ruby!("enqueue.rb", ["elixir", "1", "f", log, "FailingJob"])

    {_name, _pid} = start_queue!([])

    wait_until(fn ->
      one!(conn, "SELECT count(*) FROM solid_queue_failed_executions") == 1
    end)

    # Well-formed error payload, straight from the DB.
    [[error_json]] = rows!(conn, "SELECT error FROM solid_queue_failed_executions")
    error = Jason.decode!(error_json)
    assert error["exception_class"] == "ArgumentError"
    assert error["message"] == "boom from the beam handler"
    assert [first | _] = error["backtrace"]
    assert first =~ "queue_handlers"

    # Claimed row deleted, job NOT finished (mirrors ClaimedExecution#failed_with).
    assert one!(conn, "SELECT count(*) FROM solid_queue_claimed_executions") == 0
    assert one!(conn, "SELECT count(*) FROM solid_queue_jobs WHERE finished_at IS NOT NULL") == 0

    # And SolidQueue::FailedExecution on the Ruby side reads it back.
    line = "inspect_failed.rb" |> ruby!() |> String.split("\n", trim: true) |> List.first()
    loaded = Jason.decode!(line)
    assert loaded["class_name"] == "FailingJob"
    assert loaded["exception_class"] == "ArgumentError"
    assert loaded["message"] == "boom from the beam handler"
    assert loaded["backtrace_size"] > 0
  end

  test "a job class with no registered handler fails loudly instead of dangling",
       %{conn: conn, dir: dir} do
    log = Path.join(dir, "log")
    ruby!("enqueue.rb", ["elixir", "1", "x", log, "FailingJob"])

    # Registry only knows MarkerJob.
    {_name, _pid} = start_queue!(handlers: %{"MarkerJob" => QueueHandlers.Marker})

    wait_until(fn ->
      one!(conn, "SELECT count(*) FROM solid_queue_failed_executions") == 1
    end)

    [[error_json]] = rows!(conn, "SELECT error FROM solid_queue_failed_executions")
    error = Jason.decode!(error_json)
    assert error["exception_class"] == "OdoshiBeam.Queue.UnknownJobClassError"
    assert error["message"] =~ ~s(no Elixir handler registered for ActiveJob class "FailingJob")
  end

  defp capture_job_events do
    test_pid = self()
    handler_id = "job-events-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:odoshi_beam, :job, :start],
        [:odoshi_beam, :job, :finish],
        [:odoshi_beam, :job, :failure]
      ],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:job_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
  end
end
