defmodule OdoshiBeam.QueueHelpers do
  @moduledoc """
  Shared plumbing for the Solid Queue interop tests (test/queue/, tagged
  `:queue`): Postgres connection options (env-overridable, defaulting to the
  docker container from the README), and runners for the Ruby fixture
  scripts under test/fixtures/solid_queue/.
  """

  import ExUnit.Assertions

  @fixture_dir Path.expand("../fixtures/solid_queue", __DIR__)

  def fixture_dir, do: @fixture_dir

  def pg_env do
    %{
      host: System.get_env("SOLID_QUEUE_PG_HOST", "127.0.0.1"),
      port: String.to_integer(System.get_env("SOLID_QUEUE_PG_PORT", "55433")),
      user: System.get_env("SOLID_QUEUE_PG_USER", "postgres"),
      password: System.get_env("SOLID_QUEUE_PG_PASSWORD", "postgres"),
      database: System.get_env("SOLID_QUEUE_PG_DATABASE", "odoshi_beam_queue_test")
    }
  end

  def db_opts do
    env = pg_env()

    [
      hostname: env.host,
      port: env.port,
      username: env.user,
      password: env.password,
      database: env.database
    ]
  end

  @doc "Run a Ruby fixture script under the test-only bundle; returns stdout, asserts exit 0."
  def ruby!(script, args \\ []) do
    {out, status} = ruby(script, args)

    assert status == 0,
           "#{script} #{Enum.join(args, " ")} exited #{status}:\n#{out}"

    out
  end

  def ruby(script, args) do
    System.cmd("bundle", ["exec", "ruby", script | args],
      cd: @fixture_dir,
      env: ruby_env(),
      stderr_to_stdout: true
    )
  end

  def ruby_env do
    env = pg_env()

    [
      {"BUNDLE_GEMFILE", Path.join(@fixture_dir, "Gemfile")},
      {"SOLID_QUEUE_PG_HOST", env.host},
      {"SOLID_QUEUE_PG_PORT", Integer.to_string(env.port)},
      {"SOLID_QUEUE_PG_USER", env.user},
      {"SOLID_QUEUE_PG_PASSWORD", env.password},
      {"SOLID_QUEUE_PG_DATABASE", env.database}
    ]
  end

  @doc "Reset the Solid Queue schema from the gem's installer template."
  def setup_db! do
    assert ruby!("setup_db.rb") =~ "SCHEMA_LOADED"
  end

  @doc "One-off assertion connection (link it to the test process)."
  def connect! do
    {:ok, conn} = Postgrex.start_link(db_opts())
    conn
  end

  def one!(conn, sql, params \\ []) do
    %{rows: [[value]]} = Postgrex.query!(conn, sql, params)
    value
  end

  def rows!(conn, sql, params \\ []) do
    %{rows: rows} = Postgrex.query!(conn, sql, params)
    rows
  end

  @doc "Poll `fun` every 50ms until truthy or `timeout_ms` elapses (then flunk)."
  def wait_until(timeout_ms \\ 15_000, fun) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait(deadline, fun)
  end

  defp do_wait(deadline, fun) do
    cond do
      value = fun.() ->
        value

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition not met within timeout")

      true ->
        Process.sleep(50)
        do_wait(deadline, fun)
    end
  end

  @doc "Unique per-test scratch dir."
  def tmp_dir!(tag) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "odoshi_beam_queue_#{tag}_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    dir
  end
end
