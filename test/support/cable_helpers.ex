defmodule OtpRailsBeam.CableHelpers do
  @moduledoc """
  Shared plumbing for the Solid Cable interop tests (test/cable/, tagged
  `:cable`): Postgres connection options (env-overridable, defaulting to
  the docker container from the README), and runners for the Ruby fixture
  scripts under test/fixtures/solid_cable/.
  """

  import ExUnit.Assertions

  @fixture_dir Path.expand("../fixtures/solid_cable", __DIR__)

  # One shared secret_key_base for the whole suite (the Ruby fixture and
  # every beam Cable instance derive the same Turbo verifier key from it).
  @secret_key_base "otp-rails-beam-cable-interop-secret"

  def fixture_dir, do: @fixture_dir
  def secret_key_base, do: @secret_key_base

  def pg_env do
    %{
      host: System.get_env("SOLID_CABLE_PG_HOST", "127.0.0.1"),
      port: String.to_integer(System.get_env("SOLID_CABLE_PG_PORT", "55433")),
      user: System.get_env("SOLID_CABLE_PG_USER", "postgres"),
      password: System.get_env("SOLID_CABLE_PG_PASSWORD", "postgres"),
      database: System.get_env("SOLID_CABLE_PG_DATABASE", "otp_rails_beam_cable_test")
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
      {"CABLE_SECRET_KEY_BASE", @secret_key_base},
      {"SOLID_CABLE_PG_HOST", env.host},
      {"SOLID_CABLE_PG_PORT", Integer.to_string(env.port)},
      {"SOLID_CABLE_PG_USER", env.user},
      {"SOLID_CABLE_PG_PASSWORD", env.password},
      {"SOLID_CABLE_PG_DATABASE", env.database}
    ]
  end

  @doc "Reset the Solid Cable schema from the gem's installer template."
  def setup_db! do
    assert ruby!("setup_db.rb") =~ "SCHEMA_LOADED"
  end

  @doc "A genuinely turbo-rails-signed stream name for `stream`."
  def sign!(stream) do
    "sign.rb" |> ruby!([stream]) |> String.trim()
  end

  @doc """
  Broadcast `message` (any JSON-able term) to `stream` through the real
  Solid Cable pubsub (Action Cable server.broadcast → adapter → insert).
  """
  def broadcast!(stream, message) do
    assert ruby!("broadcast.rb", [stream, Jason.encode!(message)]) =~ "BROADCASTED"
  end

  @doc "The @rails/actioncable-style identifier string for a Turbo Streams subscription."
  def turbo_identifier(signed_stream_name) do
    Jason.encode!(%{
      "channel" => "Turbo::StreamsChannel",
      "signed_stream_name" => signed_stream_name
    })
  end

  @doc "Unique loopback port for a per-test Cable instance."
  def unique_port do
    # Ephemeral-ish range, spaced out so parallel tests can't collide.
    39_000 + System.unique_integer([:positive, :monotonic])
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

  @doc """
  The docker container running the cable Postgres — the README recipe's
  name locally, the GitHub Actions service container id in CI (the cable
  job exports it as CABLE_PG_CONTAINER). Used by the DB-outage test to
  stop/start a REAL Postgres under the suite.
  """
  def pg_container do
    System.get_env("CABLE_PG_CONTAINER", "otp-rails-beam-queue-pg")
  end

  def stop_db! do
    {_out, 0} = System.cmd("docker", ["stop", "-t", "1", pg_container()], stderr_to_stdout: true)
    :ok
  end

  def start_db! do
    {_out, 0} = System.cmd("docker", ["start", pg_container()], stderr_to_stdout: true)

    wait_until(30_000, fn ->
      match?(
        {_out, 0},
        System.cmd("docker", ["exec", pg_container(), "pg_isready", "-U", pg_env().user],
          stderr_to_stdout: true
        )
      )
    end)

    :ok
  end
end
