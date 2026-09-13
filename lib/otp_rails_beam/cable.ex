defmodule OtpRailsBeam.Cable do
  @moduledoc """
  ActionCable-compatible realtime from the Solid Cable schema (otp-rails
  Phase 4 step 3, final): beam serves the ActionCable v1 JSON wire protocol
  over WebSockets, sourcing broadcasts from the SAME `solid_cable_messages`
  table the Rails app's Solid Cable adapter writes. Existing Turbo Streams /
  `@rails/actioncable` clients repoint their cable URL at beam with zero
  Rails-side code changes; Rails keeps broadcasting exactly as before
  (`Turbo::Broadcastable`, `ActionCable.server.broadcast`).

      {:ok, cable} =
        OtpRailsBeam.Cable.start_link(
          port: 28080,
          db: [
            hostname: "localhost",
            username: "app",
            password: "...",
            database: "app_production_cable"   # the cable database
          ],
          secret_key_base: System.fetch_env!("OTP_RAILS_CABLE_SECRET")
        )

  Point clients at `ws://host:28080/cable`.

  Options:

  * `:port` (required) — TCP port to listen on. `:ip` defaults to loopback
    (`{127, 0, 0, 1}`); set `ip: {0, 0, 0, 0}` (or a tuple of your choice)
    to expose it, ideally behind the same TLS-terminating proxy as the app.
  * `:db` (required) — `Postgrex.start_link/1` options for the database
    holding `solid_cable_messages` (Rails 8's default `cable` database).
  * `:secret_key_base` — the Rails app's `secret_key_base`; the Turbo
    verifier key is derived from it exactly like
    `Rails.application.key_generator` does (see
    `OtpRailsBeam.Cable.SignedStreamName`). Defaults to the
    `OTP_RAILS_CABLE_SECRET` env var, then `SECRET_KEY_BASE`. Turbo keys
    off `secret_key_base` (not a cable-specific secret), so sharing
    `SECRET_KEY_BASE` with beam is what makes Rails-signed stream names
    verify here.
  * `:signed_stream_verifier_key` — raw verifier key, for apps that set
    `config.turbo.signed_stream_verifier_key` explicitly. Overrides
    `:secret_key_base` derivation.
  * `:key_digest` — `:sha256` (default; `config.load_defaults 7.0`+, all
    Rails 8 apps) or `:sha1` for apps still on pre-7.0 key-generator
    defaults.
  * `:allowed_channels` — map of channel class name to
    `(identifier params) -> stream | [streams] | nil`, the beam-side
    stand-in for a non-Turbo channel's `subscribed` method. Default `%{}`:
    only `Turbo::StreamsChannel` subscriptions are accepted.
  * `:path` — mount path, default `"/cable"`
    (`ActionCable::INTERNAL[:default_mount_path]`).
  * `:polling_interval_ms` — default 100 (Solid Cable's
    `polling_interval` default of 0.1s).
  * `:ping_interval_ms` — default 3000
    (`ActionCable::Server::Connections::BEAT_INTERVAL`). Change only for
    tests; real clients time out against unexpected cadences.
  * `:pool_size` — Postgrex pool size, default 2.
  * `:name` — supervisor name, default `OtpRailsBeam.Cable`.

  ## Auth boundary (v1)

  Authorization is the Turbo signed stream name and nothing else: a
  subscription is accepted iff its `signed_stream_name` verifies against
  the shared secret (or its channel is explicitly allowlisted). There is no
  cookie/session authentication — beam never sees the Rails session, so
  `identified_by :current_user`-style connection auth does not exist here —
  and no Origin checking. Broadcasts are readable by anyone who holds a
  validly signed stream name for that stream. Native Phoenix channel
  semantics are explicitly deferred (decision log 2026-09-13).

  ## Telemetry

  Beam-local events (the §6 supervision contract is frozen and untouched):

      [:otp_rails_beam, :cable, :connect]    %{system_time}  %{}
      [:otp_rails_beam, :cable, :subscribe]  %{system_time}  %{identifier, streams}
      [:otp_rails_beam, :cable, :reject]     %{system_time}  %{identifier, reason}
      [:otp_rails_beam, :cable, :broadcast]  %{subscribers}  %{channel, message_id}
  """

  use Supervisor

  alias OtpRailsBeam.Cable.SignedStreamName

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, Keyword.put(opts, :name, name), name: name)
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    db_opts = Keyword.fetch!(opts, :db)
    port = Keyword.fetch!(opts, :port)
    verifier_key = verifier_key!(opts)

    db_name = :"#{name}.DB"
    listener_name = :"#{name}.Listener"

    db_spec = %{
      id: :db,
      start:
        {Postgrex, :start_link,
         [Keyword.merge(db_opts, name: db_name, pool_size: Keyword.get(opts, :pool_size, 2))]}
    }

    listener_spec = %{
      id: :listener,
      start:
        {OtpRailsBeam.Cable.Listener, :start_link,
         [
           [
             name: listener_name,
             db: db_name,
             polling_interval_ms: Keyword.get(opts, :polling_interval_ms, 100)
           ]
         ]}
    }

    endpoint_spec =
      Supervisor.child_spec(
        {Bandit,
         plug:
           {OtpRailsBeam.Cable.Endpoint,
            [
              listener: listener_name,
              verifier_key: verifier_key,
              allowed_channels:
                validate_allowed_channels!(Keyword.get(opts, :allowed_channels, %{})),
              path: Keyword.get(opts, :path, "/cable"),
              ping_interval_ms: Keyword.get(opts, :ping_interval_ms, 3_000)
            ]},
         scheme: :http,
         ip: Keyword.get(opts, :ip, {127, 0, 0, 1}),
         port: port,
         startup_log: false},
        id: :endpoint
      )

    # rest_for_one: sockets outlive a listener restart (they just miss
    # broadcasts for the gap), but a lost DB pool restarts the listener so
    # its cursor re-baselines instead of replaying.
    Supervisor.init([db_spec, listener_spec, endpoint_spec],
      strategy: :rest_for_one,
      max_restarts: Keyword.get(opts, :max_restarts, 10),
      max_seconds: Keyword.get(opts, :max_seconds, 60)
    )
  end

  defp verifier_key!(opts) do
    cond do
      key = Keyword.get(opts, :signed_stream_verifier_key) ->
        key

      secret =
          Keyword.get(opts, :secret_key_base) || System.get_env("OTP_RAILS_CABLE_SECRET") ||
            System.get_env("SECRET_KEY_BASE") ->
        SignedStreamName.derive_key(secret, Keyword.get(opts, :key_digest, :sha256))

      true ->
        raise ArgumentError,
              "OtpRailsBeam.Cable needs the Rails secret to verify Turbo signed stream names: " <>
                "pass :secret_key_base (or :signed_stream_verifier_key), or set " <>
                "OTP_RAILS_CABLE_SECRET / SECRET_KEY_BASE in the environment"
    end
  end

  defp validate_allowed_channels!(allowed) do
    with true <- is_map(allowed),
         true <-
           Enum.all?(allowed, fn {name, fun} -> is_binary(name) and is_function(fun, 1) end) do
      allowed
    else
      _ ->
        raise ArgumentError,
              ":allowed_channels must be a map of channel class name => " <>
                "(params -> stream | [streams] | nil), got: #{inspect(allowed)}"
    end
  end
end
