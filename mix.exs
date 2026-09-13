defmodule OtpRailsBeam.MixProject do
  use Mix.Project

  def project do
    [
      app: :otp_rails_beam,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Elixir sidecar supervisor for Rails processes speaking the otp-rails §5/§6 contract"
    ]
  end

  def application do
    # Library-style app: supervision trees are started explicitly via
    # OtpRailsBeam.start_link/1, never implicitly at application boot.
    [extra_applications: [:logger, :crypto]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # The BEAM-standard event bus; DESIGN §6 names events specifically so the
      # Ruby side "mirrors Elixir :telemetry" — this is that library.
      {:telemetry, "~> 1.2"},
      # JSON codec for the §5 NDJSON wire format. Elixir has no built-in JSON
      # until 1.18/OTP 27; we support 1.15+, so Jason is required.
      {:jason, "~> 1.4"},
      # PostgreSQL driver for the shared Solid Queue job tables (Phase 4 step
      # 2). Hand-rolled SQL against the Solid Queue schema — no Ecto; see the
      # README dependency section for the reasoning.
      {:postgrex, "~> 0.17"},
      # Minimal websocket stack for the ActionCable-compatible cable endpoint
      # (Phase 4 step 3) — the decision log explicitly allows serving the
      # protocol without full Phoenix. Bandit is a pure-Elixir HTTP server;
      # websock_adapter upgrades a Plug request to a WebSock handler. See the
      # README dependency section for the reasoning.
      {:bandit, "~> 1.5"},
      {:websock_adapter, "~> 0.5"}
    ]
  end
end
