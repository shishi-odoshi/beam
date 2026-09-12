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
      {:jason, "~> 1.4"}
    ]
  end
end
