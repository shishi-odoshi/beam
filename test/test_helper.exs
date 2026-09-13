# Cross-implementation contract tests (test/contract/) need ruby + the
# otp-rails gem on PATH; excluded by default so `mix test` stays pure-Elixir.
# Run them with `mix test --include contract` (or `--only contract`).
ExUnit.configure(exclude: [:contract])
ExUnit.start()
