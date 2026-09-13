# Cross-implementation contract tests (test/contract/) need ruby + the
# otp-rails gem on PATH; excluded by default so `mix test` stays pure-Elixir.
# Run them with `mix test --include contract` (or `--only contract`).
#
# Shared-queue interop tests (test/queue/, tagged :queue) additionally need
# Postgres (docker, see README) and the test/fixtures/solid_queue bundle.
# Run them with `mix test --only queue`.
ExUnit.configure(exclude: [:contract, :queue])
ExUnit.start()
