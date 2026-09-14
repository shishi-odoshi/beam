defmodule OdoshiBeam.Queue.Handler do
  @moduledoc """
  Behaviour for Elixir job handlers backing ActiveJob classes.

  A handler is registered in `OdoshiBeam.Queue`'s `:handlers` map under the
  ActiveJob class name (`"HardJob"` => `MyApp.HardJob`) and receives the
  job's ActiveJob `arguments` array, deserialized to plain Elixir terms (see
  `OdoshiBeam.Queue.ActiveJob` for exactly what is and isn't supported).

  Return `:ok` on success. Return `{:error, term}` — or raise/throw/exit — to
  fail the execution: the job lands in `solid_queue_failed_executions` with a
  well-formed error payload, where Solid Queue's normal Ruby-side tooling
  (Mission Control, `SolidQueue::FailedExecution#retry`, discard) takes over.

  Handler failures are NOT ActiveJob retries: `retry_on`/`discard_on`
  declared on the Ruby job class only run inside a Ruby worker, so a job that
  fails in an Elixir handler goes straight to `failed_executions` with
  `executions` still at its enqueue-time value. Retrying it from the Ruby
  side re-dispatches it like any other failed job.

  Any return value other than `:ok` or `{:error, term}` is treated as a
  failure too (strict on purpose — a handler that "returns something" was
  probably not written against this contract).
  """

  @callback perform(args :: list()) :: :ok | {:error, term()}
end
