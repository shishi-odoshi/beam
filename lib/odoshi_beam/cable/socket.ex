defmodule OdoshiBeam.Cable.Socket do
  @moduledoc """
  A WebSock handler speaking the ActionCable v1 JSON protocol — client
  frames and server frames exactly as Action Cable (8.0.x) does:

  * On open: `{"type":"welcome"}` (`Connection::Base#send_welcome_message`).
  * Every 3s: `{"type":"ping","message":<unix seconds>}`
    (`Connection::Base#beat`, `Server::Connections::BEAT_INTERVAL = 3`).
  * `{"command":"subscribe","identifier":"{\\"channel\\":...}"}` →
    `{"identifier":...,"type":"confirm_subscription"}` or
    `{"identifier":...,"type":"reject_subscription"}` — the identifier is
    echoed VERBATIM (clients match subscriptions by exact string).
  * `{"command":"unsubscribe",...}` → no reply frame (Action Cable sends
    none).
  * Broadcasts: `{"identifier":...,"message":<decoded payload>}`
    (`Channel::Base#transmit` via the default stream handler).

  ## Subscription auth (v1 boundary)

  beam runs no Ruby channel code, so the ONLY identifiers accepted are:

  * `Turbo::StreamsChannel` with a `signed_stream_name` that verifies
    against the shared Turbo verifier key — beam then streams from the
    verified name, precisely `Turbo::StreamsChannel#subscribed`
    (`stream_from verified_stream_name_from_params`, else `reject`).
  * Channel names present in the configured `:allowed_channels` map
    (default empty), whose function maps the identifier's params to the
    stream names to subscribe — the beam-side stand-in for that channel's
    Ruby `subscribed` method.

  Everything else is rejected. There is NO cookie/session authentication:
  Action Cable's `Connection#connect` (cookie-based `identified_by` auth)
  has no beam equivalent in v1, and no origin checking is performed —
  authorization lives entirely in the unguessable signed stream name.

  Divergence, on purpose: for a subscribe whose identifier decodes but
  names an unknown channel, Ruby Action Cable only logs "Subscription
  class not found" and sends nothing, leaving the client waiting. beam
  sends an explicit `reject_subscription` (the frame a rejecting channel
  would produce, which @rails/actioncable handles via the `rejected`
  callback) so clients aren't left hanging — documented in the README.

  ## Surviving listener restarts (beam#8)

  The socket's `subscriptions` map is the durable copy of what this client
  subscribed to; the listener's subscriber map is soft state. Each socket
  monitors the listener, and when it goes down the socket stays connected
  (pings keep flowing), re-registers all of its subscriptions with the
  restarted listener, and misses only the broadcasts of the gap. The fresh
  registrations take new baselines at the current `MAX(id)`, so nothing is
  replayed. If a subscription cannot be registered at all — the listener
  is down mid-subscribe or the DB is unreachable, so no replay-guard
  baseline exists (beam#9) — the socket closes with 1013 (Try Again
  Later): confirming would lie and rejecting would read as an auth
  failure, while a close makes @rails/actioncable's monitor reconnect
  with backoff.
  """

  @behaviour WebSock

  require Logger

  @ping_interval_ms 3_000

  # How often a socket retries attaching to a restarted listener (beam#8).
  @reattach_interval_ms 100

  @impl true
  def init(opts) do
    state = %{
      listener: Keyword.fetch!(opts, :listener),
      verifier_key: Keyword.fetch!(opts, :verifier_key),
      allowed_channels: Keyword.get(opts, :allowed_channels, %{}),
      ping_interval_ms: Keyword.get(opts, :ping_interval_ms, @ping_interval_ms),
      # identifier => [stream] — `return if subscriptions.key?(id_key)`
      # duplicate-subscribe suppression needs this map even for streams the
      # listener tracks. It is also the socket's authority for re-registering
      # with a restarted listener (beam#8): the listener's subscriber map is
      # soft state, this map is the durable copy.
      subscriptions: %{},
      # Monitor on the current listener process; a DOWN triggers the
      # re-subscribe loop below.
      listener_ref: nil
    }

    # Monitoring by registered name: if the listener happens to be down
    # right now we get an immediate :noproc DOWN, which flows into the same
    # retry loop as a mid-connection listener crash.
    state = %{state | listener_ref: Process.monitor(state.listener)}

    :telemetry.execute(
      [:odoshi_beam, :cable, :connect],
      %{system_time: System.system_time()},
      %{}
    )

    Process.send_after(self(), :beat, state.ping_interval_ms)
    {:push, frame(%{"type" => "welcome"}), state}
  end

  @impl true
  def handle_in({data, [opcode: :text]}, state) do
    case Jason.decode(data) do
      {:ok, %{"command" => command} = payload} ->
        handle_command(command, payload, state)

      _ ->
        # Subscriptions#execute_command rescues and logs; no reply frame.
        Logger.error("odoshi_beam.cable: unparseable client frame: #{inspect(data)}")
        {:ok, state}
    end
  end

  def handle_in(_frame, state), do: {:ok, state}

  defp handle_command("subscribe", %{"identifier" => identifier}, state)
       when is_binary(identifier) do
    if Map.has_key?(state.subscriptions, identifier) do
      # Subscriptions#add: `return if subscriptions.key?(id_key)` — a
      # duplicate subscribe is silently ignored, no second confirmation.
      {:ok, state}
    else
      case authorize(identifier, state) do
        {:ok, streams} ->
          case register_streams(state, identifier, streams) do
            :ok ->
              :telemetry.execute(
                [:odoshi_beam, :cable, :subscribe],
                %{system_time: System.system_time()},
                %{identifier: identifier, streams: streams}
              )

              {:push, frame(%{"identifier" => identifier, "type" => "confirm_subscription"}),
               put_in(state.subscriptions[identifier], streams)}

            # The listener can't establish the subscription's replay-guard
            # baseline (DB outage) or is mid-restart. Confirming would lie
            # and rejecting would read as an auth failure to the client, so
            # close 1013 (Try Again Later): @rails/actioncable's connection
            # monitor reconnects with backoff and the subscribe succeeds
            # once storage is back.
            :unavailable ->
              Logger.warning(
                "odoshi_beam.cable: closing socket, cannot register subscription " <>
                  "(listener/DB unavailable): #{inspect(identifier)}"
              )

              {:stop, {:shutdown, :subscription_storage_unavailable}, {1013, "try again later"},
               state}
          end

        {:reject, reason} ->
          :telemetry.execute(
            [:odoshi_beam, :cable, :reject],
            %{system_time: System.system_time()},
            %{identifier: identifier, reason: reason}
          )

          {:push, frame(%{"identifier" => identifier, "type" => "reject_subscription"}), state}
      end
    end
  end

  defp handle_command("unsubscribe", %{"identifier" => identifier}, state)
       when is_binary(identifier) do
    if Map.has_key?(state.subscriptions, identifier) do
      # A dead/restarting listener holds no registration for us anyway;
      # dropping the local entry is the durable part (it also stops the
      # re-subscribe loop from re-registering this identifier).
      try do
        OdoshiBeam.Cable.Listener.unsubscribe(state.listener, self(), identifier)
      catch
        :exit, _reason -> :ok
      end

      {:ok, %{state | subscriptions: Map.delete(state.subscriptions, identifier)}}
    else
      # Subscriptions#find raises -> execute_command logs; no frame.
      {:ok, state}
    end
  end

  defp handle_command("message", _payload, state) do
    # Channel actions (`perform_action`) run Ruby channel code; Turbo's
    # StreamsChannel defines none. Ignored in v1 (logged like the Ruby
    # rescue path would).
    Logger.debug("odoshi_beam.cable: ignoring channel action (no channel code in beam v1)")
    {:ok, state}
  end

  defp handle_command(other, payload, state) do
    # Subscriptions#execute_command: "Received unrecognized command".
    Logger.error(
      "odoshi_beam.cable: unrecognized command #{inspect(other)} in #{inspect(payload)}"
    )

    {:ok, state}
  end

  # Turbo::StreamsChannel#subscribed, in beam terms: verified name => stream
  # from it; anything unverifiable => reject.
  defp authorize(identifier, state) do
    case Jason.decode(identifier) do
      {:ok, %{"channel" => "Turbo::StreamsChannel"} = params} ->
        case OdoshiBeam.Cable.SignedStreamName.verify(
               params["signed_stream_name"],
               state.verifier_key
             ) do
          {:ok, stream} -> {:ok, [stream]}
          :error -> {:reject, :invalid_signed_stream_name}
        end

      {:ok, %{"channel" => channel} = params} when is_binary(channel) ->
        case state.allowed_channels do
          %{^channel => streams_fun} -> allowlisted_streams(streams_fun, params)
          _ -> {:reject, :channel_not_allowed}
        end

      _ ->
        {:reject, :malformed_identifier}
    end
  end

  defp allowlisted_streams(streams_fun, params) do
    case streams_fun.(params) do
      streams when is_list(streams) ->
        if Enum.all?(streams, &is_binary/1),
          do: {:ok, streams},
          else: {:reject, :allowlist_rejected}

      stream when is_binary(stream) ->
        {:ok, [stream]}

      _ ->
        # A nil/false return is the allowlist function's `reject`.
        {:reject, :allowlist_rejected}
    end
  rescue
    error ->
      Logger.error("odoshi_beam.cable: allowed_channels function raised: #{inspect(error)}")
      {:reject, :allowlist_rejected}
  end

  @impl true
  def handle_info(:beat, state) do
    Process.send_after(self(), :beat, state.ping_interval_ms)
    {:push, frame(%{"type" => "ping", "message" => System.system_time(:second)}), state}
  end

  def handle_info({:cable_broadcast, identifier, message}, state) do
    # Deliver only while the identifier is still subscribed — an in-flight
    # broadcast can race an unsubscribe.
    if Map.has_key?(state.subscriptions, identifier) do
      {:push, frame(%{"identifier" => identifier, "message" => message}), state}
    else
      {:ok, state}
    end
  end

  # The listener died (beam#8): this socket stays up — its subscriptions
  # map is the durable copy — and re-registers with the restarted listener.
  # Until that succeeds it simply misses broadcasts for the gap, exactly
  # the documented contract.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{listener_ref: ref} = state) do
    Process.send_after(self(), :reattach_listener, @reattach_interval_ms)
    {:ok, %{state | listener_ref: nil}}
  end

  def handle_info(:reattach_listener, state) do
    case reattach(state) do
      {:ok, state} ->
        {:ok, state}

      :retry ->
        Process.send_after(self(), :reattach_listener, @reattach_interval_ms)
        {:ok, state}
    end
  end

  def handle_info(_other, state), do: {:ok, state}

  defp reattach(state) do
    case Process.whereis(state.listener) do
      nil ->
        :retry

      pid ->
        ref = Process.monitor(pid)

        result =
          Enum.reduce_while(state.subscriptions, :ok, fn {identifier, streams}, :ok ->
            case register_streams(state, identifier, streams) do
              :ok -> {:cont, :ok}
              :unavailable -> {:halt, :unavailable}
            end
          end)

        case result do
          :ok ->
            {:ok, %{state | listener_ref: ref}}

          # New listener is up but the DB (or the listener again) isn't:
          # keep the monitor on whatever we saw and retry the registration
          # sweep. register_streams is idempotent per {socket, identifier,
          # stream}, so re-running it never double-subscribes.
          :unavailable ->
            Process.demonitor(ref, [:flush])
            :retry
        end
    end
  end

  # Registers every stream with the listener; :unavailable when the
  # listener is down mid-call or replies {:error, :db_unavailable}.
  defp register_streams(state, identifier, streams) do
    Enum.reduce_while(streams, :ok, fn stream, :ok ->
      try do
        case OdoshiBeam.Cable.Listener.subscribe(state.listener, self(), identifier, stream) do
          :ok -> {:cont, :ok}
          {:error, :db_unavailable} -> {:halt, :unavailable}
        end
      catch
        :exit, _reason -> {:halt, :unavailable}
      end
    end)
  end

  @impl true
  def terminate(_reason, _state) do
    # The listener monitors this process and clears its subscriptions on
    # DOWN; nothing to unwind here.
    :ok
  end

  defp frame(map), do: {:text, Jason.encode!(map)}
end
