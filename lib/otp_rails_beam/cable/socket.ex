defmodule OtpRailsBeam.Cable.Socket do
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
  """

  @behaviour WebSock

  require Logger

  @ping_interval_ms 3_000

  @impl true
  def init(opts) do
    state = %{
      listener: Keyword.fetch!(opts, :listener),
      verifier_key: Keyword.fetch!(opts, :verifier_key),
      allowed_channels: Keyword.get(opts, :allowed_channels, %{}),
      ping_interval_ms: Keyword.get(opts, :ping_interval_ms, @ping_interval_ms),
      # identifier => [stream] — `return if subscriptions.key?(id_key)`
      # duplicate-subscribe suppression needs this map even for streams the
      # listener tracks.
      subscriptions: %{}
    }

    :telemetry.execute(
      [:otp_rails_beam, :cable, :connect],
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
        Logger.error("otp_rails_beam.cable: unparseable client frame: #{inspect(data)}")
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
          Enum.each(
            streams,
            &OtpRailsBeam.Cable.Listener.subscribe(state.listener, self(), identifier, &1)
          )

          :telemetry.execute(
            [:otp_rails_beam, :cable, :subscribe],
            %{system_time: System.system_time()},
            %{identifier: identifier, streams: streams}
          )

          {:push, frame(%{"identifier" => identifier, "type" => "confirm_subscription"}),
           put_in(state.subscriptions[identifier], streams)}

        {:reject, reason} ->
          :telemetry.execute(
            [:otp_rails_beam, :cable, :reject],
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
      OtpRailsBeam.Cable.Listener.unsubscribe(state.listener, self(), identifier)
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
    Logger.debug("otp_rails_beam.cable: ignoring channel action (no channel code in beam v1)")
    {:ok, state}
  end

  defp handle_command(other, payload, state) do
    # Subscriptions#execute_command: "Received unrecognized command".
    Logger.error(
      "otp_rails_beam.cable: unrecognized command #{inspect(other)} in #{inspect(payload)}"
    )

    {:ok, state}
  end

  # Turbo::StreamsChannel#subscribed, in beam terms: verified name => stream
  # from it; anything unverifiable => reject.
  defp authorize(identifier, state) do
    case Jason.decode(identifier) do
      {:ok, %{"channel" => "Turbo::StreamsChannel"} = params} ->
        case OtpRailsBeam.Cable.SignedStreamName.verify(
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
      Logger.error("otp_rails_beam.cable: allowed_channels function raised: #{inspect(error)}")
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

  def handle_info(_other, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state) do
    # The listener monitors this process and clears its subscriptions on
    # DOWN; nothing to unwind here.
    :ok
  end

  defp frame(map), do: {:text, Jason.encode!(map)}
end
