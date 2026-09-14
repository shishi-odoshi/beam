defmodule OdoshiBeam.Cable.CableInteropTest do
  @moduledoc """
  Interop acceptance suite for the ActionCable-compatible cable (Phase 4
  step 3): the Ruby side is the REAL solid_cable + actioncable + turbo-rails
  stack (test/fixtures/solid_cable/) signing stream names and broadcasting
  through Solid Cable's pubsub, the Elixir side is `OdoshiBeam.Cable`
  serving the ActionCable v1 JSON protocol, and both meet in one Postgres
  `solid_cable_messages` table. Requires the docker Postgres from the
  README (`:cable` tag).
  """

  use ExUnit.Case, async: false

  import OdoshiBeam.CableHelpers

  alias OdoshiBeam.CableWsClient, as: Client

  @moduletag :cable
  @moduletag timeout: 120_000

  setup do
    setup_db!()
    :ok
  end

  defp start_cable!(opts \\ []) do
    port = unique_port()
    name = :"cable_under_test_#{System.unique_integer([:positive])}"

    defaults = [
      name: name,
      port: port,
      db: db_opts(),
      secret_key_base: secret_key_base(),
      polling_interval_ms: 50
    ]

    start_supervised!({OdoshiBeam.Cable, Keyword.merge(defaults, opts)}, id: name)
    {port, name}
  end

  # A broadcast frame is {"identifier":..., "message":...} with no "type" —
  # pings also carry "message", so the type must be absent.
  defp broadcast_frame?(m), do: is_map_key(m, "message") and not is_map_key(m, "type")

  defp connect_and_welcome!(port, opts \\ []) do
    client = Client.connect!(port, opts)
    {welcome, client} = Client.recv_json!(client)
    assert welcome == %{"type" => "welcome"}
    client
  end

  defp subscribe!(client, identifier) do
    Client.send_json(client, %{"command" => "subscribe", "identifier" => identifier})

    {confirmation, client} =
      Client.recv_json_until!(client, fn m ->
        m["type"] in ["confirm_subscription", "reject_subscription"]
      end)

    assert confirmation == %{"identifier" => identifier, "type" => "confirm_subscription"}
    client
  end

  test "welcome frame and actioncable-v1-json subprotocol negotiation" do
    {port, _name} = start_cable!()

    # @rails/actioncable offers both protocols; the first supported one wins.
    client = Client.connect!(port)
    assert client.protocol == "actioncable-v1-json"
    {welcome, client} = Client.recv_json!(client)
    assert welcome == %{"type" => "welcome"}
    Client.close(client)

    # A client offering no subprotocol still connects, with none negotiated
    # (Action Cable does not refuse it either).
    bare = Client.connect!(port, protocols: [])
    assert bare.protocol == nil
    {welcome, bare} = Client.recv_json!(bare)
    assert welcome == %{"type" => "welcome"}
    Client.close(bare)
  end

  test "ping frames on the 3s Action Cable cadence carrying unix time" do
    {port, _name} = start_cable!()
    client = connect_and_welcome!(port)

    before = System.os_time(:second)
    {ping, client} = Client.recv_json_until!(client, 4_500, fn m -> m["type"] == "ping" end)
    assert is_integer(ping["message"])
    assert_in_delta ping["message"], before, 6

    # The next beat lands roughly one interval later, well inside 2x.
    {_ping2, client} = Client.recv_json_until!(client, 6_000, fn m -> m["type"] == "ping" end)
    Client.close(client)
  end

  test "Ruby-signed Turbo subscription confirms; Ruby broadcast arrives verbatim" do
    {port, _name} = start_cable!()
    signed = sign!("board:42")
    identifier = turbo_identifier(signed)

    client = port |> connect_and_welcome!() |> subscribe!(identifier)

    # A Turbo broadcast is a raw HTML string; assert byte-for-byte delivery.
    html =
      ~s(<turbo-stream action="append" target="messages"><template>hi</template></turbo-stream>)

    broadcast!("board:42", html)

    {frame, client} =
      Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame == %{"identifier" => identifier, "message" => html}

    # Structured payloads survive the JSON round-trip too.
    broadcast!("board:42", %{"kind" => "object", "n" => 7, "nested" => [1, "two", nil]})

    {frame2, client} =
      Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame2["identifier"] == identifier
    assert frame2["message"] == %{"kind" => "object", "n" => 7, "nested" => [1, "two", nil]}

    Client.close(client)
  end

  test "tampered signature is rejected (and telemetry says why)" do
    {port, _name} = start_cable!()
    test_pid = self()

    :telemetry.attach(
      "cable-reject-#{inspect(test_pid)}",
      [:odoshi_beam, :cable, :reject],
      fn _event, _measurements, metadata, _config -> send(test_pid, {:reject, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("cable-reject-#{inspect(test_pid)}") end)

    signed = sign!("board:42")
    tampered = String.slice(signed, 0..-3//1) <> "ff"
    identifier = turbo_identifier(tampered)

    client = connect_and_welcome!(port)
    Client.send_json(client, %{"command" => "subscribe", "identifier" => identifier})

    {rejection, client} =
      Client.recv_json_until!(client, fn m -> Map.has_key?(m, "type") and m["type"] != "ping" end)

    assert rejection == %{"identifier" => identifier, "type" => "reject_subscription"}
    assert_receive {:reject, %{reason: :invalid_signed_stream_name}}, 2_000

    # And a rejected subscription receives no broadcasts.
    broadcast!("board:42", "should not arrive")
    client = Client.refute_json!(client, 1_000, fn m -> broadcast_frame?(m) end)
    Client.close(client)
  end

  test "unknown channels are rejected unless allowlisted (default allowlist is empty)" do
    {port, _name} =
      start_cable!(
        allowed_channels: %{
          "FixtureChannel" => fn params -> "fixture:#{params["room"]}" end
        }
      )

    client = connect_and_welcome!(port)

    # Not in the allowlist -> rejected.
    unknown = Jason.encode!(%{"channel" => "NopeChannel"})
    Client.send_json(client, %{"command" => "subscribe", "identifier" => unknown})

    {rejection, client} =
      Client.recv_json_until!(client, fn m -> m["type"] == "reject_subscription" end)

    assert rejection["identifier"] == unknown

    # Allowlisted -> confirmed, streaming from the mapped stream name.
    allowed = Jason.encode!(%{"channel" => "FixtureChannel", "room" => "7"})
    client = subscribe!(client, allowed)
    broadcast!("fixture:7", %{"ok" => true})

    {frame, client} =
      Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame == %{"identifier" => allowed, "message" => %{"ok" => true}}
    Client.close(client)
  end

  test "broadcasts to other streams are not delivered; unsubscribe stops delivery" do
    {port, _name} = start_cable!()
    identifier = turbo_identifier(sign!("board:mine"))
    client = port |> connect_and_welcome!() |> subscribe!(identifier)

    broadcast!("board:other", "not for us")
    broadcast!("board:mine", "for us")

    # The first (and only) broadcast frame is ours — the other stream's
    # message was never delivered, though it has a lower message id.
    {frame, client} =
      Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame == %{"identifier" => identifier, "message" => "for us"}
    client = Client.refute_json!(client, 1_000, fn m -> broadcast_frame?(m) end)

    # Unsubscribe has no reply frame (Action Cable sends none) and halts
    # delivery.
    Client.send_json(client, %{"command" => "unsubscribe", "identifier" => identifier})
    broadcast!("board:mine", "after unsubscribe")
    client = Client.refute_json!(client, 1_500, fn m -> broadcast_frame?(m) end)
    Client.close(client)
  end

  test "reconnect does not replay messages broadcast before the new subscription" do
    {port, _name} = start_cable!()
    identifier = turbo_identifier(sign!("board:replay"))

    client1 = port |> connect_and_welcome!() |> subscribe!(identifier)
    broadcast!("board:replay", "m1")

    {frame, client1} =
      Client.recv_json_until!(client1, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame["message"] == "m1"
    Client.close(client1)

    # Broadcast while nobody is connected...
    broadcast!("board:replay", "m2")

    # ...then reconnect: neither m1 nor m2 replays (the new subscription's
    # baseline is the max message id at subscribe time, mirroring Solid
    # Cable's Listener#add_channel).
    client2 = port |> connect_and_welcome!() |> subscribe!(identifier)
    client2 = Client.refute_json!(client2, 1_500, fn m -> broadcast_frame?(m) end)

    broadcast!("board:replay", "m3")

    {frame3, client2} =
      Client.recv_json_until!(client2, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame3["message"] == "m3"
    Client.close(client2)
  end

  test "two concurrent subscribers on one stream each receive exactly one copy" do
    {port, _name} = start_cable!()
    identifier = turbo_identifier(sign!("board:shared"))

    client1 = port |> connect_and_welcome!() |> subscribe!(identifier)
    client2 = port |> connect_and_welcome!() |> subscribe!(identifier)

    broadcast!("board:shared", %{"seq" => 1})

    {frame1, client1} =
      Client.recv_json_until!(client1, 15_000, fn m -> broadcast_frame?(m) end)

    {frame2, client2} =
      Client.recv_json_until!(client2, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame1 == %{"identifier" => identifier, "message" => %{"seq" => 1}}
    assert frame2 == frame1

    # Exactly one copy each.
    client1 = Client.refute_json!(client1, 1_000, fn m -> broadcast_frame?(m) end)
    _client2 = Client.refute_json!(client2, 1_000, fn m -> broadcast_frame?(m) end)
    Client.close(client1)
    Client.close(client2)
  end

  test "duplicate subscribe is ignored and does not double-deliver" do
    {port, _name} = start_cable!()
    identifier = turbo_identifier(sign!("board:dup"))
    client = port |> connect_and_welcome!() |> subscribe!(identifier)

    # Subscriptions#add: `return if subscriptions.key?(id_key)` — no second
    # confirmation, no doubled stream.
    Client.send_json(client, %{"command" => "subscribe", "identifier" => identifier})
    client = Client.refute_json!(client, 500, fn m -> m["type"] == "confirm_subscription" end)

    broadcast!("board:dup", "once")

    {frame, client} =
      Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)

    assert frame["message"] == "once"
    client = Client.refute_json!(client, 1_000, fn m -> broadcast_frame?(m) end)
    Client.close(client)
  end

  test "listener restart: live socket stays connected, no replay, new broadcasts delivered" do
    # beam#8: the tree is one_for_one and sockets re-register with a fresh
    # listener, so a listener crash costs a connected client nothing but
    # the broadcasts of the gap.
    {port, name} = start_cable!()
    listener = :"#{name}.Listener"
    identifier = turbo_identifier(sign!("board:restart"))

    client = port |> connect_and_welcome!() |> subscribe!(identifier)
    broadcast!("board:restart", "m1")

    {frame, client} = Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)
    assert frame["message"] == "m1"

    old_pid = Process.whereis(listener)
    Process.exit(old_pid, :kill)

    # A fresh listener registers under the same name...
    new_pid =
      wait_until(fn ->
        case Process.whereis(listener) do
          nil -> false
          ^old_pid -> false
          pid -> pid
        end
      end)

    # ...and the socket re-registers its subscription with it (the socket's
    # map is the durable copy; the listener's was soft state).
    wait_until(fn -> map_size(:sys.get_state(new_pid).streams) == 1 end)

    # The socket was never closed and nothing replays — not m1, not any
    # frame (refute_json! flunks on a close frame too).
    client = Client.refute_json!(client, 1_000, fn m -> broadcast_frame?(m) end)

    # New broadcasts flow through the restarted listener to the SAME socket.
    broadcast!("board:restart", "m2")
    {frame2, client} = Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)
    assert frame2 == %{"identifier" => identifier, "message" => "m2"}
    Client.close(client)
  end

  test "DB outage: listener rides it out without restarting; socket survives and resumes" do
    # beam#9: connection errors back off (polling_interval * 10) instead of
    # crashing, so a sustained outage costs ZERO supervisor restarts and
    # ends with polling resuming where the cursor left off. Real outage:
    # docker stop/start on the suite's Postgres.
    {port, name} = start_cable!()
    listener = :"#{name}.Listener"
    identifier = turbo_identifier(sign!("board:outage"))
    # Signed BEFORE the outage: the late subscriber below must target a
    # stream with no existing baseline, or the listener can (correctly)
    # accept it without touching the DB.
    fresh_identifier = turbo_identifier(sign!("board:outage-fresh"))

    client = port |> connect_and_welcome!() |> subscribe!(identifier)
    broadcast!("board:outage", "m1")

    {frame, client} = Client.recv_json_until!(client, 15_000, fn m -> broadcast_frame?(m) end)
    assert frame["message"] == "m1"

    listener_pid = Process.whereis(listener)

    # Whatever happens below, leave the database running for the rest of
    # the suite.
    on_exit(fn -> start_db!() end)
    stop_db!()

    # Several failed poll cycles pass (50ms interval -> 500ms backoff)...
    Process.sleep(2_500)

    # ...and the listener NEVER crashed (pre-fix: ~9 restarts in 35s, whole
    # supervisor dead at ~60s), while the existing socket stayed connected —
    # pings keep flowing right through the outage.
    assert Process.whereis(listener) == listener_pid
    {ping, client} = Client.recv_json_until!(client, 4_500, fn m -> m["type"] == "ping" end)
    assert is_integer(ping["message"])

    # A NEW subscription to a fresh stream can't take its replay-guard
    # baseline while the DB is down: that socket is closed with 1013 (Try
    # Again Later) so the client's monitor retries; it is never falsely
    # confirmed or rejected. (Subscribing to a stream that already HAS a
    # baseline still succeeds without the DB — SubscriberMap semantics.)
    late_client = connect_and_welcome!(port)
    Client.send_json(late_client, %{"command" => "subscribe", "identifier" => fresh_identifier})
    assert Client.recv_close!(late_client, 15_000) == 1013

    start_db!()

    # Polling resumes on the same listener process; new broadcasts arrive
    # on the surviving socket, and m1 does not replay.
    broadcast!("board:outage", "m2")

    {frame2, client} = Client.recv_json_until!(client, 20_000, fn m -> broadcast_frame?(m) end)
    assert frame2 == %{"identifier" => identifier, "message" => "m2"}
    assert Process.whereis(listener) == listener_pid
    Client.close(client)
  end
end
