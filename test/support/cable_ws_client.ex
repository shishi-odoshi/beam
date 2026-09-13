defmodule OtpRailsBeam.CableWsClient do
  @moduledoc """
  A minimal RFC 6455 WebSocket client for the cable interop tests — plain
  `:gen_tcp`, no test dependency, just enough protocol for what the suite
  asserts: the opening handshake (including `Sec-WebSocket-Protocol`
  negotiation and `Sec-WebSocket-Accept` validation), masked text frames
  out, unfragmented text/close frames in.
  """

  import ExUnit.Assertions

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  defstruct [:sock, :protocol, buffer: <<>>]

  @doc "Open + handshake. Returns the client struct with the negotiated subprotocol (or nil)."
  def connect!(port, opts \\ []) do
    path = Keyword.get(opts, :path, "/cable")
    protocols = Keyword.get(opts, :protocols, ["actioncable-v1-json", "actioncable-unsupported"])

    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw], 5_000)

    key = Base.encode64(:crypto.strong_rand_bytes(16))

    protocol_header =
      case protocols do
        [] -> ""
        list -> "Sec-WebSocket-Protocol: #{Enum.join(list, ", ")}\r\n"
      end

    :ok =
      :gen_tcp.send(sock, [
        "GET #{path} HTTP/1.1\r\n",
        "Host: 127.0.0.1:#{port}\r\n",
        "Upgrade: websocket\r\n",
        "Connection: Upgrade\r\n",
        "Sec-WebSocket-Key: #{key}\r\n",
        "Sec-WebSocket-Version: 13\r\n",
        protocol_header,
        "\r\n"
      ])

    {headers_blob, rest} = read_until_headers_end(sock, <<>>)
    [status_line | header_lines] = String.split(headers_blob, "\r\n", trim: true)
    assert status_line =~ "101", "expected 101 Switching Protocols, got: #{status_line}"

    headers =
      Map.new(header_lines, fn line ->
        [name, value] = String.split(line, ":", parts: 2)
        {String.downcase(String.trim(name)), String.trim(value)}
      end)

    expected_accept = Base.encode64(:crypto.hash(:sha, key <> @guid))
    assert headers["sec-websocket-accept"] == expected_accept

    %__MODULE__{sock: sock, protocol: headers["sec-websocket-protocol"], buffer: rest}
  end

  defp read_until_headers_end(sock, acc) do
    case :binary.match(acc, "\r\n\r\n") do
      {pos, 4} ->
        <<headers::binary-size(pos), _::binary-size(4), rest::binary>> = acc
        {headers, rest}

      :nomatch ->
        {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
        read_until_headers_end(sock, acc <> data)
    end
  end

  @doc "Send one masked text frame."
  def send_text(%__MODULE__{sock: sock}, payload) when is_binary(payload) do
    mask = :crypto.strong_rand_bytes(4)
    masked = mask_payload(payload, mask)
    len = byte_size(payload)

    length_field =
      cond do
        len < 126 -> <<1::1, len::7>>
        len < 65_536 -> <<1::1, 126::7, len::16>>
        true -> <<1::1, 127::7, len::64>>
      end

    # FIN=1, RSV=000, opcode=1 (text); MASK bit is the leading 1 of length_field.
    :ok = :gen_tcp.send(sock, [<<0x81>>, length_field, mask, masked])
  end

  def send_json(client, map), do: send_text(client, Jason.encode!(map))

  @doc """
  Receive the next complete frame. Returns `{{:text, payload} | {:close, code}, client}`
  or `{:timeout, client}`.
  """
  def recv_frame(%__MODULE__{} = client, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_recv_frame(client, deadline)
  end

  defp do_recv_frame(client, deadline) do
    case parse_frame(client.buffer) do
      {:ok, frame, rest} ->
        {frame, %{client | buffer: rest}}

      :more ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          {:timeout, client}
        else
          case :gen_tcp.recv(client.sock, 0, remaining) do
            {:ok, data} -> do_recv_frame(%{client | buffer: client.buffer <> data}, deadline)
            {:error, :timeout} -> {:timeout, client}
            {:error, reason} -> {{:error, reason}, client}
          end
        end
    end
  end

  # Server frames are unmasked; Bandit sends unfragmented text frames.
  defp parse_frame(<<_fin::1, _rsv::3, opcode::4, 0::1, len::7, rest::binary>>) do
    {payload_len, rest} =
      case len do
        126 ->
          case rest do
            <<l::16, r::binary>> -> {l, r}
            _ -> {:more, nil}
          end

        127 ->
          case rest do
            <<l::64, r::binary>> -> {l, r}
            _ -> {:more, nil}
          end

        _ ->
          {len, rest}
      end

    with true <- payload_len != :more,
         <<payload::binary-size(payload_len), remainder::binary>> <- rest do
      case opcode do
        1 ->
          {:ok, {:text, payload}, remainder}

        8 ->
          code =
            case payload do
              <<c::16, _::binary>> -> c
              _ -> nil
            end

          {:ok, {:close, code}, remainder}

        _other ->
          # ping/pong/binary: not asserted on; skip the frame.
          {:ok, {:skipped, opcode}, remainder}
      end
    else
      _ -> :more
    end
  end

  defp parse_frame(_buffer), do: :more

  @doc "Receive and JSON-decode the next text frame (raising on timeout/close)."
  def recv_json!(client, timeout \\ 5_000) do
    case recv_frame(client, timeout) do
      {{:text, payload}, client} -> {Jason.decode!(payload), client}
      {other, _client} -> flunk("expected a text frame, got: #{inspect(other)}")
    end
  end

  @doc "Receive JSON frames until `fun` returns truthy for one; pings etc. are discarded."
  def recv_json_until!(client, timeout \\ 5_000, fun) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_recv_json_until(client, deadline, fun)
  end

  defp do_recv_json_until(client, deadline, fun) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)
    {message, client} = recv_json!(client, remaining)

    if fun.(message) do
      {message, client}
    else
      do_recv_json_until(client, deadline, fun)
    end
  end

  @doc """
  Assert that no frame matching `fun` arrives within `window_ms` (pings and
  other frames may still flow).
  """
  def refute_json!(client, window_ms, fun) do
    deadline = System.monotonic_time(:millisecond) + window_ms
    do_refute_json(client, deadline, fun)
  end

  defp do_refute_json(client, deadline, fun) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      client
    else
      case recv_frame(client, remaining) do
        {:timeout, client} ->
          client

        {{:text, payload}, client} ->
          message = Jason.decode!(payload)
          refute fun.(message), "unexpected frame arrived: #{inspect(message)}"
          do_refute_json(client, deadline, fun)

        {_other, client} ->
          do_refute_json(client, deadline, fun)
      end
    end
  end

  def close(%__MODULE__{sock: sock}), do: :gen_tcp.close(sock)

  defp mask_payload(payload, mask), do: mask_payload(payload, mask, 0, [])

  defp mask_payload(<<>>, _mask, _i, acc), do: Enum.reverse(acc)

  defp mask_payload(<<byte, rest::binary>>, mask, i, acc) do
    mask_payload(rest, mask, i + 1, [Bitwise.bxor(byte, :binary.at(mask, rem(i, 4))) | acc])
  end
end
