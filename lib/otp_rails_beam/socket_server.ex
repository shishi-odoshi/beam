defmodule OtpRailsBeam.SocketServer do
  @moduledoc """
  DESIGN §5 active heartbeats + §9 control transport — the frozen wire
  contract, mirroring `OtpRails::SocketServer` in the Ruby gem exactly.

  Newline-delimited JSON over a Unix domain socket, mode 0600, per-boot token.
  No MessagePack, no length prefixes, no versions, no acks.

      Heartbeat: {"id":"jobs","state":"healthy","ts":1757700000,"token":"…","meta":{}}
      Control:   {"cmd":"restart","id":"jobs","token":"…"}

  Any line with a missing or wrong token is dropped without a reply.
  Malformed JSON is dropped. Unknown commands and unknown ids are ignored.
  Messages with a `cmd` key are control; otherwise `id` + `state` make a
  heartbeat (same dispatch order as the Ruby reference).
  """

  use GenServer

  alias OtpRailsBeam.Root

  def start_link(ctx), do: GenServer.start_link(__MODULE__, ctx)

  @impl true
  def init(ctx) do
    Process.flag(:trap_exit, true)
    path = ctx.socket_path
    File.mkdir_p!(Path.dirname(path))
    # stale socket from a dead boot
    _ = File.rm(path)

    # Raw mode with manual line reassembly, NOT `packet: :line`: line mode
    # silently truncates lines longer than the receive buffer (~1400 bytes by
    # default), and the truncated halves then fail JSON parsing and are
    # dropped — whereas the Ruby reference (`conn.each_line`) accepts §5
    # lines of any length. The contract sets no line-length bound.
    {:ok, listen} =
      :gen_tcp.listen(0, [
        :binary,
        active: false,
        ifaddr: {:local, String.to_charlist(path)}
      ])

    File.chmod!(path, 0o600)
    acceptor = spawn_link(fn -> accept_loop(listen, ctx) end)
    {:ok, %{listen: listen, ctx: ctx, acceptor: acceptor}}
  end

  @impl true
  def handle_call(:ctx, _from, state), do: {:reply, state.ctx, state}

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.listen)
    _ = File.rm(state.ctx.socket_path)
    :ok
  end

  defp accept_loop(listen, ctx) do
    case :gen_tcp.accept(listen) do
      {:ok, conn} ->
        handler = spawn(fn -> await_and_serve(conn, ctx) end)
        :ok = :gen_tcp.controlling_process(conn, handler)
        send(handler, :go)
        accept_loop(listen, ctx)

      {:error, _closed} ->
        :ok
    end
  end

  defp await_and_serve(conn, ctx) do
    receive do
      :go -> serve(conn, ctx)
    end
  end

  defp serve(conn, ctx, acc \\ "") do
    case :gen_tcp.recv(conn, 0) do
      {:ok, data} ->
        {lines, rest} = split_lines(acc <> data)
        Enum.each(lines, &handle_line(&1, ctx))
        serve(conn, ctx, rest)

      {:error, _} ->
        :gen_tcp.close(conn)
    end
  end

  # NDJSON framing: complete lines plus the trailing partial (kept as the
  # accumulator until its newline arrives).
  defp split_lines(buf) do
    case String.split(buf, "\n") do
      [partial] -> {[], partial}
      parts -> {Enum.drop(parts, -1), List.last(parts)}
    end
  end

  defp handle_line(line, ctx) do
    case Jason.decode(line) do
      {:ok, %{} = msg} ->
        # Missing/wrong token ⇒ silently dropped (checked at the socket layer).
        if msg["token"] == ctx.token, do: dispatch(msg, ctx)

      _malformed ->
        :ok
    end
  end

  # Ruby dispatches on `if msg["cmd"]` — plain truthiness — so a JSON `null`
  # or `false` cmd falls through to the heartbeat branch there, and must here
  # too (verified divergence: `not is_nil/1` alone treated `false` as
  # control and silently ate an otherwise-valid authenticated heartbeat).
  defp dispatch(%{"cmd" => cmd} = msg, ctx) when cmd not in [nil, false] do
    handle_control(cmd, msg["id"], ctx)
  end

  defp dispatch(%{"id" => id, "state" => hb_state}, ctx)
       when not is_nil(id) and not is_nil(hb_state) do
    :ets.insert(ctx.table, {{:hb, id}, System.monotonic_time(:millisecond), hb_state})
  end

  defp dispatch(_msg, _ctx), do: :ok

  # {"cmd":"restart","id":...}: unknown commands and unknown ids are ignored.
  defp handle_control("restart", id, ctx) do
    if id in ctx.ids, do: Root.restart_child(ctx, id)
  end

  defp handle_control(_cmd, _id, _ctx), do: :ok
end
