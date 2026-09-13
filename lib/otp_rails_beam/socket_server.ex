defmodule OtpRailsBeam.SocketServer do
  @moduledoc """
  DESIGN §5 active heartbeats + §9 control transport — the frozen wire
  contract, mirroring `OtpRails::SocketServer` in the Ruby gem exactly.

  Newline-delimited JSON over a Unix domain socket, mode 0600, per-boot token.
  No MessagePack, no length prefixes, no versions, no acks.

      Heartbeat: {"id":"jobs","state":"healthy","ts":1757700000,"token":"…","meta":{}}
      Control:   {"cmd":"restart","id":"jobs","token":"…"}

  Any line with a missing or wrong token is dropped without a reply.
  Malformed JSON is dropped. Lines longer than 64 KiB are malformed too:
  dropped without unbounded buffering, resyncing at the next newline.
  `token`, `cmd`, `id`, and `state` must be JSON strings; a non-string value
  in any of them drops the line, same as a bad token. A message with a `cmd`
  key is control-shaped (it never falls through to the heartbeat branch);
  otherwise string `id` + `state` make a heartbeat — same dispatch as the
  hardened Ruby reference. Unknown commands and unknown ids are ignored.
  """

  use GenServer

  alias OtpRailsBeam.Root

  # §5: max line length, INCLUDING the newline — longer lines are malformed.
  # Mirrors the Ruby reference's `conn.gets("\n", MAX_LINE)` exactly: a line
  # is accepted iff its content + "\n" fits in 64 KiB.
  @max_line 64 * 1024

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
    # dropped. The §5 contract accepts lines up to 64 KiB and treats longer
    # ones as malformed (dropped, resync at the next newline).
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

  defp serve(conn, ctx, acc \\ "", discard? \\ false) do
    case :gen_tcp.recv(conn, 0) do
      {:ok, data} ->
        {lines, rest, discard?} = split_lines(acc <> data, discard?)
        Enum.each(lines, &handle_line(&1, ctx))
        serve(conn, ctx, rest, discard?)

      {:error, _} ->
        :gen_tcp.close(conn)
    end
  end

  # NDJSON framing under the §5 line cap: complete lines within @max_line
  # (content + newline) are accepted; an over-long line is dropped WITHOUT
  # buffering it — `discard?` rides until its terminating newline, where the
  # stream resyncs. The trailing partial is kept as the accumulator only
  # while it can still become an acceptable line, so memory stays bounded
  # below @max_line regardless of what a client streams.
  defp split_lines(buf, discard?) do
    {complete, [partial]} = buf |> String.split("\n") |> Enum.split(-1)

    {lines, discard?} =
      Enum.reduce(complete, {[], discard?}, fn line, {acc, discarding?} ->
        cond do
          # The tail of an over-long line: its newline ends the discard.
          discarding? -> {acc, false}
          # Content + "\n" would exceed the cap: malformed, dropped.
          byte_size(line) >= @max_line -> {acc, false}
          true -> {[line | acc], false}
        end
      end)

    if discard? or byte_size(partial) >= @max_line do
      {Enum.reverse(lines), "", true}
    else
      {Enum.reverse(lines), partial, discard?}
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

  # §5 hardening, mirroring the Ruby reference: `cmd`, `id`, and `state`
  # must be JSON strings (`token` too — the equality check against the
  # binary token enforces that for free). Any message carrying a `cmd` key
  # is control-shaped: a non-string cmd drops the whole line, it never falls
  # through to the heartbeat branch. Heartbeats with a non-string id or
  # state are dropped likewise, and heartbeats for ids that are not children
  # of this tree are dropped at intake so ghost ids cannot grow the table.
  defp dispatch(%{"cmd" => cmd} = msg, ctx) when is_binary(cmd) do
    handle_control(cmd, msg["id"], ctx)
  end

  defp dispatch(%{"cmd" => _non_string}, _ctx), do: :ok

  defp dispatch(%{"id" => id, "state" => hb_state}, ctx)
       when is_binary(id) and is_binary(hb_state) do
    if id in ctx.ids do
      :ets.insert(ctx.table, {{:hb, id}, System.monotonic_time(:millisecond), hb_state})
    end
  end

  defp dispatch(_msg, _ctx), do: :ok

  # {"cmd":"restart","id":...}: unknown commands and unknown ids are ignored.
  defp handle_control("restart", id, ctx) do
    if id in ctx.ids, do: Root.restart_child(ctx, id)
  end

  defp handle_control(_cmd, _id, _ctx), do: :ok
end
