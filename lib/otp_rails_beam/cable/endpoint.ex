defmodule OtpRailsBeam.Cable.Endpoint do
  @moduledoc """
  The HTTP entry point: upgrades requests at the configured mount path
  (`/cable` — `ActionCable::INTERNAL[:default_mount_path]`) to a
  `OtpRailsBeam.Cable.Socket` WebSocket, negotiating the ActionCable
  subprotocol.

  Negotiation mirrors what websocket-driver does for Action Cable: the
  server's supported list is `ActionCable::INTERNAL[:protocols]`
  (`["actioncable-v1-json", "actioncable-unsupported"]`) and the FIRST
  protocol the client offers that the server supports is selected and
  echoed in the `Sec-WebSocket-Protocol` response header.
  @rails/actioncable offers both, so a real client always negotiates
  `actioncable-v1-json`. A client offering no known subprotocol still
  connects with no protocol header (Action Cable does not refuse it —
  its JS client is the side that hangs up on "actioncable-unsupported").
  """

  @behaviour Plug

  import Plug.Conn

  # ActionCable::INTERNAL[:protocols], order preserved.
  @protocols ["actioncable-v1-json", "actioncable-unsupported"]

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, opts) do
    path = Keyword.get(opts, :path, "/cable")

    if conn.method == "GET" and conn.request_path == path do
      upgrade(conn, opts)
    else
      send_resp(conn, 404, "Page not found")
    end
  end

  defp upgrade(conn, opts) do
    conn =
      case negotiate_subprotocol(conn) do
        nil -> conn
        protocol -> put_resp_header(conn, "sec-websocket-protocol", protocol)
      end

    WebSockAdapter.upgrade(conn, OtpRailsBeam.Cable.Socket, opts, timeout: :infinity)
  rescue
    # Not a WebSocket upgrade request (missing/invalid upgrade headers):
    # Action Cable's respond_to_invalid_request is a 404.
    _e in [WebSockAdapter.UpgradeError] ->
      send_resp(conn, 404, "Page not found")
  end

  # Client protocols come comma-separated, possibly across repeated
  # headers. First client-offered protocol we support wins.
  defp negotiate_subprotocol(conn) do
    conn
    |> get_req_header("sec-websocket-protocol")
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.map(&String.trim/1)
    |> Enum.find(&(&1 in @protocols))
  end
end
