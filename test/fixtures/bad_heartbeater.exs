# Usage: elixir bad_heartbeater.exs MARKER
#
# Sends one heartbeat with a WRONG token, touches MARKER to prove it was sent,
# then stays alive silently. If the supervisor wrongly accepted the heartbeat,
# the child would become "active" and the ensuing silence would get it
# restarted; if the heartbeat is dropped (correct), the child stays passive
# and healthy. Mirrors test/fixtures/bad_heartbeater.rb in the Ruby gem.

[marker | _] = System.argv()
sock_path = System.fetch_env!("ODOSHI_SOCK")

connect = fn connect, attempts ->
  case :gen_tcp.connect({:local, String.to_charlist(sock_path)}, 0, [:binary, active: false]) do
    {:ok, sock} ->
      sock

    {:error, _} when attempts > 0 ->
      Process.sleep(100)
      connect.(connect, attempts - 1)
  end
end

sock = connect.(connect, 50)

line =
  ~s({"id":"hb","state":"healthy","ts":#{System.os_time(:second)},"token":"wrong-token","meta":{}}\n)

:ok = :gen_tcp.send(sock, line)
File.touch!(marker)
Process.sleep(:infinity)
