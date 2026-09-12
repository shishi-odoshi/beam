# Usage: elixir heartbeater.exs INTERVAL_SECONDS STOPFLAG [ID]
#
# Sends NDJSON heartbeats (DESIGN §5) over OTP_RAILS_SOCK every INTERVAL
# seconds until STOPFLAG exists — then goes silent but stays alive, which is
# how a wedged-but-running child looks to the supervisor.
# Mirrors test/fixtures/heartbeater.rb in the Ruby gem.

[interval_s, stopflag | rest] = System.argv()
id = List.first(rest) || "hb"
{interval, _} = Float.parse(interval_s)
interval_ms = trunc(interval * 1000)

sock_path = System.fetch_env!("OTP_RAILS_SOCK")
token = System.get_env("OTP_RAILS_TOKEN", "")

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

loop = fn loop ->
  if not File.exists?(stopflag) do
    ts = System.os_time(:second)
    line = ~s({"id":"#{id}","state":"healthy","ts":#{ts},"token":"#{token}","meta":{}}\n)
    :gen_tcp.send(sock, line)
  end

  Process.sleep(interval_ms)
  loop.(loop)
end

loop.(loop)
