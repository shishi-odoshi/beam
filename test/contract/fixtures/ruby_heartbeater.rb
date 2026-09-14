# frozen_string_literal: true
# Usage: ruby ruby_heartbeater.rb INTERVAL STOPFLAG [ID]
#
# Contract fixture: heartbeats with the REAL Odoshi::Heartbeat helper from
# the published odoshi gem — not a hand-rolled socket writer — so the beam
# supervisor is tested against the exact bytes Rails children will send.
# When STOPFLAG appears the heartbeat thread is stopped but the process stays
# alive: a wedged-but-running child.
require "odoshi/heartbeat"

interval, stopflag, id = ARGV[0].to_f, ARGV[1], (ARGV[2] || "hb")

hb = Odoshi::Heartbeat.new(id: id, interval: interval)
abort "not supervised: ODOSHI_SOCK / ODOSHI_TOKEN missing" unless hb.start

sleep 0.05 until File.exist?(stopflag)
hb.stop
sleep
