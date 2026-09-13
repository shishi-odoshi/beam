# frozen_string_literal: true
# Usage: ruby ruby_bad_heartbeater.rb MARKER
#
# Contract fixture: the REAL OtpRails::Heartbeat helper, but with a corrupted
# OTP_RAILS_TOKEN — the §5 contract says such lines are silently dropped.
# Beats for ~0.5s (several beats at 0.1s), touches MARKER to prove the beats
# were sent, then stays alive silently. If the supervisor wrongly accepted a
# beat, the child would become heartbeat-active and the ensuing silence would
# age it into :dead and force a restart; if dropped (correct), the child stays
# passive and healthy.
ENV["OTP_RAILS_TOKEN"] = "wrong-token"
require "otp_rails/heartbeat"
require "fileutils"

marker = ARGV.fetch(0)

hb = OtpRails::Heartbeat.new(id: "hb", interval: 0.1)
abort "not supervised: OTP_RAILS_SOCK missing" unless hb.start

sleep 0.5
FileUtils.touch(marker)
sleep
