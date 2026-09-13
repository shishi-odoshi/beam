# frozen_string_literal: true

# Runs Solid Queue's OWN dead-process pruning — the exact call the Ruby
# supervisor's maintenance TimerTask makes every process_alive_threshold
# (lib/solid_queue/supervisor/maintenance.rb#prune_dead_processes) — with the
# threshold shortened so tests need not wait 5 minutes:
#
#   ruby prune.rb THRESHOLD_SECONDS
#
# Processes with a heartbeat older than the threshold get their claimed
# executions failed with SolidQueue::Processes::ProcessPrunedError and their
# process row deleted. Live heartbeats are spared.

require_relative "boot"

SolidQueue.process_alive_threshold = Float(ARGV[0]).seconds
SolidQueue::Process.prune

puts "PRUNED processes=#{SolidQueue::Process.count} " \
     "claimed=#{SolidQueue::ClaimedExecution.count} " \
     "failed=#{SolidQueue::FailedExecution.count}"
