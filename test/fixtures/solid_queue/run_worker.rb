# frozen_string_literal: true

# Runs a genuine SolidQueue::Worker (registration, heartbeats, claiming, the
# lot) against QUEUE for SECONDS: ruby run_worker.rb QUEUE SECONDS [READY_FILE]
#
# READY_FILE, when given, is touched the moment the worker thread is started
# so the Elixir test can synchronize the concurrent burst.

require_relative "boot"

queue, seconds, ready_file = ARGV

worker = SolidQueue::Worker.new(queues: [queue], threads: 3, polling_interval: 0.1)
worker.start # async mode: spawns the worker thread
File.write(ready_file, "ready") if ready_file

sleep Float(seconds)
worker.stop # joins the thread; runs the shutdown callbacks (deregister)

puts "WORKER_DONE queue=#{queue}"
