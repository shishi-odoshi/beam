# frozen_string_literal: true

# Enqueue COUNT ActiveJob-enveloped jobs through the real Solid Queue
# adapter: ruby enqueue.rb QUEUE COUNT PREFIX LOG_PATH [JOB_CLASS]

require_relative "boot"

queue, count, prefix, log_path, klass_name = ARGV
klass = Object.const_get(klass_name || "MarkerJob")

Integer(count).times do |i|
  job = klass.set(queue: queue).perform_later(log_path, "#{prefix}#{i}")
  raise "enqueue failed" unless job
end

puts "ENQUEUED #{count} #{klass} to #{queue}"
