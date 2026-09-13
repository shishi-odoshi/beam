# frozen_string_literal: true

# Loads every solid_queue_failed_executions row through the real
# SolidQueue::FailedExecution model and prints one JSON line per row — the
# proof that failures written by the BEAM worker are well-formed for the
# Ruby side's retry/discard tooling.

require_relative "boot"
require "json"

SolidQueue::FailedExecution.order(:job_id).each do |fe|
  puts({
    job_id: fe.job_id,
    class_name: fe.job.class_name,
    exception_class: fe.exception_class,
    message: fe.message,
    backtrace_size: (fe.backtrace || []).size
  }.to_json)
end
