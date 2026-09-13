# frozen_string_literal: true

# The ActiveJob classes both sides of the interop suite share. The queue each
# job lands on is chosen at enqueue time (`set(queue:)`) — routing between
# Ruby and Elixir workers is BY QUEUE, never by class, exactly like Tim's
# recorded Phase 4 decision.

# Appends "ruby <id>" to a shared log file when a RUBY worker executes it;
# the Elixir handler for the same class appends "beam <id>". The log is how
# the tests prove which side executed each job — and that nothing ran twice.
class MarkerJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform(log_path, id)
    File.open(log_path, "a") { |f| f.write("ruby #{id}\n") }
  end
end

# Enqueued to the elixir queue so the BEAM-side failing handler runs it; the
# Ruby body exists only so ActiveJob can serialize the class.
class FailingJob < ActiveJob::Base
  self.queue_adapter = :solid_queue

  def perform(*)
    raise "FailingJob must only ever be executed by the Elixir side in these tests"
  end
end
