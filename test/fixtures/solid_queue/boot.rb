# frozen_string_literal: true

# Boots the real Solid Queue gem standalone (no Rails application), connected
# to the interop-test Postgres. Everything the engine would normally wire up
# for a Rails app is done explicitly here — and ONLY engine wiring: all queue
# behavior below is the gem's own code.

require "rails"
require "active_support/all"
require "active_record"
require "solid_queue"

# The engine's autoload paths never get mounted without a Rails app boot, so
# put the gem's app/ directories on a Zeitwerk loader ourselves.
gem_path = Gem.loaded_specs["solid_queue"].full_gem_path
loader = Zeitwerk::Loader.new
loader.push_dir(File.join(gem_path, "app/models"))
loader.push_dir(File.join(gem_path, "app/jobs"))
loader.setup

# Engine initializer "solid_queue.active_job.extensions", by hand:
ActiveJob::Base.include ActiveJob::ConcurrencyControls
ActiveJob::Base.include ActiveJob::BatchId

ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  host: ENV.fetch("SOLID_QUEUE_PG_HOST", "127.0.0.1"),
  port: Integer(ENV.fetch("SOLID_QUEUE_PG_PORT", "55433")),
  username: ENV.fetch("SOLID_QUEUE_PG_USER", "postgres"),
  password: ENV.fetch("SOLID_QUEUE_PG_PASSWORD", "postgres"),
  database: ENV.fetch("SOLID_QUEUE_PG_DATABASE", "odoshi_beam_queue_test"),
  pool: 10
)

ActiveRecord::Base.logger = nil
SolidQueue.logger = Logger.new(File::NULL)
ActiveJob::Base.logger = Logger.new(File::NULL)

SOLID_QUEUE_SCHEMA = File.join(
  gem_path, "lib/generators/solid_queue/install/templates/db/queue_schema.rb"
)

require_relative "jobs"
