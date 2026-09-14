# frozen_string_literal: true

# Boots the real Solid Cable + Action Cable + turbo-rails stack standalone,
# connected to the interop-test Postgres. A minimal Rails::Application is
# defined because Solid Cable reads its adapter settings through
# `Rails.application.config_for("cable")` and turbo's verifier key comes
# from `Rails.application.key_generator` — but it is never initialize!d:
# everything below is explicit engine wiring, and ONLY engine wiring. All
# cable behavior is the gems' own code.

ENV["RAILS_ENV"] ||= "test"

require "rails"
require "active_support/all"
require "active_record"
require "active_job"
require "action_cable"
require "solid_cable"
require "turbo-rails"

class FixtureApp < Rails::Application
  config.root = __dir__
  config.eager_load = false
  # load_defaults 8.0 gives ActiveSupport::KeyGenerator.hash_digest_class =
  # SHA256 (railties configuration.rb, load_defaults 7.0+) — the derivation
  # beam mirrors. The verifier key is derived from this shared secret.
  config.load_defaults 8.0
  config.secret_key_base = ENV.fetch("CABLE_SECRET_KEY_BASE")
end

# The engine's autoload paths never get mounted without a Rails app boot, so
# put the gems' app/ directories on a Zeitwerk loader ourselves.
solid_cable_path = Gem.loaded_specs["solid_cable"].full_gem_path
turbo_path = Gem.loaded_specs["turbo-rails"].full_gem_path
loader = Zeitwerk::Loader.new
loader.push_dir(File.join(solid_cable_path, "app/models"))
loader.push_dir(File.join(solid_cable_path, "app/jobs"))
loader.push_dir(File.join(turbo_path, "app/channels"))
loader.setup

# activesupport railtie initializer
# "active_support.set_key_generator_hash_digest_class", by hand: load_defaults
# 7.0+ sets config.active_support.key_generator_hash_digest_class = SHA256,
# and the railtie (which only runs on a full app boot) applies it. Without
# this a real Rails 8 app and this fixture would derive different keys.
ActiveSupport::KeyGenerator.hash_digest_class =
  Rails.application.config.active_support.key_generator_hash_digest_class

# turbo-rails engine initializer "turbo.signed_stream_verifier_key", by hand
# (lib/turbo/engine.rb): the key is derived from secret_key_base via the
# app's key generator with turbo's salt.
Turbo.signed_stream_verifier_key =
  Rails.application.key_generator.generate_key("turbo/signed_stream_verifier_key")

ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  host: ENV.fetch("SOLID_CABLE_PG_HOST", "127.0.0.1"),
  port: Integer(ENV.fetch("SOLID_CABLE_PG_PORT", "55433")),
  username: ENV.fetch("SOLID_CABLE_PG_USER", "postgres"),
  password: ENV.fetch("SOLID_CABLE_PG_PASSWORD", "postgres"),
  database: ENV.fetch("SOLID_CABLE_PG_DATABASE", "odoshi_beam_cable_test"),
  pool: 10
)

ActiveRecord::Base.logger = nil
ActiveJob::Base.logger = Logger.new(File::NULL)

SOLID_CABLE_SCHEMA = File.join(
  solid_cable_path, "lib/generators/solid_cable/install/templates/db/cable_schema.rb"
)

# A standalone Action Cable server wired to the solid_cable adapter — the
# same pubsub object a running Rails app's ActionCable.server would hold.
def cable_server
  @cable_server ||= begin
    config = ActionCable::Server::Configuration.new
    config.cable = { "adapter" => "solid_cable" }.with_indifferent_access
    config.logger = Logger.new(File::NULL)
    ActionCable::Server::Base.new(config: config)
  end
end
