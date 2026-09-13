# frozen_string_literal: true

# (Re)creates the Solid Cable schema from the gem's own installer template —
# the schema authority. Creates the database first if it doesn't exist
# (locally the docker container only pre-creates the queue suite's DB), then
# `create_table force: :cascade` drops previous runs.

require "pg"

db_name = ENV.fetch("SOLID_CABLE_PG_DATABASE", "otp_rails_beam_cable_test")
admin = PG.connect(
  host: ENV.fetch("SOLID_CABLE_PG_HOST", "127.0.0.1"),
  port: Integer(ENV.fetch("SOLID_CABLE_PG_PORT", "55433")),
  user: ENV.fetch("SOLID_CABLE_PG_USER", "postgres"),
  password: ENV.fetch("SOLID_CABLE_PG_PASSWORD", "postgres"),
  dbname: "postgres"
)
if admin.exec_params("SELECT 1 FROM pg_database WHERE datname = $1", [db_name]).ntuples.zero?
  admin.exec("CREATE DATABASE #{admin.quote_ident(db_name)}")
end
admin.close

require_relative "boot"

ActiveRecord::Migration.verbose = false
load SOLID_CABLE_SCHEMA
puts "SCHEMA_LOADED #{SolidCable::VERSION}"
