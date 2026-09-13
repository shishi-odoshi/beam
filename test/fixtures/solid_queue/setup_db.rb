# frozen_string_literal: true

# (Re)creates the Solid Queue schema from the gem's own installer template —
# the schema authority. `create_table force: :cascade` drops previous runs.

require_relative "boot"

ActiveRecord::Migration.verbose = false
load SOLID_QUEUE_SCHEMA
puts "SCHEMA_LOADED #{SolidQueue::VERSION}"
