# frozen_string_literal: true

# Broadcasts through the REAL Solid Cable pubsub path: Action Cable's
# server.broadcast → ActiveSupport::JSON-encodes the message →
# ActionCable::SubscriptionAdapter::SolidCable#broadcast →
# SolidCable::Message.broadcast inserts the solid_cable_messages row (and
# autotrim's TrimJob runs, exactly as in a Rails app).
#
# ARGV: stream_name, JSON-encoded message (string or object — a Turbo
# broadcast is a raw HTML string).

require_relative "boot"
require "json"

stream_name = ARGV.fetch(0)
message = JSON.parse(ARGV.fetch(1), quirks_mode: true)

cable_server.broadcast(stream_name, message)
puts "BROADCASTED #{SolidCable::Message.maximum(:id)}"
