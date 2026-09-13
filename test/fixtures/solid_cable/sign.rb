# frozen_string_literal: true

# Prints the REAL turbo-rails signed stream name for ARGV[0] — the exact
# string `<%= turbo_stream_from ... %>` embeds in a page. Signing goes
# through Turbo::Streams::StreamName#signed_stream_name →
# Turbo.signed_stream_verifier (ActiveSupport::MessageVerifier, SHA256
# digest, JSON serializer) with the key derived in boot.rb exactly like the
# turbo engine initializer.

require_relative "boot"

signer = Object.new.extend(Turbo::Streams::StreamName)
puts signer.signed_stream_name(ARGV.fetch(0))
