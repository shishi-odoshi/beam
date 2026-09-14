defmodule OdoshiBeam.Cable.SignedStreamNameTest do
  # Pure unit tests against Ruby-produced vectors — no Postgres, no Ruby at
  # test time, so these run in the default `mix test` suite (the live
  # cross-language proofs are in cable_interop_test.exs, tagged :cable).
  use ExUnit.Case, async: true

  alias OdoshiBeam.Cable.{Listener, SignedStreamName}

  # Produced by the real stack (test/fixtures/solid_cable, rails 8.1 +
  # turbo-rails 2.0):
  #
  #   CABLE_SECRET_KEY_BASE=testsecret123 bundle exec ruby sign.rb "board:1"
  #
  # i.e. ActiveSupport::MessageVerifier(digest: SHA256, serializer: JSON)
  # keyed by PBKDF2-HMAC-SHA256(secret, "turbo/signed_stream_verifier_key",
  # 1000 iterations, 64 bytes).
  @secret "testsecret123"
  @signed "ImJvYXJkOjEi--7eadb36517dd962e290a5c55daad0a9a3ae0aa9a26ae0e72b7f78ae7eb0d5a3d"

  test "verifies a genuinely turbo-signed stream name" do
    key = SignedStreamName.derive_key(@secret)
    assert {:ok, "board:1"} = SignedStreamName.verify(@signed, key)
  end

  test "rejects a tampered digest" do
    key = SignedStreamName.derive_key(@secret)
    tampered = String.replace(@signed, "7ead", "8ead")
    assert :error = SignedStreamName.verify(tampered, key)
  end

  test "rejects a tampered payload (resigned data would be needed)" do
    key = SignedStreamName.derive_key(@secret)
    # "ImJvYXJkOjEi" is JSON "board:1"; swap a data char, keep the digest.
    tampered = String.replace(@signed, "ImJvYXJkOjEi", "ImJvYXJkOjIi")
    assert :error = SignedStreamName.verify(tampered, key)
  end

  test "rejects the wrong key, malformed input, and non-strings" do
    other_key = SignedStreamName.derive_key("some-other-secret")
    assert :error = SignedStreamName.verify(@signed, other_key)

    key = SignedStreamName.derive_key(@secret)
    assert :error = SignedStreamName.verify("", key)
    assert :error = SignedStreamName.verify("--", key)
    assert :error = SignedStreamName.verify("no-separator-here", key)
    assert :error = SignedStreamName.verify("f--46a0120593880c733a53b6dad75b42ddc1c8996d", key)
    assert :error = SignedStreamName.verify(nil, key)
    assert :error = SignedStreamName.verify(%{}, key)
  end

  test "sha1 key derivation is available for pre-load_defaults-7.0 apps" do
    # Only shape-checked here: 64-byte key, differing from sha256.
    sha1 = SignedStreamName.derive_key(@secret, :sha1)
    sha256 = SignedStreamName.derive_key(@secret, :sha256)
    assert byte_size(sha1) == 64
    assert byte_size(sha256) == 64
    refute sha1 == sha256
  end

  test "channel_hash matches SolidCable::Message.channel_hash_for" do
    # Digest::SHA256.digest("board:1").unpack1("q>") in the fixture bundle:
    assert Listener.channel_hash("board:1") == -8_834_535_658_113_905_626
  end
end
