defmodule OdoshiBeam.Cable.SignedStreamName do
  @moduledoc """
  Verifies Turbo signed stream names — the exact scheme turbo-rails (2.0.x)
  uses for `Turbo::StreamsChannel` subscriptions:

  * The verifier is `ActiveSupport::MessageVerifier.new(key, digest: "SHA256",
    serializer: JSON)` (`lib/turbo-rails.rb`, `Turbo.signed_stream_verifier`).
  * The key is `Rails.application.key_generator.generate_key(
    "turbo/signed_stream_verifier_key")` (turbo engine initializer
    `turbo.signed_stream_verifier_key`), i.e. PBKDF2-HMAC over
    `secret_key_base` with 1000 iterations (railties
    `Rails::Application#key_generator`), a 64-byte output
    (`ActiveSupport::KeyGenerator#generate_key` default `key_size = 64`), and
    the digest from `ActiveSupport::KeyGenerator.hash_digest_class` — SHA256
    for apps on `config.load_defaults 7.0`+ (all Rails 8 apps), SHA1 for
    older defaults.
  * A signed message is `Base64.strict_encode64(JSON.dump(name)) <> "--" <>
    OpenSSL::HMAC.hexdigest("SHA256", key, data)` — see
    `ActiveSupport::MessageVerifier#sign_encoded` / `#generate_digest`.
    turbo-rails signs without `purpose:`/`expires_*`, so the payload is the
    plain JSON-serialized name, never a `{"_rails": ...}` metadata envelope
    (`ActiveSupport::Messages::Metadata#serialize_with_metadata` only wraps
    when metadata options are present).
  """

  @salt "turbo/signed_stream_verifier_key"
  @iterations 1000
  @key_size 64
  # OpenSSL::HMAC.hexdigest("SHA256", ...) is 64 lowercase hex chars.
  @digest_hex_length 64
  @separator "--"

  @doc """
  Derive the Turbo signed-stream verifier key from a Rails
  `secret_key_base`, exactly like `Rails.application.key_generator.
  generate_key("turbo/signed_stream_verifier_key")`. `digest` is the app's
  `ActiveSupport::KeyGenerator.hash_digest_class`: `:sha256` (the
  `load_defaults 7.0`+ / Rails 8 value, default here) or `:sha1` (pre-7.0
  defaults).
  """
  def derive_key(secret_key_base, digest \\ :sha256)
      when is_binary(secret_key_base) and digest in [:sha256, :sha1] do
    # :crypto names SHA-1 :sha.
    crypto_digest = if digest == :sha1, do: :sha, else: digest
    :crypto.pbkdf2_hmac(crypto_digest, secret_key_base, @salt, @iterations, @key_size)
  end

  @doc """
  Verify a signed stream name against the derived key. Returns
  `{:ok, stream_name}` or `:error` — mirroring
  `ActiveSupport::MessageVerifier#verified`, which returns nil on any
  tampered/malformed input.
  """
  def verify(signed, key) when is_binary(signed) and is_binary(key) do
    with {:ok, data, digest} <- split(signed),
         true <- digest_matches?(data, digest, key),
         {:ok, json} <- decode64(data),
         {:ok, name} when is_binary(name) <- Jason.decode(json) do
      {:ok, name}
    else
      _ -> :error
    end
  end

  def verify(_signed, _key), do: :error

  # MessageVerifier#separator_index_for computes the split point from the
  # expected digest length (digest is the LAST digest_length_in_hex chars,
  # preceded by "--") rather than searching for the separator.
  defp split(signed) do
    data_length = byte_size(signed) - @digest_hex_length - byte_size(@separator)

    with true <- data_length > 0,
         <<data::binary-size(data_length), sep::binary-size(2), digest::binary>> <- signed,
         true <- sep == @separator do
      {:ok, data, digest}
    else
      _ -> :error
    end
  end

  defp digest_matches?(data, digest, key) do
    expected =
      :crypto.mac(:hmac, :sha256, key, data)
      |> Base.encode16(case: :lower)

    # Both are 64 bytes here, so hash_equals/2 (constant-time) applies —
    # the analog of Ruby's ActiveSupport::SecurityUtils.secure_compare.
    byte_size(digest) == @digest_hex_length and :crypto.hash_equals(expected, digest)
  end

  # MessageVerifier's codec tries the configured encoding then falls back to
  # the other (`decode` rescue in message_verifier.rb); turbo's verifier is
  # url_safe: false, so strict Base64 first, URL-safe second.
  defp decode64(data) do
    case Base.decode64(data) do
      {:ok, json} -> {:ok, json}
      :error -> Base.url_decode64(data, padding: false)
    end
  end
end
