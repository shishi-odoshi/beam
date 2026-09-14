defmodule OdoshiBeam.Queue.ActiveJob do
  @moduledoc """
  Decodes the standard ActiveJob JSON envelope that Solid Queue stores in
  `solid_queue_jobs.arguments`:

      {"job_class": "HardJob", "job_id": "uuid", "queue_name": "elixir",
       "arguments": [...], "executions": 0, ...}

  Argument coverage (v1, on purpose):

  * plain JSON types — strings, numbers, booleans, nil, arrays, objects —
    pass through unchanged (object keys stay strings);
  * ActiveJob's own hash markers (`_aj_symbol_keys`,
    `_aj_hash_with_indifferent_access`, `_aj_ruby2_keywords`) are stripped —
    Elixir has no symbol-vs-string key distinction worth preserving;
  * GlobalID references (`_aj_globalid`) and custom-serialized objects
    (`_aj_serialized`) are NOT supported: they reference Ruby objects/records
    an Elixir handler cannot deserialize. Jobs carrying them fail with a
    `DeserializationError`-shaped failed execution rather than executing with
    silently-wrong arguments. Route such jobs to Ruby-worker queues.
  """

  @unsupported_markers %{
    "_aj_globalid" => "GlobalID",
    "_aj_serialized" => "ActiveJob custom serializer"
  }

  @doc """
  Decode the raw `solid_queue_jobs.arguments` text into
  `{:ok, %{job_class: binary, arguments: list}}` or `{:error, message}`.
  """
  def decode(nil), do: {:error, "job has no ActiveJob envelope (arguments is NULL)"}

  def decode(text) when is_binary(text) do
    case Jason.decode(text) do
      {:ok, %{"job_class" => job_class, "arguments" => args}}
      when is_binary(job_class) and is_list(args) ->
        try do
          {:ok, %{job_class: job_class, arguments: decode_value(args)}}
        catch
          {:unsupported, what, value} ->
            {:error,
             "unsupported ActiveJob argument: #{what} (#{inspect(value)}) — " <>
               "GlobalID/serialized objects are out of scope for Elixir handlers (v1)"}
        end

      {:ok, other} ->
        {:error, "not an ActiveJob envelope: #{inspect(other, limit: 5, printable_limit: 200)}"}

      {:error, %Jason.DecodeError{} = e} ->
        {:error, "invalid JSON in arguments: #{Jason.DecodeError.message(e)}"}
    end
  end

  defp decode_value(map) when is_map(map) do
    Enum.each(@unsupported_markers, fn {marker, label} ->
      if Map.has_key?(map, marker), do: throw({:unsupported, label, Map.get(map, marker)})
    end)

    map
    |> Map.drop(["_aj_symbol_keys", "_aj_ruby2_keywords", "_aj_hash_with_indifferent_access"])
    |> Map.new(fn {k, v} -> {k, decode_value(v)} end)
  end

  defp decode_value(list) when is_list(list), do: Enum.map(list, &decode_value/1)
  defp decode_value(other), do: other
end
