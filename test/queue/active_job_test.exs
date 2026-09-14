defmodule OdoshiBeam.Queue.ActiveJobTest do
  use ExUnit.Case, async: true

  alias OdoshiBeam.Queue.ActiveJob

  defp envelope(args, extra \\ %{}) do
    %{
      "job_class" => "HardJob",
      "job_id" => "0917afd6-8c34-4d21-9dd4-4b76e1a3d1db",
      "queue_name" => "elixir",
      "arguments" => args,
      "executions" => 0,
      "exception_executions" => %{},
      "locale" => "en",
      "enqueued_at" => "2026-09-13T00:00:00.000000000Z"
    }
    |> Map.merge(extra)
    |> Jason.encode!()
  end

  test "decodes plain JSON arguments unchanged" do
    args = [1, "a", true, nil, 2.5, [1, 2], %{"k" => "v"}]

    assert {:ok, %{job_class: "HardJob", arguments: ^args}} = ActiveJob.decode(envelope(args))
  end

  test "strips ActiveJob hash markers, keeping string keys" do
    args = [
      %{"_aj_symbol_keys" => ["retries"], "retries" => 3},
      %{"_aj_hash_with_indifferent_access" => true, "a" => %{"_aj_symbol_keys" => [], "b" => 1}},
      %{"_aj_ruby2_keywords" => ["kw"], "kw" => "x"}
    ]

    assert {:ok, %{arguments: [%{"retries" => 3}, %{"a" => %{"b" => 1}}, %{"kw" => "x"}]}} =
             ActiveJob.decode(envelope(args))
  end

  test "rejects GlobalID references (out of scope v1)" do
    args = [%{"_aj_globalid" => "gid://app/User/1"}]

    assert {:error, message} = ActiveJob.decode(envelope(args))
    assert message =~ "GlobalID"
  end

  test "rejects custom-serialized objects, even nested" do
    args = [%{"outer" => [%{"_aj_serialized" => "ActiveJob::Serializers::TimeSerializer"}]}]

    assert {:error, message} = ActiveJob.decode(envelope(args))
    assert message =~ "custom serializer"
  end

  test "rejects NULL, invalid JSON, and non-envelope payloads" do
    assert {:error, message} = ActiveJob.decode(nil)
    assert message =~ "no ActiveJob envelope"

    assert {:error, message} = ActiveJob.decode("{nope")
    assert message =~ "invalid JSON"

    assert {:error, message} = ActiveJob.decode(~s({"foo": 1}))
    assert message =~ "not an ActiveJob envelope"
  end
end
