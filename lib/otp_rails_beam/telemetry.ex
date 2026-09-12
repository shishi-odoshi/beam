defmodule OtpRailsBeam.Telemetry do
  @moduledoc """
  The DESIGN §6 event names, verbatim. This list is a published contract shared
  with the Ruby `otp-rails` gem — never add, rename, or remove an event here
  without a DESIGN §6 edit on the Ruby side first.
  """

  @events [
    [:otp_rails, :supervisor, :start],
    [:otp_rails, :supervisor, :stop],
    [:otp_rails, :supervisor, :escalate],
    [:otp_rails, :child, :spawn],
    [:otp_rails, :child, :healthy],
    [:otp_rails, :child, :degraded],
    [:otp_rails, :child, :exit],
    [:otp_rails, :child, :restart],
    [:otp_rails, :child, :drain],
    [:otp_rails, :child, :kill]
  ]

  @doc "All contract event names (for `:telemetry.attach_many/4`)."
  def events, do: @events

  @doc "Emit a contract event. Raises at call time if the name is not in §6."
  def emit(event, measurements \\ %{}, metadata \\ %{}) do
    if event not in @events do
      raise ArgumentError, "#{inspect(event)} is not a DESIGN §6 telemetry event"
    end

    :telemetry.execute(event, measurements, metadata)
  end
end
