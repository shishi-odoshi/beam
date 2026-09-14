defmodule OdoshiBeam.Cable.Listener do
  @moduledoc """
  Polls `solid_cable_messages` and fans rows out to subscribed sockets —
  the beam mirror of Solid Cable's `Listener` thread
  (`lib/action_cable/subscription_adapter/solid_cable.rb`, gem 3.0.x).
  Solid Cable is a POLLING adapter: its own Ruby listener wakes every
  `polling_interval` (0.1s default) and selects new rows, so beam doing the
  same is the native consumption model, not an approximation.

  Semantics mirrored from the Ruby listener:

  * **Poll cursor** — `last_id` starts at `Message.maximum(:id) || 0` when
    the listener boots (`Listener#last_message_id`) and advances to each
    selected row's id, so a fresh listener never delivers history.
  * **Per-channel baseline** — `add_channel` records `last_message_id` (a
    fresh `MAX(id)` query) the moment a channel gains its first subscriber;
    rows at or below that id are never delivered to it
    (`channel_last_id >= message.id` skip in `broadcast_messages`). This is
    how reconnects avoid replay: a re-subscribe is a new channel entry with
    a now-current baseline.
  * **Selection** — `Message.broadcastable(channels, last_id)`:
    `WHERE channel_hash IN (...) AND id > last_id ORDER BY id`. The hash is
    `Digest::SHA256.digest(channel).unpack1("q>")` — the first 8 bytes of
    the SHA-256 as a signed big-endian 64-bit integer (`Message.
    channel_hash_for`; signed because Postgres has no unsigned bigint).
    Hash collisions are resolved at fan-out by exact channel bytes, exactly
    like the Ruby side keying its subscriber map on `message.channel`.
  * **Channel removal** — when the last subscriber goes, the channel entry
    (and its baseline) is dropped (`remove_channel`), matching
    `SubscriberMap#remove_subscriber`.

  Trimming: Solid Cable's `TrimJob` (Rails side) deletes rows older than
  `message_retention` — beam only ever reads this table. The `id > cursor`
  predicate means trimmed history is naturally invisible; beam never
  deletes or writes `solid_cable_messages` rows.

  Subscribers receive `{:cable_broadcast, identifier, message}` where
  `message` is the JSON-decoded payload — the same decode ActionCable's
  default stream handler applies (`Channel::Streams#default_stream_handler`,
  `coder: ActiveSupport::JSON`) before framing the broadcast.

  ## DB outages (beam#9)

  Every DB touch is wrapped: `DBConnection.ConnectionError` never crashes
  the listener. Polls back off to 10x the polling interval and retry
  forever, so the listener rides out an outage of any length and resumes
  where its cursor left off when Postgres returns. The boot-time cursor is
  taken in `handle_continue` (or lazily at the first reachable poll) so a
  restart during an outage can't crash-loop `init`. A subscribe during an
  outage is refused with `{:error, :db_unavailable}` — its replay-guard
  baseline cannot be established — and the socket closes that connection
  (1013) so the client's monitor retries with backoff.

  The listener holds only soft state (subscriber map + baselines): when it
  crashes for a non-connection reason and restarts, sockets stay up (the
  supervisor is `:one_for_one`) and re-register themselves with the fresh
  listener, taking new baselines — see `OdoshiBeam.Cable.Socket`.
  """

  use GenServer
  require Logger

  # DB outage backoff: connection errors reschedule the poll at
  # polling_interval_ms * this multiplier (default 100ms -> 1s), the same
  # backoff Queue.Worker uses for failed claims. The listener process never
  # crashes on a connection error, so an outage of any length costs zero
  # supervisor restarts.
  @db_backoff_multiplier 10

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:name])

  @doc """
  Subscribe `pid` (a socket) under `identifier` to `stream`. Synchronous on
  purpose: the subscription confirmation must not be sent before the
  listener is watching the stream, mirroring ActionCable's
  success-callback ordering (confirm fires from `pubsub.subscribe`'s
  success callback).
  """
  def subscribe(listener, pid, identifier, stream) when is_binary(stream) do
    GenServer.call(listener, {:subscribe, pid, identifier, stream})
  end

  @doc "Drop `pid`'s subscription registered under `identifier`."
  def unsubscribe(listener, pid, identifier) do
    GenServer.call(listener, {:unsubscribe, pid, identifier})
  end

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    interval = Keyword.get(opts, :polling_interval_ms, 100)

    state = %{
      db: db,
      interval: interval,
      # Listener#last_id ||= last_message_id — the boot-time cursor. Set in
      # handle_continue (or lazily at the first successful poll) so a DB
      # outage at boot cannot crash-loop init and burn the supervisor's
      # restart budget while the DB is down.
      last_id: nil,
      # stream (exact channel bytes) => %{hash:, last_id:, subs: %{{pid, identifier} => true}}
      streams: %{},
      # pid => %{ref: monitor_ref, subs: %{identifier => [stream]}}
      pids: %{}
    }

    schedule_poll(interval)
    {:ok, state, {:continue, :init_cursor}}
  end

  @impl true
  def handle_continue(:init_cursor, state) do
    case try_max_message_id(state.db) do
      {:ok, max_id} ->
        {:noreply, %{state | last_id: max_id}}

      {:error, err} ->
        Logger.warning(
          "odoshi_beam.cable: cursor init deferred, DB unavailable (will retry at poll time): #{Exception.message(err)}"
        )

        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:subscribe, pid, identifier, stream}, _from, state) do
    case subscription_entry(state, stream) do
      # The baseline query needs the DB; without it a subscription would
      # have no replay guard. Reply with the error (the socket closes the
      # connection so the client's monitor retries later) instead of
      # crashing the listener while the DB is down.
      :db_unavailable ->
        {:reply, {:error, :db_unavailable}, state}

      entry ->
        add_subscription(state, pid, identifier, stream, entry)
    end
  end

  def handle_call({:unsubscribe, pid, identifier}, _from, state) do
    {:reply, :ok, drop_subscription(state, pid, identifier)}
  end

  defp subscription_entry(state, stream) do
    case state.streams[stream] do
      nil ->
        # SubscriberMap#add_subscriber -> add_channel on first subscriber:
        # baseline at the CURRENT max id, so nothing already in the table
        # is ever delivered to this subscription.
        case try_max_message_id(state.db) do
          {:ok, max_id} -> %{hash: channel_hash(stream), last_id: max_id, subs: %{}}
          {:error, _err} -> :db_unavailable
        end

      existing ->
        existing
    end
  end

  defp add_subscription(state, pid, identifier, stream, entry) do
    entry = put_in(entry.subs[{pid, identifier}], true)

    pid_entry =
      case state.pids[pid] do
        nil -> %{ref: Process.monitor(pid), subs: %{}}
        existing -> existing
      end

    pid_entry =
      update_in(pid_entry.subs, fn subs ->
        Map.update(subs, identifier, [stream], fn streams ->
          # Idempotent: sockets re-register after a listener restart, and a
          # partially-failed sweep may retry streams already registered.
          if stream in streams, do: streams, else: [stream | streams]
        end)
      end)

    {:reply, :ok,
     %{
       state
       | streams: Map.put(state.streams, stream, entry),
         pids: Map.put(state.pids, pid, pid_entry)
     }}
  end

  @impl true
  def handle_info(:poll, state) do
    # A DB blip must not burn through the supervisor's restart intensity at
    # polling speed (the same rule as Queue.Worker's claim loop): connection
    # errors back off 10x the polling interval and try again, so the
    # listener rides out an outage of any length and resumes when the DB
    # returns. Anything that isn't a connection error crashes (crash-only).
    case try_poll(state) do
      {:ok, state} ->
        schedule_poll(state.interval)
        {:noreply, state}

      {:error, err} ->
        Logger.warning("odoshi_beam.cable: poll failed (will retry): #{Exception.message(err)}")

        schedule_poll(state.interval * @db_backoff_multiplier)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    state =
      case state.pids[pid] do
        nil ->
          state

        %{subs: subs} ->
          Enum.reduce(Map.keys(subs), state, &drop_subscription(&2, pid, &1))
      end

    {:noreply, state}
  end

  defp drop_subscription(state, pid, identifier) do
    case state.pids[pid] do
      nil ->
        state

      %{ref: ref, subs: subs} = pid_entry ->
        {streams_for_id, remaining} = Map.pop(subs, identifier, [])

        streams =
          Enum.reduce(streams_for_id, state.streams, fn stream, acc ->
            case acc[stream] do
              nil ->
                acc

              entry ->
                entry = update_in(entry.subs, &Map.delete(&1, {pid, identifier}))

                # remove_channel when the last subscriber goes: the baseline
                # is dropped with it, so a later re-subscribe starts fresh.
                if map_size(entry.subs) == 0 do
                  Map.delete(acc, stream)
                else
                  Map.put(acc, stream, entry)
                end
            end
          end)

        pids =
          if map_size(remaining) == 0 do
            Process.demonitor(ref, [:flush])
            Map.delete(state.pids, pid)
          else
            Map.put(state.pids, pid, %{pid_entry | subs: remaining})
          end

        %{state | streams: streams, pids: pids}
    end
  end

  ## Polling

  defp try_poll(state) do
    {:ok, poll(state)}
  rescue
    err in [DBConnection.ConnectionError, Postgrex.Error] ->
      if db_unavailable_error?(err), do: {:error, err}, else: reraise(err, __STACKTRACE__)
  end

  # What counts as "the DB is unavailable" (back off) vs a real query error
  # (crash-only, let it raise):
  # * DBConnection.ConnectionError — checkout/queue failures while the pool
  #   has no live connections;
  # * Postgrex.Error carrying a connection-class SQLSTATE: class 08
  #   (connection exception), 57 (operator intervention: admin_shutdown /
  #   crash_shutdown / cannot_connect_now — what an in-flight query raises
  #   at the moment Postgres stops), 53 (insufficient resources, e.g.
  #   too_many_connections during a recovery stampede).
  defp db_unavailable_error?(%DBConnection.ConnectionError{}), do: true

  defp db_unavailable_error?(%Postgrex.Error{postgres: %{code: _} = pg}) do
    case pg[:pg_code] do
      <<class::binary-size(2), _::binary>> -> class in ["08", "53", "57"]
      _ -> false
    end
  end

  defp db_unavailable_error?(_err), do: false

  defp poll(%{streams: streams} = state) when map_size(streams) == 0, do: state

  # Cursor never initialized (DB was down at boot): initialize it now, at
  # the current MAX(id) — the boot-time-cursor semantics, just deferred to
  # the first poll that can reach the DB. Selection starts next tick;
  # per-stream baselines (all taken at or after this moment) already guard
  # every subscriber against replay.
  defp poll(%{last_id: nil} = state) do
    %{state | last_id: max_message_id(state.db)}
  end

  defp poll(state) do
    hashes = state.streams |> Map.values() |> Enum.map(& &1.hash) |> Enum.uniq()

    # Message.broadcastable(channels, last_id): hash match + id > cursor,
    # ordered by id.
    %{rows: rows} =
      Postgrex.query!(
        state.db,
        """
        SELECT id, channel, payload FROM solid_cable_messages
        WHERE channel_hash = ANY($1) AND id > $2
        ORDER BY id ASC
        """,
        [hashes, state.last_id]
      )

    Enum.reduce(rows, state, fn [id, channel, payload], acc ->
      acc = deliver(acc, id, channel, payload)
      # Ruby sets self.last_id = message.id for every broadcastable row,
      # delivered or not (hash-collision rows advance the cursor too).
      %{acc | last_id: id}
    end)
  end

  defp deliver(state, id, channel, payload) do
    case state.streams[channel] do
      # Hash matched but exact channel bytes didn't (collision), or the
      # subscriber went away between SELECT and now — not ours to deliver.
      nil ->
        state

      %{last_id: baseline} when baseline >= id ->
        # Pre-subscription history: channel_last_id >= message.id skip.
        state

      entry ->
        case Jason.decode(payload) do
          {:ok, message} ->
            Enum.each(Map.keys(entry.subs), fn {pid, identifier} ->
              send(pid, {:cable_broadcast, identifier, message})
            end)

            :telemetry.execute(
              [:odoshi_beam, :cable, :broadcast],
              %{subscribers: map_size(entry.subs)},
              %{channel: channel, message_id: id}
            )

          {:error, reason} ->
            # ActionCable's stream decoder would raise per delivery and drop
            # the message; same outcome, one log line.
            Logger.warning(
              "odoshi_beam.cable: undecodable solid_cable payload id=#{id}: #{inspect(reason)}"
            )
        end

        put_in(state.streams[channel].last_id, id)
    end
  end

  defp max_message_id(db) do
    %{rows: [[max_id]]} = Postgrex.query!(db, "SELECT MAX(id) FROM solid_cable_messages", [])
    max_id || 0
  end

  defp try_max_message_id(db) do
    {:ok, max_message_id(db)}
  rescue
    err in [DBConnection.ConnectionError, Postgrex.Error] ->
      if db_unavailable_error?(err), do: {:error, err}, else: reraise(err, __STACKTRACE__)
  end

  @doc """
  `SolidCable::Message.channel_hash_for`:
  `Digest::SHA256.digest(channel).unpack1("q>")` — first 8 bytes of the
  SHA-256 digest as a signed big-endian 64-bit integer. Public so the unit
  suite can pin it against a Ruby-produced vector.
  """
  def channel_hash(channel) do
    <<hash::signed-big-integer-size(64), _rest::binary>> = :crypto.hash(:sha256, channel)
    hash
  end

  defp schedule_poll(interval), do: Process.send_after(self(), :poll, interval)
end
