defmodule OdoshiBeam.Root do
  @moduledoc """
  The root of one supervision tree instance:

      Root (Supervisor, max_restarts: 0)
      ├── SocketServer        (§5 NDJSON heartbeats + control)
      └── ChildrenSupervisor  (native OTP strategy/intensity over Child ports)

  The socket server starts first so the per-boot token and socket path exist
  before any child spawns; children receive ODOSHI_SOCK / ODOSHI_TOKEN
  in their environment (DESIGN §9).
  """

  use Supervisor

  alias OdoshiBeam.{ChildrenSupervisor, ChildSpec, SocketServer, Telemetry}

  @strategies [:one_for_one, :rest_for_one]

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    socket_path = Keyword.fetch!(opts, :socket_path)
    specs = opts |> Keyword.fetch!(:children) |> Enum.map(&ChildSpec.new/1)
    strategy = Keyword.get(opts, :strategy, :one_for_one)
    max_restarts = Keyword.get(opts, :max_restarts, 3)
    max_seconds = Keyword.get(opts, :max_seconds, 5)

    if strategy not in @strategies do
      raise ArgumentError, "strategy must be one of #{inspect(@strategies)}"
    end

    ids = Enum.map(specs, & &1.id)

    if length(Enum.uniq(ids)) != length(ids) do
      raise ArgumentError, "duplicate child ids: #{inspect(ids)}"
    end

    # Owned by the root supervisor process, lives exactly as long as the tree.
    # Holds heartbeats ({:hb, id}), spawn counters ({:count, id}) and
    # control-restart markers ({:manual, id}).
    table = :ets.new(:odoshi_beam, [:set, :public])
    token = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

    ctx = %{
      root: self(),
      table: table,
      socket_path: socket_path,
      token: token,
      strategy: strategy,
      ids: ids
    }

    Telemetry.emit([:odoshi, :supervisor, :start], %{}, %{strategy: strategy, children: ids})

    children = [
      {SocketServer, ctx},
      {ChildrenSupervisor,
       %{
         ctx: ctx,
         specs: specs,
         strategy: strategy,
         max_restarts: max_restarts,
         max_seconds: max_seconds
       }}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 0)
  end

  @doc """
  Replace one child: graceful drain (TERM → wait → KILL) then a fresh spawn.
  The transport for `{"cmd":"restart"}` control messages. Mirrors the Ruby
  `restart!`: emits drain (+ kill on timeout) and spawn, but no child.restart
  — deliberate replacement is not a crash and never counts toward intensity.
  """
  def restart_child(ctx, id) do
    case children_sup(ctx.root) do
      nil ->
        :ok

      sup ->
        :ets.insert(ctx.table, {{:manual, id}, true})
        _ = Supervisor.terminate_child(sup, id)
        _ = Supervisor.restart_child(sup, id)
        :ok
    end
  end

  @doc "The pid of the native supervisor over the external children."
  def children_sup(root) do
    root
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {ChildrenSupervisor, pid, _type, _mods} when is_pid(pid) -> pid
      _other -> nil
    end)
  end

  @doc "The pid of the socket server child."
  def socket_server(root) do
    root
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {SocketServer, pid, _type, _mods} when is_pid(pid) -> pid
      _other -> nil
    end)
  end
end
