defmodule Atomic.Queue do
  @moduledoc """
  Concurrent queue that can be used from multiple processes simultaneously.

  All queue methods (except new) from this module raise ArgumentError
  if their first argument is not queue.
  """
  alias Atomic.Base
  alias Atomic.Base.FastMutex
  alias __MODULE__

  @typep id_counter :: :atomics.atomics_ref
  @typep map_storage ::
    {:map, %{any => pos_integer}, %{pos_integer => any}}     # VU = false
    | {:pt, {__MODULE__, integer}, FastMutex.t, id_counter}  # VU = :rare
    | {:ets, map :: :ets.tid, remap :: :ets.tid, id_counter} # otherwise

  @opaque t ::
    %Queue{
      queue: Base.Queue.queue,
      map: map_storage, # mapping between values and atomic integers
      capacity: pos_integer
    }

  @enforce_keys [:queue, :map, :capacity]
  defstruct @enforce_keys

  @typedoc "TODO"
  @type item :: any

  # @typedoc "TODO"
  @type opt_capacity :: {:capacity, pos_integer
                                  | (non_neg_integer -> pos_integer)}
  # @typedoc "TODO"
  @type opt_init :: {:init, boolean | non_neg_integer}
  # @typedoc "TODO"
  @type opt_value_updates :: {:value_updates, false | :rare | :common | true}
  @type opts :: [opt_capacity
               | opt_init
               | opt_value_updates]

  @spec new(Enumerable.t, opts) :: {:ok, Queue.t}
  def new(args, opts \\ []) do
    {mapping, count} = make_arg_mapping(args, opts)
    {bq_opts, capacity} = make_base_opts(count, opts)

    {:ok, queue} = Base.Queue.new(bq_opts ++ opts)

    {:ok,
     %Queue{
       queue: queue,
       map: mapping,
       capacity: capacity
     }}
  end

  defp make_base_opts(count, opts) do
    capacity =
      case Keyword.get(opts, :capacity, count) do
        capfun when is_function(capfun, 1) -> capfun.(count)
        cap when is_integer(cap) and cap > 0 -> cap
        _ ->
          msg = "either args must not be empty or capacity must be set"
          raise ArgumentError, message: msg
      end

    init =
      case Keyword.get(opts, :init, count) do
        bool when is_boolean(bool) -> bool
        i when is_integer(i) and i >= 0 -> i
      end

    bq_opts = Keyword.merge(opts, [capacity: capacity, init: init])
    {bq_opts, capacity}
  end


  @doc """
  Pop next element from the queue.

  Returns {:ok, item} if queue is not empty, {:error, :empty} otherwise.
  Raises ArgumentError if argument is not a queue.
  """
  @spec pop(t) :: {:ok, item} | {:error, any}
  def pop(%Queue{queue: queue, map: map}) do
    do_pop(queue, map)
  end
  def pop(_), do: raise ArgumentError

  defp do_pop(bq, {:map, _map, remap}) do
    case Base.Queue.pop(bq) do
      :locked -> {:error, :empty}
      idx -> {:ok, Map.fetch!(remap, idx)}
    end
  end
  defp do_pop(bq, {:pt, key, _mutex, _counter} = mapping) do
    case Base.Queue.pop(bq) do
      :locked -> {:error, :empty}
      idx ->
        {_map, remap} = :persistent_term.get(key)
        case Map.fetch(remap, idx) do
          {:ok, _item} = ok -> ok
          :error -> do_pop(bq, mapping) # Possible value was erased
        end
    end
  end

  @doc """
  Push an element to the queue.

  Returns :ok if successfull,
  {:error, :full} if queue was full,
  {:error, :unknown_value} if value is unknown
  and value_updates was set to false either by default or manually.
  Raises ArgumentError if argument is not a queue.
  """
  @spec push(t, item) :: :ok | {:error, any}
  def push(%Queue{queue: queue, map: map}, item) do
    do_push(queue, map, item)
  end
  def push(_, _value), do: raise ArgumentError

  defp do_push(bq, {:map, map, _remap}, item) do
    with {:ok, idx} <- Map.fetch(map, item),
         :ok <- Base.Queue.push(bq, idx)
      do
        :ok
      else
        :error -> {:error, :unknown_value}
        :locked -> {:error, :full}
    end
  end
  defp do_push(bq, {:pt, key, mutex, id_counter}, item) do
    {map, _remap} = :persistent_term.get(key)
    idx =
      case Map.fetch(map, item) do
        {:ok, idx} -> idx
        :error -> # Unknown value: add mappings
          item_id = next_id(id_counter)

          :ok = FastMutex.acquire(mutex)
          {map, remap} = :persistent_term.get(key) # Get the last version
          pt_newval = {Map.put(map, item, item_id), Map.put(remap, item_id, item)}
          :persistent_term.put(key, pt_newval)
          FastMutex.release(mutex)

          item_id
      end

    # Maybe TODO: mapping clean if failed?
    case Base.Queue.push(bq, idx) do
      :ok -> :ok
      :locked -> {:error, :full}
    end
  end

  defp do_push(bq, {:ets, table, retable, id_counter}) do
    # TODO
  end


  @doc """
  Delete items from a queue.

  Returns :ok on success,
  {:error, :not_supported} for value_updates: false.
  Raises ArgumentError also if items is not list.
  """
  def drop(%Queue{queue: bq, map: map}, items) when is_list(items) do
    do_drop(bq, map, items)
  end
  def drop(_, _), do: raise ArgumentError

  def do_drop(_bq, {:map, _map, _remap}, _i), do: {:error, :not_supported}
  def do_drop(bq, {:pt, key, mutex, _cnter}, items) do
    FastMutex.acquire(mutex)
    {map, remap} = :persistent_term.get(key)
    {items_map, new_map} = Map.split(map, items)
    new_remap = Map.drop(remap, Map.values(items_map))
    :ok = :persistent_term.put(key, {new_map, new_remap})
    FastMutex.release(mutex)
    :ok
  end

  @doc """
  Delete a queue.

  Must be called for queues with value_updates set to :rare or true
  in order to deallocate resources.
  """
  @spec delete(t) :: :ok | {:error, any}
  def delete(%Queue{map: map}) do
    delete_arg_mapping(map)
  end


  defp make_arg_mapping(args, opts) do
    if Enumerable.impl_for(args) == nil do
      raise ArgumentError, message: "args are not enumerable"
    end

    case Keyword.get(opts, :value_updates, false) do
      false -> map_arg_mapping(args)
      :rare -> pt_arg_mapping(args)
      :uncommon -> ets_arg_mapping(args)
      true -> ets_arg_mapping(args, write_concurrency: true)
    end
  end

  defp map_arg_mapping(args) do
    {ml, rml, count} = enum_to_maplists(args)
    {{:map, Map.new(ml), Map.new(rml)}, count}
  end

  defp pt_arg_mapping(args) do
    {{:map, ml, rml}, count} = map_arg_mapping(args)
    key = {__MODULE__, :erlang.unique_integer()}
    :ok = :persistent_term.put(key, {ml, rml})
    {{:pt, key, FastMutex.new(), make_id_counter(count)}, count}
  end

  defp ets_arg_mapping(args, extra_ets_opts \\ []) do
    ets_opts = extra_ets_opts ++ [:ordered_set, :public, read_concurrency: true]
    tid = :ets.new(__MODULE__, ets_opts)
    retid = :ets.new(__MODULE__, ets_opts)

    {maplist, remaplist, count} = enum_to_maplists(args)
    true = :ets.insert(tid, maplist)
    true = :ets.insert(retid, remaplist)
    counter = make_id_counter(count)
    {:ets, tid, retid, counter}
  end

  defp enum_to_maplists(args) do
    Enum.reduce(args, {[], [], 0},
      fn item, {ml, rml, count} ->
          i = count + 1
        {
          [{item, i} | ml],
          [{i, item} | rml],
          i
        }
      end)
  end

  defp delete_arg_mapping({:map, _, _}), do: :ok
  defp delete_arg_mapping({:pt, key, _, _}) do
    :persistent_term.erase(key)
    :ok
  end
  defp delete_arg_mapping({:ets, tid, retid, _}) do
    true = :ets.delete(tid)
    true = :ets.delete(retid)
    :ok
  end

  defp make_id_counter(count) do
    counter = :atomics.new(1, signed: true)
    :ok = :atomics.put(counter, 1, count)
    counter
  end

  defp next_id(counter) do
    :atomics.add_get(counter, 1, 1)
  end

  def to_string(%Queue{capacity: capacity}) do
    "<#{inspect __MODULE__}(#{capacity})>"
  end
end

defimpl Inspect, for: Atomic.Queue do
  import Inspect.Algebra

  def inspect(%Atomic.Queue{capacity: capacity}, opts) do
    concat(["#Atomic.Queue<", to_doc(capacity, opts), ">"])
  end
end
