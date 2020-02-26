defmodule Atomic.Base.Queue do

  # Guarantees to fulfill:
  # 1. Pop Pointer should always be behind or the same as Push Pointer
  #
  # Store details:
  #
  # :atomics
  # Queue: N-wide array, where N - cap, or number of slots for queue
  # Meta: info about queue. [LastPopIndex, Get(Pop)Semaphore, PushSemaphore]
  # :persistent_term
  # {__MODULE__, queue_name} -> {queue_ref, meta_ref, queue_length}

  @opaque queue :: reference
  @opaque meta :: reference
  @opaque capacity :: integer
  @opaque t :: {queue, meta, capacity}

  @type opts :: [init: boolean | integer]

  defmacro lastpop, do: 1
  defmacro popsem, do: 2
  defmacro lastpush, do: 3
  defmacro pushsem, do: 4
  defmacro meta_len, do: 4

  @spec new(opts) :: {:ok, queue} | {:error, any}
  def new(opts) do
    capacity =
      case Keyword.fetch(opts, :capacity) do
        {:ok, capacity} when is_integer(capacity) ->
          capacity
        :error ->
          raise ArgumentError,
            message: "#{__MODULE__}: [capacity: integer] option is required"
      end

    init =
      case Keyword.get(opts, :init, true) do
        true ->
          capacity
        false ->
          0
        i when is_integer(i) and i >= 0 and i <= capacity ->
          i
        i when is_integer(i) ->
          raise ArgumentError,
            message: "#{__MODULE__}: 'init' option value is "<>
            "boolean or pos_integer <= cap"
      end

    queue =
      capacity
      |> :atomics.new(signed: true)
      |> init_queue(init)

    meta =
      meta_len()
      |> :atomics.new(signed: true)
      |> init_meta(capacity, init)

    {:ok, {queue, meta, capacity}}
  end

  defp init_queue(ref, init), do: do_init_queue(ref, init, 1)

  defp do_init_queue(ref, init_cnt, i) when i > init_cnt, do: ref
  defp do_init_queue(ref, init_cnt, i) do
    :ok = :atomics.put(ref, i, i)
    do_init_queue(ref, init_cnt, i+1)
  end

  defp init_meta(ref, cap, init) do
    :ok = :atomics.put(ref, lastpop(), -1)
    :ok = :atomics.put(ref, popsem(), init)
    :ok = :atomics.put(ref, lastpush(), mod(init-1, cap))
    :ok = :atomics.put(ref, pushsem(), cap - init)

    ref
  end

  @spec pop(t) :: integer | :locked
  def pop({queue, meta, cap} = q) do
    label = :erlang.unique_integer()
    __debug__("Before pop #{label}", q)

    answer =
      if :atomics.sub_get(meta, popsem(), 1) < 0 do
        :ok = :atomics.add(meta, popsem(), 1)
        :locked
      else
        raw_idx = :atomics.add_get(meta, lastpop(), 1)

        if raw_idx == cap, do: :atomics.sub(meta, lastpop(), cap) # try mod

        item = :atomics.exchange(queue, mod(raw_idx, cap)+1, -1)
        :ok = :atomics.add(meta, pushsem(), 1)
        item
      end

    __debug__("After pop (#{answer}) #{label}", q)
    answer
  end

  @spec push(t, item :: integer) :: :ok | :locked
  def push({queue, meta, cap} = q, item) do
    label = :erlang.unique_integer()
    __debug__("Before push #{item} #{label}", q)

    answer =
    if :atomics.sub_get(meta, pushsem(), 1) < 0 do
      :ok = :atomics.add(meta, pushsem(), 1)
      :locked
    else
      raw_idx = :atomics.add_get(meta, lastpush(), 1)

      if raw_idx == cap, do: :atomics.sub(meta, lastpush(), cap) # try mod

      :ok = :atomics.put(queue, mod(raw_idx, cap)+1, item)
      :ok = :atomics.add(meta, popsem(), 1)
      :ok
    end

    __debug__("After push #{item} (#{answer}) #{label}", q)
    answer
  end

  defp __debug__(header \\ "", queue)
  # defp __debug__(_, _), do: :ok
  defp __debug__(header, {q, m, l}) do
    require Logger
    items = Enum.map(1..l, &:atomics.get(q, &1))
    {lo, os, lu, uo} = Enum.map(1..4, &:atomics.get(m, &1)) |> List.to_tuple()
    """
    # #{header}
    Queue
    Capacity: #{l}
    Meta: LO=#{lo} OS=#{os} LU=#{lu} UO=#{uo}
    Items: #{inspect items}
    """
    |> Logger.debug()
  end

  defp mod(i, n) do
    case rem(i, n) do
      neg when neg < 0 -> n + neg
      nonneg -> nonneg
    end
  end
end
