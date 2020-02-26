defmodule Atomic.Base.Queue do

  # Guarantees to fulfill:
  # 1. Pop Pointer should always be behind or the same as Push Pointer
  #
  # Store details:
  #
  # :atomics
  # Queue: N-wide array, where N - length, or number of slots for queue
  # Meta: info about queue. [LastPopIndex, Get(Pop)Semaphore, PushSemaphore]
  # :persistent_term
  # {__MODULE__, queue_name} -> {queue_ref, meta_ref, queue_length}

  @opaque queue :: reference
  @opaque meta :: reference
  @opaque length :: integer
  @opaque t :: {queue, meta, length}

  @type opts :: [init: boolean | integer]

  defmacro lastpop, do: 1
  defmacro getsem, do: 2
  defmacro pushsem, do: 3

  @spec new(opts) :: {:ok, queue} | {:error, any}
  def new(opts) do
    length =
      case Keyword.fetch(opts, :length) do
        {:ok, length} when is_integer(length) -> length
        :error -> raise "#{__MODULE__}: [length: integer] option is required"
      end

    queue =
      length
      |> :atomics.new(signed: true)
      |> init_queue(length, opts)

    meta =
      :atomics.new(3, signed: true)
      |> init_meta(length, opts)

    {:ok, {queue, meta, length}}
  end

  defp init_queue(ref, length, opts) do
    case Keyword.get(opts, :init, true) do
      i when is_integer(i) ->
        do_init_queue(ref, min(length, i), 1)
      true ->
        do_init_queue(ref, length, 1)
      false ->
        ref
    end
  end
  defp do_init_queue(ref, limit, i) when i > limit, do: ref
  defp do_init_queue(ref, limit, i) do
    :atomics.put(ref, i, i)
    do_init_queue(ref, limit, i+1)
  end

  defp init_meta(ref, length, opts) do
    :ok = :atomics.put(ref, lastpop(), -1)

    case Keyword.get(opts, :init, true) do
      true ->
        :ok = :atomics.put(ref, getsem(), length)
        :ok = :atomics.put(ref, pushsem(), 0)
      false ->
        :ok = :atomics.put(ref, getsem(), 0)
        :ok = :atomics.put(ref, pushsem(), length)
    end

    ref
  end

  @spec pop(t) :: integer | :locked
  def pop({queue, meta, len} = q) do
    label = :erlang.unique_integer()
    __debug__("Before pop #{label}", q)

    answer =
      if :atomics.sub_get(meta, getsem(), 1) < 0 do
        :ok = :atomics.add(meta, getsem(), 1)
        :locked
      else
        # Conflict:
        # If lastpop is incremented (1)
        # but then used in push to calculate idx for push
        # before PS gets fixed (2)
        # the idx for push will be off by one

        # 1
        raw_idx = :atomics.add_get(meta, lastpop(), 1)

        if raw_idx == len, do: :atomics.sub(meta, lastpop(), len)
        idx = mod(raw_idx, len)

        item = :atomics.exchange(queue, idx+1, -1)

        # 2
        :ok = :atomics.add(meta, pushsem(), 1)
        item
      end

    __debug__("After pop (#{answer}) #{label}", q)
    answer
  end

  @spec push(t, item :: integer) :: :ok | :locked
  def push({queue, meta, len} = q, item) do
    label = :erlang.unique_integer()
    __debug__("Before push #{item} #{label}", q)

    # 3
    idx_base = :atomics.get(meta, lastpop())
    # 4
    dec = :atomics.sub_get(meta, pushsem(), 1)
    answer =
      if dec < 0 do
        :ok = :atomics.add(meta, pushsem(), 1)
        :locked
      else
        idx = mod(idx_base - dec, len)
        :ok = :atomics.put(queue, idx+1, item)
        :ok = :atomics.add(meta, getsem(), 1)
      end

    __debug__("After push #{item} (#{answer}) #{label}", q)
    answer
  end

  defp __debug__(header \\ "", queue)
  # defp __debug__(_, _), do: :ok
  defp __debug__(header, {q, m, l}) do
    require Logger
    items = Enum.map(1..l, &:atomics.get(q, &1))
    {fp, gs, ps} = Enum.map(1..3, &:atomics.get(m, &1)) |> List.to_tuple()
    """
    # #{header}
    Queue
    Length: #{l}
    Meta: FP=#{fp} GS=#{gs} PS=#{ps}
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
