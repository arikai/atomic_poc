defmodule Atomic.Queue do
  alias Atomic.Base
  alias __MODULE__

  @opaque t ::
    %Queue{
      queue: Base.Queue.queue,
      map: %{any => integer},
      remap: %{integer => any},
      count: integer,

      # Maybe TODO:
      # make separate Atomic.OpenQueue with possibility to add new values
      # allow_new: boolean,
      # name: term
    }

  @enforce_keys [:count, :queue, :map, :remap]
  defstruct @enforce_keys ++ [allow_new: false]

  def new(args, opts \\ []) do
    {map, remap, count} = make_args(args) |> IO.inspect(label: "a")

    opts =
      case count do
        0 ->
          opts
        _ ->
          Keyword.put_new(opts, :length, count)
      end

    {:ok, queue} = Base.Queue.new(opts)
    {:ok,
     %Queue{
      queue: queue,
      map: map,
      remap: remap,
      count: count
    }}
  end

  defp make_args(args) do
    if Enumerable.impl_for(args) == nil do
      raise ArgumentError, message: "args are not enumerable"
    end

    {ml, rml, count} =
      Enum.reduce(args, {[], [], 0},
        fn elem, {ml, rml, count} ->
          i = count + 1
          {
            [{elem, i} | ml],
            [{i, elem} | rml],
            i
          }
        end)
    {Map.new(ml), Map.new(rml), count}
  end

  def pop(%Queue{queue: queue, remap: remap}) do
    case Base.Queue.pop(queue) do
      :locked -> {:error, :empty}
      idx -> {:ok, Map.fetch!(remap, idx)}
    end
  end

  def push(%Queue{queue: queue, map: map}, value) do
    with {:ok, idx} <- Map.fetch(map, value),
         :ok <- Base.Queue.push(queue, idx)
      do
        :ok
      else
        :error -> {:error, :unknown_value}
        {:error, _} = e -> e
    end
  end

  def to_string(%Queue{count: count}) do
    "<#{inspect __MODULE__}(#{count})>"
  end
end

defimpl Inspect, for: Atomic.Queue do
  import Inspect.Algebra

  def inspect(%Atomic.Queue{count: count}, opts) do
    concat(["#Atomic.Queue<", to_doc(count, opts), ">"])
  end
end
