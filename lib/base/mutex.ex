defmodule Atomic.Base.FastMutex do
  @moduledoc """
  Implementation of Fast Mutex.
  That is, same-thread release constraint is unchecked.
  """

  alias __MODULE__

  @opaque t :: %FastMutex{cells: :atomics.atomics_ref, release_check: boolean}

  @enforce_keys [:cells]
  defstruct @enforce_keys ++ [release_check: true]

  @spec new() :: t
  def new(opts \\ []) do
    cells = :atomics.new(1, signed: false)
    :ok = :atomics.put(cells, 1, 0)
    struct(FastMutex, ([cells: cells] ++ opts))
  end

  @spec acquire(t) :: :ok
  def acquire(%FastMutex{cells: cells}), do: do_acquire(cells)
  defp do_acquire(cells) do
    case :atomics.compare_exchange(cells, 1, 0, 1) do
      :ok -> :ok
      _ -> do_acquire(cells) # TODO: without busy-locking
    end
  end

  @spec release(t) :: :ok | {:error, :already_released}
  def release(%FastMutex{cells: cells, release_check: check}) do
    case {:atomics.compare_exchange(cells, 1, 1, 0), check} do
      {0, true} -> {:error, :already_released}
      _ -> :ok
    end
  end

  def transaction(mutex, transaction) do
    acquire(mutex)
    transaction.()
    release(mutex)
  end
end
