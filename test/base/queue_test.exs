defmodule AtomicTest.Base.Queue do
  alias Atomic.Base.Queue

  use ExUnit.Case
  doctest Queue

  def new(length \\ 5) do
    assert {:ok, queue} = Queue.new(capacity: length)
    queue
  end

  def randint(a \\ 0, b) do
    trunc(a + (:rand.uniform() * (b-a)))
  end

  test "Correct Order" do
    queue = new(5)

    assert [1, 2, 3, 4, 5, :locked] =
      Enum.map(1..6, fn _ -> Queue.pop(queue) end)
    assert [:ok, :ok, :ok, :ok, :ok, :locked] =
      Enum.map(1..6, &Queue.push(queue, &1))
  end

  test "Async pop and push" do
    length = randint(10, 30)
    queue = new(length)

    1..length
    |> Task.async_stream(fn _ ->
      item = Queue.pop(queue)
      assert item != :locked
      assert :ok = Queue.push(queue, item)
    end)
  end
end
