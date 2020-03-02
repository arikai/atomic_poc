defmodule AtomicTest.Queue do
  alias Atomic.Queue

    use ExUnit.Case
    doctest Queue

    @words ["The", "quick", "brown", "fox", "jumps"]

    test "Mapping test" do
      values = @words
      assert {:ok, q} = Queue.new(values)
      assert ^values = Enum.map(values, fn _ -> {:ok, v} = Queue.pop(q); v end)
    end

    test "Capacity test" do
      values = 1..5
      assert {:ok, q} = Queue.new(values, capacity: &(&1 + 3))
      Enum.each(1..3, &(assert :ok = Queue.push(q, &1)))
      expected = Stream.concat([1..5, 1..3]) |> Enum.to_list()
      assert ^expected = Enum.map(1..8, fn _ -> {:ok, v} = Queue.pop(q); v end)
    end

    test "Try push unknown (to mapping) value" do
      values = @words
      first = hd(values)
      assert {:ok, q} = Queue.new(values)
      assert {:ok, ^first} = Queue.pop(q)
      assert {:error, :unknown_value} = Queue.push(q, "over")
    end

    test "Check value_updates: :rare" do
      values = @words
      assert {:ok, q} = Queue.new(values, value_updates: :rare)
      Enum.each(@words, fn i -> assert {:ok, ^i} = Queue.pop(q) end)
      assert :ok = Queue.push(q, :all_new_value)
      assert {:ok, :all_new_value} = Queue.pop(q)
    end
end
