defmodule PingServer do
  use GenServer

  def ping(p) do
    GenServer.call(p, :ping)
  end

  def start_link(i) do
    GenServer.start_link(__MODULE__, i)
  end

  def init(idx) do
    {:ok, idx}
  end

  def handle_call(:ping, _, idx) do
    {:reply, {:pong, idx}, idx}
  end
end

defmodule PingManager do
  def run(iters) do
    1..iters
    |> Task.async_stream(fn _ -> ping_any() end)
    |> Stream.run()
  end

  def setup(count) do
    GenServer.start_link(__MODULE__, count, name: __MODULE__)
  end

  defp ping_any() do
    pid = get_pid()
    msg = {:pong, _} = PingServer.ping(pid)
    :ok = return_pid(pid)
    msg
  end

  use GenServer

  defp get_pid() do
    case GenServer.call(__MODULE__, :get_pid, :infinity) do
      {:ok, pid} -> pid
      _ -> get_pid()
    end
  end

  defp return_pid(pid) do
    GenServer.call(__MODULE__, {:return_pid, pid})
  end

  def init(count) do
    pids =
      1..count
      |> Enum.map(fn idx -> {:ok, pid} = PingServer.start_link(idx); pid end)
    {:ok, pids}
  end

  def handle_call(:get_pid, _, [pid | pids]) do
    {:reply, {:ok, pid}, pids}
  end
  def handle_call(:get_pid, _, []) do
    {:reply, {:error, :locked}, []}
  end
  def handle_call({:return_pid, pid}, _, pids) do
    {:reply, :ok, [pid | pids]}
  end
end


defmodule ProcesslessPingManager do
  def run(iters) do
    1..iters
    |> Task.async_stream(fn _ -> ping_any() end)
    |> Stream.run()
  end

  def setup(count) do
    {:ok, queue} =
      1..count
      |> Enum.map(fn idx -> {:ok, pid} = PingServer.start_link(idx); pid end)
      |> Atomic.Queue.new()

    :persistent_term.put(__MODULE__, queue)
  end

  defp ping_any() do
    q = :persistent_term.get(__MODULE__)

    pid = get_pid(q)
    msg = {:pong, _} = PingServer.ping(pid)
    :ok = return_pid(q, pid)

    msg
  end

  defp get_pid(q) do
    case Atomic.Queue.pop(q) do
      {:ok, item} -> item
      _ -> get_pid(q)
    end
  end

  defp return_pid(q, pid) do
    Atomic.Queue.push(q, pid)
  end
end

defmodule PingServerBenchmark do
  def run do
    count = 5
    PingManager.setup(count)
    ProcesslessPingManager.setup(count)

    Benchee.run(
      %{
        "process" => fn iters -> PingManager.run(iters) end,
        "process-less" => fn iters -> ProcesslessPingManager.run(iters) end
      },
      inputs: %{
        # "Small" => 100,
        "Medium" => 10_000,
        # "Big" => 1_000_000
      }
    )
  end
end

PingServerBenchmark.run()


# Operating System: macOS
# CPU Information: Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz
# Number of Available Cores: 8
# Available memory: 16 GB
# Elixir 1.9.4
# Erlang 22.1.8

# Benchmark suite executing with the following configuration:
# warmup: 2 s
# time: 5 s
# memory time: 0 ns
# parallel: 1
# inputs: Medium
# Estimated total run time: 14 s

# Benchmarking process with input Medium...
# Benchmarking process-less with input Medium...

# ##### With input Medium #####
# Name                   ips        average  deviation         median         99th %
# process-less          7.13      140.17 ms     ±4.59%      138.75 ms      163.88 ms
# process               5.94      168.35 ms     ±3.97%      165.93 ms      191.59 ms

# Comparison:
# process-less          7.13
# process               5.94 - 1.20x slower +28.18 ms
