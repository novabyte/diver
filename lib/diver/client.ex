defmodule Diver.Client do

  @doc """
  Returns a snapshot of usage statistics for this client.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#stats()
  """
  def client_stats(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:client_stats}, timeout)
  end

  @doc """
  Flushes to HBase any buffered client-side write operation.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#flush()
  """
  def flush(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:flush}, timeout)
  end

  @doc """
  Stores data in HBase.

  __Note:__ This operation provides no guarantee as to the order in which subsequent
  put requests are going to be applied to the backend. If you need ordering, you
  must enforce it manually yourself.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#put(org.hbase.async.PutRequest)
  """
  def put(table, key, family, qualifiers, values, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:put, table, key, family, qualifiers, values}, timeout)
  end

  @doc false
  defp get_java_server() do
    java_node = "__diver__" <> Atom.to_string(Kernel.node())
    {:diver_java_server, String.to_atom(java_node)}
  end

end
