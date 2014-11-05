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
  Deletes data from HBase.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#delete(org.hbase.async.DeleteRequest)
  """
  def delete(table, key, family, qualifiers, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:delete, table, key, family, qualifiers}, timeout)
  end

  @doc """
  Ensures that a given table really exists.

  It's recommended to call this method in the startup code of your application
  if you know ahead of time which tables / families you're going to need, because
  it'll allow you to "fail fast" if they're missing.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#ensureTableExists(byte[])
  """
  def ensure_table_exists(table, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:ensure_table_exists, table}, timeout)
  end

  @doc """
  Ensures that a given table/family pair really exists.

  It's recommended to call this method in the startup code of your application
  if you know ahead of time which tables / families you're going to need, because
  it'll allow you to "fail fast" if they're missing.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#ensureTableFamilyExists(byte[],%20byte[])
  """
  def ensure_table_family_exists(table, family, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:ensure_table_family_exists, table, family}, timeout)
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
  Returns the maximum time (in milliseconds) for which edits can be buffered.

  The default value is unspecified and implementation dependant, but is guaranteed
  to be non-zero. A return value of 0 indicates that edits are sent directly to
  HBase without being buffered.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#getFlushInterval()
  """
  def get_flush_interval(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:get_flush_interval}, timeout)
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
