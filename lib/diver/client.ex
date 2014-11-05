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
  Retrieves data from HBase.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#get(org.hbase.async.GetRequest)
  """
  def get(table, key, family, qualifier, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:get, table, key, family, qualifier}, timeout)
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
  Returns the capacity of the increment buffer.

  Note this returns the capacity of the buffer, not the number of items currently
  in it. There is currently no API to get the current number of items in it.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#getIncrementBufferSize()
  """
  def get_increment_buffer_size(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:get_increment_buffer_size}, timeout)
  end

  @doc """
  Eagerly prefetches and caches a table's region metadata from HBase.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#prefetchMeta(byte[])
  """
  def prefetch_meta(table, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:prefetch_meta, table}, timeout)
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

  @doc """
  Sets the maximum time (in milliseconds) for which edits can be buffered.

  This interval will be honored on a "best-effort" basis. Edits can be buffered
  for longer than that due to GC pauses, the resolution of the underlying timer,
  thread scheduling at the OS level (particularly if the OS is overloaded with
  concurrent requests for CPU time), any low-level buffering in the TCP/IP stack
  of the OS, etc.

  Setting a longer interval allows the code to batch requests more efficiently
  but puts you at risk of greater data loss if the JVM or machine was to fail.
  It also entails that some edits will not reach HBase until a longer period of
  time, which can be troublesome if you have other applications that need to read
  the "latest" changes.

  Setting this interval to 0 disables this feature.

  The change is guaranteed to take effect at most after a full interval has elapsed,
  _using the previous interval_ (which is returned).

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#setFlushInterval(short)
  """
  def set_flush_interval(interval, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:set_flush_interval, interval}, timeout)
  end

  @doc """
  Changes the size of the increment buffer.

  __Note:__ Because there is no way to resize the existing buffer, this method
  will flush the existing buffer and create a new one. This side effect might be
  unexpected but is unfortunately required.

  This determines the maximum number of counters this client will keep in-memory
  to allow increment coalescing through `buffer_atomic_increment/4`.

  The greater this number, the more memory will be used to buffer increments, and
  the more efficient increment coalescing can be if you have a high-throughput
  application with a large working set of counters.

  If your application has excessively large keys or qualifiers, you might consider
  using a lower number in order to reduce memory usage.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#setIncrementBufferSize(int)
  """
  def set_increment_buffer_size(size, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:set_increment_buffer_size, size}, timeout)
  end

  @doc false
  defp get_java_server() do
    java_node = "__diver__" <> Atom.to_string(Kernel.node())
    {:diver_java_server, String.to_atom(java_node)}
  end

end
