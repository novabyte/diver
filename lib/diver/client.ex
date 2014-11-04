defmodule Diver.Client do

  def client_stats(timeout \\ 5000) do
    java_node = "__diver__" <> Atom.to_string(Kernel.node())
    proc = {:diver_java_server, String.to_atom(java_node)}
    GenServer.call(proc, {:client_stats}, timeout)
  end

  def put(table, key, family, qualifiers, values, timeout \\ 5000) do
    java_node = "__diver__" <> Atom.to_string(Kernel.node())
    proc = {:diver_java_server, String.to_atom(java_node)}
    GenServer.call(proc, {:put, table, key, family, qualifiers, values}, timeout)
  end

end
