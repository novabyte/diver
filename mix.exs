defmodule Diver.Mixfile do
  use Mix.Project

  @version File.read!("VERSION") |> String.strip()

  def project do
    [app: :diver,
     name: "Diver",
     version: @version,
     source_url: "https://github.com/novabyte/diver",
     homepage_url: "http://hexdocs.pm/diver/",
     elixir: "~> 1.0",
     deps: deps,
     package: [
       files: ~w(lib priv mix.exs README.md LICENSE VERSION),
       contributors: ["Chris Molozian"],
       licenses: ["Apache 2.0"],
       links: %{github: "https://github.com/novabyte/diver",
                docs: "http://hexdocs.pm/diver/"}],
     description: """
     A HBase driver for Erlang/Elixir using Jinterface and the Asynchbase Java client
     to query the database.
     """]
  end

  def application do
    [applications: [:logger],
     env: [zk: [quorum_spec: "localhost", base_path: "/hbase"],
           jvm_args: ["-Djava.awt.headless=true", "-Xms256m", "-Xmx1024m", "-XX:MaxPermSize=128m"]],
     mod: {Diver, []}]
  end

  defp deps do
    [{:earmark, "~> 1.0", only: :dev},
     {:ex_doc, "~> 0.13.0", only: :dev}]
  end
end
