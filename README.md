Diver
=====

A HBase driver for Erlang/Elixir using [Jinterface](http://www.erlang.org/doc/apps/jinterface/jinterface_users_guide.html) and the [Asynchbase Java client](https://github.com/OpenTSDB/asynchbase) to query the database.

Diver creates a Java server as a [hidden Erlang node](http://www.erlang.org/doc/reference_manual/distributed.html#id85406) at startup and dispatches `GenServer` requests directly to the `HBaseClient` running on the Java server. These requests are executed asynchronously by the client on the HBase cluster and responses are returned directly to the calling process. The Java server is monitored by the `Diver.Supervisor` and is restarted as necessary.

This work is inspired by the [`lucene_server`](https://github.com/inaka/lucene_server) and [`yokozuna`](https://github.com/basho/yokozuna) projects.

Diver was created and is maintained by Chris Molozian (@novabyte) and contributors.
<br/>
Code licensed under the [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0).
 Documentation licensed under [CC BY 3.0](http://creativecommons.org/licenses/by/3.0/).

## Download ##

Diver is available on [Hex.pm](https://hex.pm/packages/diver).

Adding Diver to your application takes two steps:

1. Add `diver` to your `mix.exs` dependencies:

    ```elixir
    def deps do
      [{:diver, "~> 0.1"}]
    end
    ```

2. Add `:diver` to your application dependencies:

    ```elixir
    def application do
      [applications: [:diver]]
    end
    ```

Diver requires the Java Runtime Environment (JRE) and the `java` executable accessible via [`os:find_executable/1`](http://www.erlang.org/doc/man/os.html#find_executable-1) to start. A standalone Jar package of the Java server (and its dependencies) is bundled with this codebase.

You will only need the Java Development Kit (JDK) if you want to build the Java code directly from source. See [Developer Notes](#developer-notes) for more information.

## Usage ##

Diver uses a registered name to send messages to the Java node which executes requests to HBase. You must set the Erlang [node name](http://www.erlang.org/doc/reference_manual/distributed.html#id85266) before you start the application.

### Configuration ###

The driver can be configured in the usual way by adding these settings to your `config.exs` file:

```elixir
config :diver,
  zk: [quorum_spec: "localhost",
       base_path: "/hbase"],
  jvm_args: ["-Djava.awt.headless=true", "-Xms256m", "-Xmx1024m", "-XX:MaxPermSize=128m"]
```

The configuration above is the application's default settings.

### Example ###

In these examples you'll need to have HBase setup and running and Diver configured. The default settings will work with a local HBase instance started with the `bin/start-hbase.sh` script in the HBase distribution. For more information on setup and configuration of HBase see [here](http://hbase.apache.org/book/quickstart.html).

You should start your application as a named node, you can do this with `iex`:

```elixir
$ iex --name "myserver@127.0.0.1" --cookie "mycookie" -S mix
iex(myserver@127.0.0.1)1> Application.ensure_all_started(:diver)
{:ok, []}
iex(myserver@127.0.0.1)2> Diver.Client.ensure_table_exists("tbl")
{:ok}
iex(myserver@127.0.0.1)3> Diver.Client.get("tbl", "key", "family", "qualifier")
{:ok, [...]}
```

For more detailed examples on using Diver check out the [documentation](http://hexdocs.pm/diver/).

To learn more about HBase's data model, and how to build applications with HBase, check out this [book](http://hbase.apache.org/book/datamodel.html).

__Note:__ This client does not yet have feature parity with the features in the Asynchbase client. It is under development, any feedback and bug reports are welcome.

## Developer Notes ##

The codebase requires [Gradle](http://gradle.org/) and JDK 7 or greater, as well as the Elixir toolchain. The dependencies for the Java codebase will be downloaded from [Maven Central](http://search.maven.org/) and those for the Erlang/Elixir codebase from [Hex](https://hex.pm/).

To build the codebase:

```bash
$ gradle build && cp _java_build/libs/diver-0.3.0-dev.jar priv/
$ mix do deps.get, deps.compile, compile
```

### Why Java? ###

Building a client driver for HBase is a complex endeavour, the official [HTable](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/HTable.html) client and the Asynchbase client are "smart clients". They cache knowledge about the operational state of the HBase cluster by communicating directly with the Zookeeper ensemble.

Building a pure Erlang/Elixir "smart client" is __a lot__ of work, it would need to replicate all the coordination logic with Zookeeper, handle the messaging protocol to retrieve data via the RegionServers, copy any other "special" (undocumented) client logic used by the official client, include work on performance tuning with benchmarks and would need a very large test suite to keep up with changes in the HBase project.

An alternative is to run a [Thrift server](http://hbase.apache.org/book/thrift.html) as a gateway to the cluster. There is an Erlang client for HBase that communicates over the Thrift gateway [here](https://github.com/zhentao/erlang-hbase-thrift2). This approach is great but introduces its own complexity including additional server components to run and manage.

There's also [Stargate](http://wiki.apache.org/hadoop/Hbase/Stargate), a REST gateway for HBase. This approach has similar problems to running a Thrift gateway server.

With this project I've chosen to build on the performance, maturity and stability of an existing HBase client, and focus on bridging the two languages (as transparently as possible) to provide a high performance, simple Erlang/Elixir driver API for HBase.

### Special Thanks ###

> If I have seen further it is by standing on the shoulders of giants. - _Isaac Newton_

A huge thank you to the [OpenTSDB](https://github.com/OpenTSDB) team for their fantastic work on the Asynchbase HBase client.

### Contribute ###

All contributions to the documentation and the codebase are very welcome.
