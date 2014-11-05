Diver
=====

A HBase driver for Erlang/Elixir using [jinterface](http://www.erlang.org/doc/apps/jinterface/jinterface_users_guide.html) and the [Asynchbase Java client](https://github.com/OpenTSDB/asynchbase) to query the database.

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

## Developer Notes ##

// TODO

### Why Java? ###

// TODO

### Special Thanks ###

> If I have seen further it is by standing on the shoulders of giants.
> _Isaac Newton_

A huge thank you to the [OpenTSDB](https://github.com/OpenTSDB) team for their fantastic work on the Asynchbase HBase client.

### Contribute ###

All contributions to the documentation and the codebase are very welcome.
