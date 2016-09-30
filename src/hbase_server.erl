-module(hbase_server).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([server/0]).
-export([ensure_table_exists/1, ensure_table_family_exists/2]).
-export([get_config/1, set_config/2]).
-export([flush/0, prefetch_meta/1]).

-export([get/2, get/3, get/4, scan/3, scan_sync/2]).
-export([put/5, compare_and_set/6, increment/4]).
-export([delete/2, delete/3, delete/4]).

-define(CONNECT_TIMEOUT, (10 * 1000)).

-type table() :: binary().
-type cf() :: binary().
-type rowkey() :: binary().
-type qualifier() :: binary().
-type value() :: binary().
-type ts() :: integer().

-type hbase_tuples() :: [hbase_tuple()].
-type hbase_tuple() :: {table(), cf(), rowkey(), qualifier(), value(), ts()}.

-type config_key() :: flush_interval | increment_buffer_size.
-type scan_opts() :: [scan_opt()].
-type scan_opt() :: {num_rows, integer()}
    | {family, binary()}
    | {key_regexp, binary()}
    | {max_num_bytes, integer()}
    | {max_num_keyvalues, integer()}
    | {max_num_rows, integer()}
    | {max_timestamp, integer()}
    | {max_versions, integer()}
    | {qualifier, integer()}
    | {server_block_cache, integer()}
    | {start_key, binary()}
    | {stop_key, binary()}
    | {time_range, integer(), integer()}
    | {filter, filter_opts()}.

-type filter_opts() :: [filter_opt()].
-type filter_opt() :: {column_prefix, binary()}
    | {column_range, binary(), binary()}
    | {first_key_only}
    | {fuzzy_row, [{binary(), binary()}]}
    | {key_only}
    | {key_regexp, binary()}.

-type error() :: {error, binary(), binary()} | {error, atom()}.

-spec server() -> {atom(), atom()}.
server() ->
    {ok, NodeName} = gen_server:call(?MODULE, nodename),
    NodeName.

-spec ensure_table_exists(table()) -> ok | error().
ensure_table_exists(Table) ->
    gen_server:call(server(), {ensure_table_exists, Table}).

-spec ensure_table_family_exists(table(), cf()) -> ok | error().
ensure_table_family_exists(Table, CF) ->
    gen_server:call(server(), {ensure_table_family_exists, Table, CF}).

-spec get_config(config_key()) -> {ok, integer()} | error().
get_config(Option) ->
    gen_server:call(server(), {get_conf, Option}).

-spec set_config(config_key(), integer()) -> {ok, integer()} | error().
set_config(Option, Value) ->
    gen_server:call(server(), {set_conf, Option, Value}).

-spec flush() -> ok | error().
flush() ->
    gen_server:call(server(), {flush}).

-spec prefetch_meta(table()) -> ok | error().
prefetch_meta(Table) ->
    gen_server:call(server(), {prefetch_meta, Table}).

-spec get(table(), rowkey()) -> {ok, hbase_tuples()}.
get(Table, Key) ->
    gen_server:call(server(), {get, Table, Key}).

-spec get(table(), rowkey(), cf()) -> {ok, hbase_tuples()}.
get(Table, Key, CF) ->
    gen_server:call(server(), {get, Table, Key, CF}).

-spec get(table(), rowkey(), cf(), qualifier()) -> {ok, hbase_tuples()}.
get(Table, Key, CF, Qualifier) ->
    gen_server:call(server(), {get, Table, Key, CF, Qualifier}).

-spec scan(binary(), scan_opts(), reference()) -> ok.
scan(Table, Opts, Ref) ->
    gen_server:call(server(), {scan, Table, Opts, Ref}).

-spec scan_sync(binary(), scan_opts()) -> {ok, [hbase_tuples()]} | error().
scan_sync(Table, Opts) ->
    Ref = make_ref(),
    ok = scan(Table, Opts, Ref),
    receive_scan(Ref).

receive_scan(Ref) ->
    receive_scan(Ref, []).

receive_scan(Ref, Acc) ->
    receive
        {Ref, row, Row} ->
            receive_scan(Ref, [Row | Acc]);
        {Ref, done} ->
            {ok, lists:reverse(Acc)};
        {Ref, error, _, _, _} ->
            {error, internal}
    after 5000 -> {error, timeout}
    end.

-spec put(table(), rowkey(), cf(), [qualifier()], [value()]) -> {ok, list()}.
put(Table, Key, CF, Qualifiers, Values) ->
    gen_server:call(server(), {put, {Table, Key, CF, Qualifiers, Values}}).

-spec compare_and_set(table(), rowkey(), cf(), qualifier(), value(), value()) -> {ok, true | false}.
compare_and_set(Table, Key, CF, Qualifier, Value, Expected) ->
    gen_server:call(server(), {compare_and_set, {Table, Key, CF, [Qualifier], [Value]}, Expected}).

-spec increment(table(), rowkey(), cf(), qualifier()) -> {ok, number()}.
increment(Table, Key, CF, Qualifier) ->
    gen_server:call(server(), {increment, Table, Key, CF, Qualifier}).

-spec delete(table(), rowkey()) -> ok.
delete(Table, Key) ->
    gen_server:call(server(), {delete, Table, Key}).

-spec delete(table(), rowkey(), cf()) -> ok.
delete(Table, Key, CF) ->
    gen_server:call(server(), {delete, Table, Key, CF}).

-spec delete(table(), rowkey(), cf(), [qualifier()]) -> ok.
delete(Table, Key, CF, Qualifiers) ->
    gen_server:call(server(), {delete, Table, Key, CF, Qualifiers}).

-define(PROC_NAME, java_diver_server).
-define(JAR_NAME, "diver-0.3.0-dev.jar").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Self = atom_to_list(node()),
    JavaNode = list_to_atom("__diver__" ++ Self),
    Pid = start_jvm(JavaNode),
    case wait_start(Pid, JavaNode) of
        ok ->
            Server = {?PROC_NAME, JavaNode},
            % force connect to HBase by sending arbitrary query
            gen_server:call(Server, {ensure_table_exists, <<"foo">>}, ?CONNECT_TIMEOUT),
            % disable query batching
            gen_server:call(Server, {set_conf, flush_interval, 0}, ?CONNECT_TIMEOUT),
            {ok, #{pid => Pid, java_node => JavaNode}};
        {stop, Reason} ->
            {stop, Reason}
    end.

start_jvm(NodeName) ->
    Self = atom_to_list(node()),
    JarFile = code:priv_dir(hbase) ++ "/" ?JAR_NAME,
    {ok, HbaseQuorum} = application:get_env(hbase, hbase_quorum),
    {ok, HbasePath} = application:get_env(hbase, hbase_path),
    Args = ["-jar", JarFile, Self, NodeName, erlang:get_cookie(), atom_to_list(?PROC_NAME), HbaseQuorum, HbasePath],
    error_logger:info_msg("JVM args: ~p~n", [Args]),
    case os:find_executable("java") of
        false ->
            {stop, no_java_executable};
        ExecPath ->
            open_port({spawn_executable, ExecPath}, [{line, 1000}, {args, Args}, stderr_to_stdout, exit_status])
    end.

wait_start(Pid, JavaNode) ->
    receive
        {Pid, {data, {eol, "READY"}}} ->
            error_logger:info_msg("JVM process started"),
            net_kernel:connect(JavaNode),
            {ok, NodePid} = gen_server:call({?PROC_NAME, JavaNode}, {pid}),
            true = link(NodePid),
            true = erlang:monitor_node(JavaNode, true),
            ok;
        {Pid, {data, {eol, Data}}} ->
            error_logger:info_msg("unknown output from JVM, stopping: ~p", [Data]),
            {stop, Data};
        {Pid, {exit_status, Status}} ->
            error_logger:info_msg("JVM exited with status code: ~p", [Status]),
            {stop, exit};
        Msg ->
            {stop, Msg}
    end.

handle_call(nodename, _From, #{java_node := JavaNode} = State) ->
    {reply, {ok, {?PROC_NAME, JavaNode}}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({node_down, JavaNode}, #{java_node := JavaNode} = S) ->
    error_logger:info_msg("Java server process is down."),
    {stop, nodedown, S};
handle_info({Pid, {data, {eol, Log}}}, #{pid := Pid} = S) ->
    error_logger:info_msg(Log),
    {noreply, S};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #{pid := Pid} = _State) ->
    port_close(Pid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
