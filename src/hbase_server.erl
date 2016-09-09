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
-export([get/2, get/3, get/4, scan/3, scan_sync/2]).
-export([put/5, compare_and_set/6, increment/4]).
-export([delete/2, delete/3, delete/4]).

-spec server() -> {atom(), atom()}.
server() ->
    {ok, NodeName} = gen_server:call(?MODULE, nodename),
    NodeName.

-spec get(binary(), binary()) -> {ok, list()}.
get(Table, Key) ->
    gen_server:call(server(), {get, Table, Key}).

-spec get(binary(), binary(), binary()) -> {ok, list()}.
get(Table, Key, CF) ->
    gen_server:call(server(), {get, Table, Key, CF}).

-spec get(binary(), binary(), binary(), binary()) -> {ok, list()}.
get(Table, Key, CF, Qualifier) ->
    gen_server:call(server(), {get, Table, Key, CF, Qualifier}).

-spec scan(binary(), list(), reference()) -> ok.
scan(Table, Opts, Ref) ->
    gen_server:call(server(), {scan, Table, Opts, Ref}).

-spec scan_sync(binary(), list()) -> ok.
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

-spec put(binary(), binary(), binary(), list(binary()), list(binary())) -> {ok, list()}.
put(Table, Key, CF, Qualifiers, Values) ->
    gen_server:call(server(), {put, {Table, Key, CF, Qualifiers, Values}}).

-spec compare_and_set(binary(), binary(), binary(), binary(), binary(), binary()) -> {ok, list()}.
compare_and_set(Table, Key, CF, Qualifier, Value, Expected) ->
    gen_server:call(server(), {put, {Table, Key, CF, [Qualifier], [Value]}, Expected}).

-spec increment(binary(), binary(), binary(), binary()) -> {ok, number()}.
increment(Table, Key, CF, Qualifier) ->
    gen_server:call(server(), {increment, Table, Key, CF, Qualifier}).

-spec delete(binary(), binary()) -> ok.
delete(Table, Key) ->
    gen_server:call(server(), {delete, Table, Key}).

-spec delete(binary(), binary(), binary()) -> ok.
delete(Table, Key, CF) ->
    gen_server:call(server(), {delete, Table, Key, CF}).

-spec delete(binary(), binary(), binary(), binary()) -> ok.
delete(Table, Key, CF, Qualifiers) ->
    gen_server:call(server(), {delete, Table, Key, CF, Qualifiers}).

-define(PROC_NAME, java_diver_server).
-define(JAR_NAME, "diver-0.3.0-dev.jar").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Self = atom_to_list(node()),
    JarFile = code:priv_dir(hbase) ++ "/" ?JAR_NAME,
    JavaNode = list_to_atom("__diver__" ++ Self),
    {ok, HbaseQuorum} = application:get_env(hbase, hbase_quorum),
    {ok, HbasePath} = application:get_env(hbase, hbase_path),
    Args = ["-jar", JarFile, Self, JavaNode, erlang:get_cookie(), atom_to_list(?PROC_NAME), HbaseQuorum, HbasePath],
    io:format("args: ~p~n", [Args]),
    Pid = open_port({spawn_executable, "/usr/bin/java"}, [{line, 1000}, {args, Args}, stderr_to_stdout]),
    io:format("pid: ~p~n", [Pid]),
    case wait_start(Pid, JavaNode) of
        ok ->
            {ok, #{pid => Pid, java_node => JavaNode}};
        {stop, Reason} ->
            {stop, Reason}
    end.

wait_start(Pid, JavaNode) ->
    receive
        {Pid, {data, {eol, "READY"}}} ->
            error_logger:info_msg("Successfully started Java server process"),
            net_kernel:connect(JavaNode),
            {ok, NodePid} = gen_server:call({?PROC_NAME, JavaNode}, {pid}),
            true = link(NodePid),
            true = erlang:monitor_node(JavaNode, true),
            ok;
        {Pid, {data, {eol, Data}}} ->
            {stop, Data};
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
