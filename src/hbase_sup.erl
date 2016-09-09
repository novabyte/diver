-module(hbase_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 1000, Type, [I]}).

init([]) ->
    {ok, { {one_for_all, 0, 1}, [
        ?CHILD(hbase_server, worker)
    ]} }.
