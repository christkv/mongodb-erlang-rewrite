%% @author David Weldon
%% @hidden

-module(mongopool_sup).
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ConnectionSup =
        {mongopool_connection_sup, {mongopool_connection_sup, start_link, []},
         permanent, 2000, supervisor, [mongopool_connection_sup]},   
    Pool =
        {mongopool, {mongopool, start_link, []},
         permanent, 2000, worker, [mongopool]},
    {ok, {{one_for_all, 5, 30}, [ConnectionSup, Pool]}}.
