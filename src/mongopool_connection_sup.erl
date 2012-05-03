%% @author David Weldon
%% @hidden

-module(mongopool_connection_sup).
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{connections, {mongo_socket, start_link, []},
            temporary, brutal_kill, worker, [mongo_socket]}]}}.
