-module(mongo_sup).
-export([
  start_link/0,
  start_pool/3,
  stop_pool/1
]).

-behaviour(supervisor).
-export([init/1]).

-define(SUPERVISOR(Id, Tag),       {Id, {supervisor, start_link, [?MODULE, Tag]}, permanent, infinity, supervisor, [?MODULE]}).
-define(SUPERVISOR(Id, Name, Tag), {Id, {supervisor, start_link, [{local, Name}, ?MODULE, Tag]}, permanent, infinity, supervisor, [?MODULE]}).
-define(WORKER(M, F, A, R),        {M,  {M, F, A}, R, 5000, worker, [M]}).

-spec start_link() -> {ok, pid()}.
start_link() ->
  erlang:display("======================================= start_link"),
  supervisor:start_link({local, ?MODULE}, ?MODULE, app).

-spec start_pool(atom(), pos_integer(), mongo_socket:service()) -> {ok, pid()}.
start_pool(Name, Size, Service) ->
  erlang:display("======================================= start_pool"),
  {ok, Supervisor} = supervisor:start_child(mongo_pools_sup, ?SUPERVISOR(Name, pool)),
  {ok, Connections} = supervisor:start_child(Supervisor, ?SUPERVISOR(mongo_connections_sup, {connections, Service, [{timeout, 5000}]})),
  supervisor:start_child(Supervisor, ?WORKER(mongo_pool, start_link, [Name, Size, Connections], permanent)).

-spec stop_pool(term()) -> ok.
stop_pool(Name) ->
  erlang:display("======================================= stop_pool"),
  supervisor:terminate_child(mongo_pools_sup, Name),
  supervisor:delete_child(mongo_pools_sup, Name).

%% @hidden
init(app) ->
  erlang:display("======================================= init:app"),
  {ok, {
    {one_for_one, 5, 10}, [
      ?WORKER(mongo_id_server, start_link, [], permanent),
      ?SUPERVISOR(mongo_cursors_sup, mongo_cursors_sup, cursors),
      ?SUPERVISOR(mongo_pools_sup, mongo_pools_sup, pools)
    ]
  }};

init(pools) ->
  erlang:display("======================================= init:pools"),
  {ok, {
    {one_for_one, 5, 10}, [
      %% Pool supervisors are dynamically created
    ]
  }};

init(pool) ->
  erlang:display("======================================= init:pool"),
  {ok, {
    {one_for_all, 5, 10}, [
      %% Pool Manager dynamically created
      %% Pool Connection supervisor dynamically created
    ]
  }};

init({connections, Service, Options}) ->
  erlang:display("======================================= init:connections"),
  {ok, {
    {simple_one_for_one, 5, 10}, [
      ?WORKER(mongopool_connection, start_link, [Service, Options], temporary)
    ]
  }}.

