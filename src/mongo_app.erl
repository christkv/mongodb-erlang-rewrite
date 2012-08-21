-module(mongo_app).

-behaviour(application).
-export ([
  start/2,
  stop/1
]).


%% @hidden
start(_, _) ->
  erlang:display("=============================== mongo_app:start 0"),
  erlang:display(mongo_sup),
  erlang:display("=============================== mongo_app:start 1"),
  Result = mongo_sup:start_link(),
  erlang:display("=============================== mongo_app:start 2"),
  erlang:display(Result),
  Result.

%% @hidden
stop(_) ->
  erlang:display("=============================== mongo_app:stop"),
  ok.

