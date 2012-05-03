%% @author Christian Kvalheim
%% Based on riakpool by David Weldon
%% @hidden

-module(mongopool_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) -> mongopool_sup:start_link().

stop(_State) -> ok.