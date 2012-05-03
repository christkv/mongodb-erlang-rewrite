-module(mongopool_app_tests).

-include_lib("eunit/include/eunit.hrl").

connection_pool_test() ->
  application:set_env(mongopool, mongopool_host, "localhost"),
  application:set_env(mongopool, mongopool_port, 27017),
  S = application:start(mongopool),
  Fun = fun(C) -> pong end,
  ?assertEqual({ok, pong}, mongopool:execute(Fun)),
  ?assertEqual(1, mongopool:count()),
  application:stop(mongopool),
  application:unset_env(mongopool, mongopool_host),
  application:unset_env(mongopool, mongopool_port).
	
