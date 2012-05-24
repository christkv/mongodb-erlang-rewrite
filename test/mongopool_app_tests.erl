-module(mongopool_app_tests).

-include_lib("eunit/include/eunit.hrl").

connection_pool_test() ->
  % setup the connection
  application:set_env(mongopool, mongopool_host, "localhost"),  
  application:set_env(mongopool, mongopool_port, 27017),
  % start the mongopool
  S = application:start(mongopool),
  % set up a command
  Fun = fun(C) -> mongo_socket:is_master(C) end,
  % execute the command
  {ok, {reply, CommandResult}} = mongopool:execute(Fun),
  % validate that we have a ok for the isMaster and a max bson object field
  FirstDoc = lists:nth(1, proplists:get_value(docs, CommandResult)),
  % validate that the command was ok
  ?assertNot(proplists:get_value(bson:utf8("ok"), FirstDoc) == undefined),
  ?assertNot(proplists:get_value(bson:utf8("maxBsonObjectSize"), FirstDoc) == undefined),
  % ?debugFmt("connection_pool_test :: ~p~n", [mongopool:execute(Fun)]),
  % ?assertEqual({ok, pong}, mongopool:execute(Fun)),
  ?assertEqual(1, mongopool:count()),
  application:stop(mongopool),
  application:unset_env(mongopool, mongopool_host),
  application:unset_env(mongopool, mongopool_port).
	
