-module(mongopool_app_tests).

-include_lib("eunit/include/eunit.hrl").

connection_pool_test() ->
  % setup the connection
  application:set_env(mongopool, mongopool_host, "localhost"),  
  application:set_env(mongopool, mongopool_port, 27017),
  % start the mongopool
  S = application:start(mongopool),
  % execute the command
  {ok, {reply, CommandResult}} = mongopool:execute(fun(C) -> mongo_socket:is_master(C) end),
  % validate that we have a ok for the isMaster and a max bson object field
  FirstDoc = lists:nth(1, proplists:get_value(docs, CommandResult)),
  % validate that the command was ok
  ?assertNot(proplists:get_value(bson:utf8("ok"), FirstDoc) == undefined),
  ?assertNot(proplists:get_value(bson:utf8("maxBsonObjectSize"), FirstDoc) == undefined),
  % execute the command
  ?assertEqual(1, mongopool:count()),
  application:stop(mongopool),
  application:unset_env(mongopool, mongopool_host),
  application:unset_env(mongopool, mongopool_port).

multiple_connections_in_connection_pool_test() ->
  % setup the connection
  application:set_env(mongopool, mongopool_host, "localhost"),  
  application:set_env(mongopool, mongopool_port, 27017),
  % start the mongopool
  S = application:start(mongopool),
  % spawn 3 is_master 
  spawn(mongopool, execute, [fun(C) -> mongo_socket:is_master(C) end]),
  spawn(mongopool, execute, [fun(C) -> mongo_socket:is_master(C) end]),
  spawn(mongopool, execute, [fun(C) -> mongo_socket:is_master(C) end]),
  % sleep to let 3 connections get created
  timer:sleep(100),
  % assert the number of connections  
  ?assertEqual(3, mongopool:count()),
  application:stop(mongopool),
  application:unset_env(mongopool, mongopool_host),
  application:unset_env(mongopool, mongopool_port).
	
connection_authenticate_test() ->
  % ?debugFmt("connection_pool_test :: ~p~n", [mongopool:execute(Fun)]),
  [].
