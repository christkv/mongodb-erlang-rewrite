-module(find_tests).

-include_lib("eunit/include/eunit.hrl").

% simple_find_one_test() ->
%   % setup the connection
%   application:set_env(mongopool, mongopool_host, "localhost"),
%   application:set_env(mongopool, mongopool_port, 27017),
%   % start the mongopool
%   application:start(mongopool),
%   % insert a simple document not using getlast error
%   Result = mongopool:execute(fun(C) ->
%     % create timestamp
%     {A, B, D} = erlang:now(),
%     Time = bson:utf8(integer_to_list(A) ++ integer_to_list(B) ++ integer_to_list(D)),
%     % create a simple document, prop list
%     Document = [{<<"a">>, Time}],
%     % insert the document with no write concern
%     mongo_socket:insert(C, <<"erl_test">>, <<"test">>, Document),
%     % sleep a little then refetch it from the db
%     ok = timer:sleep(100),
%     % fetch the record
%     ReturnDoc = mongo_socket:find_one(C, <<"erl_test">>, <<"test">>, Document),
%     % ?debugFmt("simple_find_one_test 1 :: ~p~n", [Time]),
%     % ?debugFmt("simple_find_one_test 2 : : ~p~n", [proplists:get_value(<<"a">>, ReturnDoc)]),
%     % fetch value
%     ?assertEqual(Time, proplists:get_value(<<"a">>, ReturnDoc))
%   end).
