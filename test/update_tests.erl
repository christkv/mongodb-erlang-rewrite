-module(update_tests).

-include_lib("eunit/include/eunit.hrl").

% simple_single_document_insert_and_update_test() ->
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
%     Document = [{<<"a">>, Time}, {<<"c">>, 1}],
%     ?debugFmt("-----------------------------------------------------------1 ~n", []),
%     % insert the document with write concern
%     {reply, Result} = mongo_socket:insert(C, <<"erl_test">>, <<"update_tests">>, Document, [{safe, true}]),
%     ?assertEqual(null, proplists:get_value(<<"err">>, lists:nth(1, proplists:get_value(docs, Result)))),
%     ?assertEqual(0, proplists:get_value(<<"n">>, lists:nth(1, proplists:get_value(docs, Result)))),
%     ?debugFmt("-----------------------------------------------------------2 ~n", []),
%     % update document used to add a new variable
%     UpdateDoc = [{<<"$set">>, [{<<"d">>, 1}]}],
%     ?debugFmt("-----------------------------------------------------------3 ~n", []),
%     % % perform an update with write concern
%     {reply, UpdateResult} = mongo_socket:update(C, <<"erl_test">>, <<"update_tests">>, Document, UpdateDoc, [{safe, true}]),
%     % ?debugFmt("-----------------------------------------------------------4 ~n", []),
%     % ?debugFmt("simple_single_document_insert_and_update_test 1 :: ~p~n", [UpdateResult]),


%     % fetch the first object
%     ReturnDoc = mongo_socket:find_one(C, <<"erl_test">>, <<"update_tests">>, Document)
%     % ?debugFmt("simple_single_document_insert_and_update_test 2 :: ~p~n", [ReturnDoc])
%     % ?assertEqual(1, proplists:get_value(<<"b">>, ReturnDoc))
%   end).