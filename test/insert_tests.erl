-module(insert_tests).

-include_lib("eunit/include/eunit.hrl").

simple_single_document_insert_safe_test() ->
  % setup the connection
  application:set_env(mongopool, mongopool_host, "localhost"),
  application:set_env(mongopool, mongopool_port, 27017),
  % start the mongopool
  application:start(mongopool),
  % insert a simple document not using getlast error
  Result = mongopool:execute(fun(C) ->
    % create timestamp
    {A, B, D} = erlang:now(),
    Time = bson:utf8(integer_to_list(A) ++ integer_to_list(B) ++ integer_to_list(D)),
    % create a simple document, prop list
    Document = [{<<"a">>, Time}, {<<"b">>, 1}],
    % insert the document with write concern
    {reply, Result} = mongo_socket:insert(C, <<"erl_test">>, <<"insert_tests">>, Document, [{safe, true}]),
    ?assertEqual(null, proplists:get_value(<<"err">>, lists:nth(1, proplists:get_value(docs, Result)))),
    ?assertEqual(0, proplists:get_value(<<"n">>, lists:nth(1, proplists:get_value(docs, Result)))),
    % fetch the first object
    ReturnDoc = mongo_socket:find_one(C, <<"erl_test">>, <<"insert_tests">>, Document),
    ?assertEqual(1, proplists:get_value(<<"b">>, ReturnDoc))
  end).

simple_single_document_insert_with_journal_set_test() ->
  % setup the connection
  application:set_env(mongopool, mongopool_host, "localhost"),
  application:set_env(mongopool, mongopool_port, 27017),
  % start the mongopool
  application:start(mongopool),
  % insert a simple document not using getlast error
  Result = mongopool:execute(fun(C) ->
    % create timestamp
    {A, B, D} = erlang:now(),
    Time = bson:utf8(integer_to_list(A) ++ integer_to_list(B) ++ integer_to_list(D)),
    % create a simple document, prop list
    Document = [{<<"a">>, Time}, {<<"b">>, 2}],
    % insert the document with no write concern
    {reply, Result} = mongo_socket:insert(C, <<"erl_test">>, <<"insert_tests">>, Document, [{safe, [{j, true}]}]),
    ?assertEqual(null, proplists:get_value(<<"err">>, lists:nth(1, proplists:get_value(docs, Result)))),
    ?assertEqual(0, proplists:get_value(<<"n">>, lists:nth(1, proplists:get_value(docs, Result)))),
    % fetch the first object
    ReturnDoc = mongo_socket:find_one(C, <<"erl_test">>, <<"insert_tests">>, Document),
    ?assertEqual(1, proplists:get_value(<<"b">>, ReturnDoc))
  end).
