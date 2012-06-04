%% @author Christian Kvalheim
%% @doc Simple wrapper to work with mongo reply prop list
-module(mongo_reply).

-export([first_document/1]).

first_document(MongoReply) ->
  lists:nth(1, proplists:get_value(docs, MongoReply)).
