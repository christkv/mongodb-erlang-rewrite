-module(wireprotocol_tests).

-include_lib("eunit/include/eunit.hrl").

%%
%%	Wire protocol generation tests
%%

generate_valid_insert_command_test() ->
	% Create a doc we wish to insert
	DocProp = [{bson:utf8("name"), bson:utf8("peter pan")}],
	DocProp1 = [{bson:utf8("name"), bson:utf8("monkey king")}],
	% Serialize the document	
	BinDocProp = bson:serialize(DocProp),
	BinDocProp1 = bson:serialize(DocProp1),
	% Create a wireprotocol binary message
	InsertWireProtocolDoc = mongodb_wire:create_insert(0, <<"db.users">>, 1, [BinDocProp, BinDocProp1]),
	% ?debugFmt("~p~n", [byte_size(InsertWireProtocolDoc)]),
	% ?debugFmt("~p~n", [InsertWireProtocolDoc]),
	[].
	
generate_valid_update_command_test() ->
	% Create a doc we wish to insert
	Selector = [{bson:utf8("name"), bson:utf8("peter pan")}],
 	Document = [{bson:utf8("$set"), {bson:utf8("value"), 1}}],
	% Serialize the document	
	BinSelector = bson:serialize(Selector),
	BinDocument = bson:serialize(Document),
	% Create a wireprotocol binary message
	UpdateWireProtocolDoc = mongodb_wire:create_update(1, <<"db.users">>, 1, 0, BinSelector, BinDocument),
	?debugFmt("~p~n", [byte_size(UpdateWireProtocolDoc)]),
	?debugFmt("~p~n", [UpdateWireProtocolDoc]),
	[].	
	
generate_valid_query_command_test() ->
	% Create a doc we wish to insert
	Query = [{bson:utf8("name"), bson:utf8("peter pan")}],
	% Serialize the document	
	BinQuery = bson:serialize(Query),
	% Create a wireprotocol binary message
	QueryProtocolDoc = mongodb_wire:create_query(1, <<"db.users">>, 0, 100, [tailable], BinQuery, <<>>),
	?debugFmt("~p~n", [byte_size(QueryProtocolDoc)]),
	?debugFmt("~p~n", [QueryProtocolDoc]),
	[].
	
generate_valid_get_more_command_test() ->
	GetMoreProtocolDoc = mongodb_wire:create_get_more(1, <<"db.users">>, 100, 100),
	?debugFmt("~p~n", [byte_size(GetMoreProtocolDoc)]),
	?debugFmt("~p~n", [GetMoreProtocolDoc]),
	[].
	
generate_valid_delete_command_test() ->
	% Create a doc we wish to insert
	Selector = [{bson:utf8("name"), bson:utf8("peter pan")}],
	% Serialize the document	
	BinSelector = bson:serialize(Selector),
	% Create a wireprotocol binary message
	DeleteProtocolDoc = mongodb_wire:create_delete(1, <<"db.users">>, 1, BinSelector),
	?debugFmt("~p~n", [byte_size(DeleteProtocolDoc)]),
	?debugFmt("~p~n", [DeleteProtocolDoc]),
	[].

generate_valid_kill_cursors_command_test() ->
	% Create a wireprotocol binary message
	KillCursorsProtocolDoc = mongodb_wire:create_kill_cursors(1, <<"db.users">>, [2, 3]),
	?debugFmt("~p~n", [byte_size(KillCursorsProtocolDoc)]),
	?debugFmt("~p~n", [KillCursorsProtocolDoc]),
	[].
