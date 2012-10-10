-module(wireprotocol_tests).

-include_lib("eunit/include/eunit.hrl").

%%
%%	Wire protocol generation tests
%%

generate_valid_insert_command_test() ->
	% Create a doc we wish to insert
	DocProp = [{bson:utf8("name"), bson:utf8("peter pan")}, 
		{bson:utf8("_id"), bson:objectid("50759122b6f598f07f000001")}],
	DocProp1 = [{bson:utf8("name"), bson:utf8("monkey king")},
		{bson:utf8("_id"), bson:objectid("50759132b6f598f07f000001")}],
	% Serialize the documents
	BinDocProp = bson:serialize(DocProp),
	BinDocProp1 = bson:serialize(DocProp1),
	% Create a wireprotocol binary message
	_InsertWireProtocolDoc = mongodb_wire:create_insert(0, <<"test.test">>, 1, BinDocProp),
	% ?debugFmt("~p~n", [byte_size(_InsertWireProtocolDoc)]),
	% ?debugFmt("~p~n", [_InsertWireProtocolDoc]),
	?assertEqual(<<72,0,0,0,0,0,0,0,0,0,0,0,210,7,0,0,1,0,0,0,116,101,115,116,46,
		116,101,115,116,0,42,0,0,0,2,110,97,109,101,0,10,0,0,0,112,101,116,101,114,
		32,112,97,110,0,7,95,105,100,0,80,117,145,34,182,245,152,240,127,0,0,1,0>>, 
		_InsertWireProtocolDoc),

	% multiple document insert protocol message
	_InsertWireProtocolDoc2 = mongodb_wire:create_insert(0, <<"test.test">>, 1, [BinDocProp, BinDocProp1]),
	?assertEqual(<<116,0,0,0,0,0,0,0,0,0,0,0,210,7,0,0,1,0,0,0,116,101,115,116,
		46,116,101,115,116,0,42,0,0,0,2,110,97,109,101,0,10,0,0,0,112,101,116,101,
		114,32,112,97,110,0,7,95,105,100,0,80,117,145,34,182,245,152,240,127,0,0,1,
		0,44,0,0,0,2,110,97,109,101,0,12,0,0,0,109,111,110,107,101,121,32,107,105,
		110,103,0,7,95,105,100,0,80,117,145,50,182,245,152,240,127,0,0,1,0>>, 
		_InsertWireProtocolDoc2).
	
generate_valid_update_command_test() ->
	% Create a doc we wish to insert
	Selector = [{bson:utf8("name"), bson:utf8("peter pan")}],
 	Document = [{bson:utf8("$set"), {bson:utf8("value"), 1}}],
	% Serialize the document	
	BinSelector = bson:serialize(Selector),
	BinDocument = bson:serialize(Document),
	% Create a wireprotocol binary message
	_UpdateWireProtocolDoc = mongodb_wire:create_update(1, <<"db.users">>, 1, 0, BinSelector, BinDocument),
	% ?debugFmt("~p~n", [byte_size(UpdateWireProtocolDoc)]),
	% ?debugFmt("~p~n", [UpdateWireProtocolDoc]),
	[].	
	
generate_valid_query_command_test() ->
	% Create a doc we wish to insert
	Query = [{bson:utf8("name"), bson:utf8("peter pan")}],
	% Serialize the document	
	BinQuery = bson:serialize(Query),
	% Create a wireprotocol binary message
	_QueryProtocolDoc = mongodb_wire:create_query(1, <<"db.users">>, 0, 100, [tailable], BinQuery, <<>>),
	% ?debugFmt("~p~n", [byte_size(QueryProtocolDoc)]),
	% ?debugFmt("~p~n", [QueryProtocolDoc]),
	[].
	
generate_valid_get_more_command_test() ->
	_GetMoreProtocolDoc = mongodb_wire:create_get_more(1, <<"db.users">>, 100, 100),
	% ?debugFmt("~p~n", [byte_size(GetMoreProtocolDoc)]),
	% ?debugFmt("~p~n", [GetMoreProtocolDoc]),
	[].
	
generate_valid_delete_command_test() ->
	% Create a doc we wish to insert
	Selector = [{bson:utf8("name"), bson:utf8("peter pan")}],
	% Serialize the document	
	BinSelector = bson:serialize(Selector),
	% Create a wireprotocol binary message
	_DeleteProtocolDoc = mongodb_wire:create_delete(1, <<"db.users">>, 1, BinSelector),
	% ?debugFmt("~p~n", [byte_size(DeleteProtocolDoc)]),
	% ?debugFmt("~p~n", [DeleteProtocolDoc]),
	[].

generate_valid_kill_cursors_command_test() ->
	% Create a wireprotocol binary message
	_KillCursorsProtocolDoc = mongodb_wire:create_kill_cursors(1, <<"db.users">>, [2, 3]),
	% ?debugFmt("~p~n", [byte_size(KillCursorsProtocolDoc)]),
	% ?debugFmt("~p~n", [KillCursorsProtocolDoc]),
	[].
