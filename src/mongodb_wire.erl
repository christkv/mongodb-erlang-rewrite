-module(mongodb_wire).

-export([create_insert/4, create_update/6, create_query/6, create_query/7, create_get_more/4, create_delete/4, create_kill_cursors/3, unpack_mongo_reply/1]).

% Include the macros for writing code
-include ("mongo.hrl").

%
%
% Unpack a MongoReply message
unpack_mongo_reply(BinaryMessage) when is_binary(BinaryMessage) ->
  % unpack the binary message header (length has been shared off by read from socket)
  <<?get_int32u (RequestID),
    ?get_int32u (ResponseTo),
    ?get_int32u (OpCode),
    CursorNotFound:1, QueryFailure:1, ShardConfigState:1, AwaitCapable:1, _:28,
    ?get_int64u (CursorID),
    ?get_int32u (StartingFrom),
    ?get_int32u (NumberReturned),
    BsonDocuments/binary>> = BinaryMessage,
  % deserialize all the bson documents
  Docs = documents_to_docs(NumberReturned, BsonDocuments),
  % package the results up as a property list
  [{request_id, RequestID},
   {response_to, ResponseTo},
   {op_code, OpCode},
   {flags,
    [{cursor_not_found, CursorNotFound},
     {query_failure, QueryFailure},
     {shard_config_state, ShardConfigState},
     {await_capable, AwaitCapable}]},
   {cursor_id, CursorID},
   {starting_from, StartingFrom},
   {number_returned, NumberReturned},
   {docs, Docs}].

documents_to_docs(N, BsonDocument) when is_integer(N), is_binary(BsonDocument), byte_size(BsonDocument) > 0 ->
  % unpack length of document
  <<?get_int32u (DocumentLength), _/binary>> = BsonDocument,
  % split of the document
  <<FirstDocument:DocumentLength/binary, Reminder/binary>> = BsonDocument,
  % deserialize the bson document
  Document = bson:deserialize(FirstDocument),
  % Deserialize the next document
  [Document] ++ documents_to_docs(N - 1, Reminder);
documents_to_docs(N , _) when N =< 0 -> [].

% 	Header
%   	int32   messageLength;
%			int32   requestID;
%			int32   responseTo;
%			int32   opCode;
%		Insert
%			int32     flags;
%			cstring   fullCollectionName;
%			document* documents;
% Create an insert wire protocol message to send to server
create_insert(RequestID, FullCollectionName, ContinueOnError, InsertMessage) when is_integer(RequestID), is_binary(FullCollectionName), is_integer(ContinueOnError), is_binary(InsertMessage) ->
	create_insert(RequestID, FullCollectionName, ContinueOnError, [InsertMessage]);
create_insert(RequestID, FullCollectionName, ContinueOnError, InsertMessages) when is_integer(RequestID), is_binary(FullCollectionName), is_integer(ContinueOnError), is_list(InsertMessages) ->
	% Flatten the list of messages
	InsertMessageBytes = erlang:list_to_binary(InsertMessages),
	% Calculate total size of the wire command for insert, the wire message format is below
	TotalInsertSize = (4 + 4 + 4 + 4) + (4) + (byte_size(FullCollectionName) + 1) + byte_size(InsertMessageBytes),
	% Generate the 32 bit flag values and set ContinueOnError if defined
	Flags = <<ContinueOnError:1, 0:31>>,
	% Generate the single binary for the protocol
	Message = <<?put_int32u(TotalInsertSize)
			, ?put_int32u(RequestID)
			, ?put_int32u(0)
			, ?put_int32u(2002)
			, Flags/binary
			, FullCollectionName/binary, 0	% cstring terminated with 0
			, InsertMessageBytes/binary
		>>,
	% Return the finished binary message
	Message.

% 	Header
%   	int32   messageLength;
%			int32   requestID;
%			int32   responseTo;
%			int32   opCode;
%		Update
%			int32     ZERO;
%			cstring   fullCollectionName;
%			int32     flags;
%			document  selector;
%			document  update;
% Create an update wire protocol message to send to server
create_update(RequestID, FullCollectionName, Upsert, MultiUpdate, Selector, Document) when is_integer(RequestID), is_binary(FullCollectionName), is_integer(Upsert), is_integer(MultiUpdate), is_binary(Selector), is_binary(Document) ->
	% Calculate total size of the wire command for insert, the wire message format is below
	TotalInsertSize = (4 + 4 + 4 + 4) + (4) + (byte_size(FullCollectionName) + 1) + (4) + byte_size(Selector) + byte_size(Document),
  erlang:display("======================================================== create_update"),
  erlang:display(binary:bin_to_list(FullCollectionName)),
  erlang:display(binary:bin_to_list(Selector)),
  erlang:display(binary:bin_to_list(Document)),
  erlang:display(TotalInsertSize),
  erlang:display("======================================================== create_update DONE"),

	% Generate the 32 bit flag values and set ContinueOnError if defined
	Flags = <<Upsert:1, MultiUpdate:1, 0:30>>,
	% Generate the single binary for the protocol
	Message = <<?put_int32u(TotalInsertSize)
			, ?put_int32u(RequestID)
			, ?put_int32u(0)
			, ?put_int32u(2001)
			, ?put_int32u(0)
			, FullCollectionName/binary, 0	% cstring terminated with 0
			, Flags/binary
			, Selector/binary
			, Document/binary
		>>,
	% Return the finished binary message
	Message.

% 	Header
%   	int32   messageLength;
%			int32   requestID;
%			int32   responseTo;
%			int32   opCode;
%		Query
%			int32     flags;
%			cstring   fullCollectionName;
%			int32     numberToSkip;
%			int32     numberToReturn;
%			document  query;
%			document  fieldsSelector[optional];
% Create a query wire protocol message to send to server
create_query(RequestID, FullCollectionName, NumberToSkip, NumberToReturn, FlagsList, Query) when is_integer(RequestID), is_binary(FullCollectionName), is_integer(NumberToSkip), is_integer(NumberToReturn), is_list(FlagsList), is_binary(Query) ->
	create_query(RequestID, FullCollectionName, NumberToSkip, NumberToReturn, FlagsList, Query, <<>>).
create_query(RequestID, FullCollectionName, NumberToSkip, NumberToReturn, FlagsList, Query, Fields) when is_integer(RequestID), is_binary(FullCollectionName), is_integer(NumberToSkip), is_integer(NumberToReturn), is_list(FlagsList), is_binary(Query), is_binary(Fields)->
	% Calculate total size of the wire command for insert, the wire message format is below
	TotalInsertSize = (4 + 4 + 4 + 4) + (4) + (byte_size(FullCollectionName) + 1) + (4 + 4) + byte_size(Query) + byte_size(Fields),
	% Unpack state of all flags
	TailableCursor = bool_to_value(lists:member(tailable, FlagsList)),
	SlaveOk = bool_to_value(lists:member(slave_ok, FlagsList)),
	OplogReplay = bool_to_value(lists:member(oplog_replay, FlagsList)),
	NoCursorTimeout = bool_to_value(lists:member(no_cursor_timeout, FlagsList)),
	AwaitData = bool_to_value(lists:member(await_data, FlagsList)),
	Exhaust = bool_to_value(lists:member(exhaust, FlagsList)),
	Partial = bool_to_value(lists:member(partial, FlagsList)),

	% Build the flags for the operation
	Flags = <<0:1, TailableCursor:1, SlaveOk:1, OplogReplay:1, NoCursorTimeout:1, AwaitData:1, Exhaust:1, Partial:1, 0:24>>,
	% Generate the single binary for the protocol
	Message = <<?put_int32u(TotalInsertSize)
			, ?put_int32u(RequestID)
			, ?put_int32u(0)
			, ?put_int32u(2004)
			, Flags/binary
			, FullCollectionName/binary, 0	% cstring terminated with 0
			, ?put_int32u(NumberToSkip)
			, ?put_int32(NumberToReturn)
			, Query/binary
			, Fields/binary
		>>,
	% Return the finished binary message
	Message.

% 	Header
%   	int32   messageLength;
%			int32   requestID;
%			int32   responseTo;
%			int32   opCode;
%		GetMoreCommand
%			int32     ZERO;
%			cstring   fullCollectionName;
%			int32     numberToReturn;
%			int64     cursorID;
% Create an get more wire protocol message to send to server
create_get_more(RequestID, FullCollectionName, NumberToReturn, CursorId) when is_integer(RequestID), is_binary(FullCollectionName), is_integer(NumberToReturn), is_integer(CursorId) ->
	% Calculate total size of the wire command for insert, the wire message format is below
	TotalInsertSize = (4 + 4 + 4 + 4) + (4) + (byte_size(FullCollectionName) + 1) + (4 + 8),
	% Generate the single binary for the protocol
	Message = <<?put_int32u(TotalInsertSize)
			, ?put_int32u(RequestID)
			, ?put_int32u(0)
			, ?put_int32u(2005)
			, ?put_int32u(0)
			, FullCollectionName/binary, 0	% cstring terminated with 0
			, ?put_int32u(NumberToReturn)
			, ?put_int64u(CursorId)
		>>,
	% Return the finished binary message
	Message.

% 	Header
%   	int32   messageLength;
%			int32   requestID;
%			int32   responseTo;
%			int32   opCode;
%		DeleteCommand
%			int32     ZERO;
%			cstring   fullCollectionName;
%			int32     flags;
%			document  selector;
% Create a delete wire protocol message to send to server
create_delete(RequestID, FullCollectionName, Single, Selector) when is_integer(RequestID), is_binary(FullCollectionName), is_integer(Single), is_binary(Selector) ->
	% Calculate total size of the wire command for insert, the wire message format is below
	TotalInsertSize = (4 + 4 + 4 + 4) + (4) + (byte_size(FullCollectionName) + 1) + (4) + byte_size(Selector),
	% Generate the 32 bit flag values and set Single if defined
	Flags = <<Single:1, 0:31>>,
	% Generate the single binary for the protocol
	Message = <<?put_int32u(TotalInsertSize)
			, ?put_int32u(RequestID)
			, ?put_int32u(0)
			, ?put_int32u(2006)
			, ?put_int32u(0)
			, FullCollectionName/binary, 0	% cstring terminated with 0
			, Flags/binary
			, Selector/binary
		>>,
	% Return the finished binary message
	Message.

% 	Header
%   	int32   messageLength;
%			int32   requestID;
%			int32   responseTo;
%			int32   opCode;
%		DeleteCommand
%			int32     ZERO;
%			int32     numberOfCursorIDs;
%			int64*    cursorIDs;
% Create a kill cursors wire protocol message to send to server
create_kill_cursors(RequestID, FullCollectionName, CursorIds) when is_integer(RequestID), is_binary(FullCollectionName), is_list(CursorIds) ->
	% Calculate total size of the wire command for insert, the wire message format is below
	TotalInsertSize = (4 + 4 + 4 + 4) + (4 + 4) + (8 * length(CursorIds)),
	% Use list comprehension to map 64 bit cursor id to binary
	CursorIdsBinaries = list_to_binary([ <<?put_int64u(X)>> || X <- CursorIds]),
	% Generate the single binary for the protocol
	Message = <<?put_int32u(TotalInsertSize)
			, ?put_int32u(RequestID)
			, ?put_int32u(0)
			, ?put_int32u(2006)
			, ?put_int32u(0)
			, ?put_int32u(length(CursorIds))
			, CursorIdsBinaries/binary
		>>,
	% Return the finished binary message
	Message.

% Convert a bool to a value usable for the binary bit
bool_to_value(true) -> 1;
bool_to_value(false) -> 0.

