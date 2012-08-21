-module(mongo_socket).
% Include all the socket functions
-include_lib("kernel/include/inet.hrl").
% Include the macros for writing code
-include ("mongo.hrl").
% Run the socket as a general otp process
-behaviour(gen_server).
% -compile(export_all).
-export([start_link/2, start_link/3, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% All exported usage methods
-export([is_master/1, insert/4, insert/5, run_command/2, run_command/3, find_one/4]).
-export([update/5, update/6]).

%% ====================================================================
%% constants
%% ====================================================================
-define(INITIAL_RECONNECT_INTERVAL, 500).
-define(DEFAULT_TIMEOUT, 60000).

%% ====================================================================
%% defined type
%% ====================================================================
-type address() :: string() | atom() | inet:ip_address().
-type portnum() :: 0..65535.
-type ctx() :: any().
-type rpb_req() :: atom() | tuple().
-type connection_failure() :: {Reason::term(), FailureCount::integer()}.

-type connection() :: pid().
-type service()    :: {Host :: address(), Post :: portnum()}.
-type options()    :: [option()].
-type option()     :: {timeout, timeout()} | {ssl, boolean()} | ssl.

%% ====================================================================
%% records defining the state of a connection and a request
%% ====================================================================
-record(request, {ref :: reference(), msg :: rpb_req(), from, ctx :: ctx(), timeout :: timeout(),
                  tref :: reference() | undefined }).

-record(state, {address :: address(),    % address to connect to
                port :: portnum(),       % port to connect to
                auto_reconnect = false :: boolean(), % if true, automatically reconnects to server
                                        % if false, exits on connection failure/request timeout
                queue_if_disconnected = false :: boolean(), % if true, add requests to queue if disconnected
                sock :: port(),       % gen_tcp socket
                active :: #request{} | undefined,     % active request
                queue :: queue() | undefined,      % queue of pending requests
                connects=0 :: non_neg_integer(), % number of successful connects
                failed=[] :: [connection_failure()],  % breakdown of failed connects
                connect_timeout=infinity :: timeout(), % timeout of TCP connection
                reconnect_interval=?INITIAL_RECONNECT_INTERVAL :: non_neg_integer()}).

start_link(Address, Port) ->
  % erlang:display("----------------------------------- start_link 0"),
  start_link(Address, Port, []).

start_link(Address, Port, Options) when is_list(Options) ->
  % erlang:display("----------------------------------- start_link 1"),
  gen_server:start_link(?MODULE, [Address, Port, Options], []).

stop(Pid) ->
  % erlang:display("----------------------------------- stop"),
  gen_server:call(Pid, stop).

%% @private
code_change(_OldVsn, State, _Extra) ->
  % erlang:display("----------------------------------- code_change"),
  {ok, State}.

%% @private
handle_cast(_Msg, State) ->
  % erlang:display("----------------------------------- handle_cast"),
  {noreply, State}.

handle_info(_, State) ->
  % erlang:display("----------------------------------- handle_info"),
  {noreply, State}.

init([Address, Port, Options]) ->
  %% Schedule a reconnect as the first action.  If the server is up then
  %% the handle_info(reconnect) will run before any requests can be sent.
  State = parse_options(Options,
        #state{address = Address, port = Port, queue = queue:new()}),
  case State#state.auto_reconnect of
    true ->
      self() ! reconnect,
      {ok, State};
    false ->
      case connect(State) of
        {error, Reason} ->
          {stop, {tcp, Reason}};
        Ok ->
          Ok
      end
  end.

%% @private
terminate(_Reason, _State) ->
  % erlang:display("----------------------------------- terminate"),
  %   erlang:display(_Reason),
  ok.

%% ====================================================================
%% internal connection functions
%% ====================================================================
%% @private
%% Connect the socket if disconnected
connect(State) when State#state.sock =:= undefined ->
  #state{address = Address, port = Port, connects = Connects} = State,
  case gen_tcp:connect(Address, Port,
             [binary, {active, false}, {packet, 0}],
             State#state.connect_timeout) of
    {ok, Sock} ->
      {ok, State#state{sock = Sock, connects = Connects+1,
                       reconnect_interval = ?INITIAL_RECONNECT_INTERVAL}};
    Error ->
      Error
  end.

%% @private
%% Disconnect socket if connected
disconnect(State) ->
  % erlang:display("----------------------------------- disconnect"),
  % %% Tell any pending requests we've disconnected
  % _ = case State#state.active of
  %         undefined ->
  %             ok;
  %         Request ->
  %             send_caller({error, disconnected}, Request)
  %     end,
  %% Make sure the connection is really closed
  case State#state.sock of
      undefined ->
          ok;
      Sock ->
          gen_tcp:close(Sock)
  end,
  NewState = State#state{sock = undefined, active = undefined},
  {stop, disconnected, NewState}.
  % %% Decide whether to reconnect or exit
  % NewState = State#state{sock = undefined, active = undefined},
  % case State#state.auto_reconnect of
  %     true ->
  %         %% Schedule the reconnect message and return state
  %         erlang:send_after(State#state.reconnect_interval, self(), reconnect),
  %         {noreply, increase_reconnect_interval(NewState)};
  %     false ->
  %         {stop, disconnected, NewState}
  % end.

%% ====================================================================
%% internal functions
%% ====================================================================
%% @private
%% Parse options
parse_options([], State) ->
    %% Once all options are parsed, make sure auto_reconnect is enabled
    %% if queue_if_disconnected is enabled.
    case State#state.queue_if_disconnected of
        true ->
            State#state{auto_reconnect = true};
        _ ->
            State
    end;
parse_options([{connect_timeout, T}|Options], State) when is_integer(T) ->
    parse_options(Options, State#state{connect_timeout = T});
parse_options([{queue_if_disconnected,Bool}|Options], State) when
      Bool =:= true; Bool =:= false ->
    parse_options(Options, State#state{queue_if_disconnected = Bool});
parse_options([queue_if_disconnected|Options], State) ->
    parse_options([{queue_if_disconnected, true}|Options], State);
parse_options([{auto_reconnect,Bool}|Options], State) when
      Bool =:= true; Bool =:= false ->
    parse_options(Options, State#state{auto_reconnect = Bool});
parse_options([auto_reconnect|Options], State) ->
    parse_options([{auto_reconnect, true}|Options], State).

%% @doc Return the default or application set timeout for the driver
get_timeout() ->
  case application:get_env(mongodb, timeout) of
  {ok, Timeout} ->
    Timeout;
  undefined ->
    ?DEFAULT_TIMEOUT
  end.

%% ====================================================================
%% server methods
%% ====================================================================

%% @doc Send the ismaster command to the server for the given socket
-spec is_master(pid()) -> {ok, ctx()} | {error, term()}.
is_master(Pid) ->
  gen_server:call(Pid, {q, <<"admin.$cmd">>, [{bson:utf8("isMaster"), 1}], get_timeout(), 0, -1, []}, infinity).

%% @doc Send the ismaster command to the server for the given socket
-spec run_command(pid(), binary(), list()) -> {ok, ctx()} | {error, term()}.
run_command(Pid, DatabaseName, Command) when is_pid(Pid), is_binary(DatabaseName), is_list(Command) ->
  gen_server:call(Pid, {q, <<DatabaseName/binary, <<".$cmd">>/binary>>, Command, get_timeout(), 0, -1, []}, infinity).

-spec run_command(pid(), list()) -> {ok, ctx()} | {error, term()}.
run_command(Pid, Command) when is_pid(Pid), is_list(Command) ->
  gen_server:call(Pid, {q, <<"admin.$cmd">>, Command, get_timeout(), 0, -1, []}, infinity).

%% @doc Insert a document into mongodb
-spec insert(pid(), binary(), binary(), ctx()) -> {ok, ctx()} | {error, term()}.
insert(Pid, DatabaseName, CollectionName, Document) when is_pid(Pid), is_binary(DatabaseName), is_binary(CollectionName), is_list(Document) ->
  insert(Pid, DatabaseName, CollectionName, Document, []).

-spec insert(pid(), binary(), binary(), ctx(), ctx()) -> {ok, ctx()} | {error, term()}.
insert(Pid, DatabaseName, CollectionName, Document, Options) when is_pid(Pid), is_binary(DatabaseName), is_binary(CollectionName), is_list(Document), is_list(Options) ->
  gen_server:call(Pid, {i, DatabaseName, CollectionName, Document, Options, get_timeout()}).

%% @doc Update a document in mongodb
-spec update(pid(), binary(), binary(), ctx(), ctx()) -> {ok, ctx()} | {error, term()}.
update(Pid, DatabaseName, CollectionName, QueryDocument, UpdateDocument) ->
  update(Pid, DatabaseName, CollectionName, QueryDocument, UpdateDocument, []).

-spec update(pid(), binary(), binary(), ctx(), ctx(), ctx()) -> {ok, ctx()} | {error, term()}.
update(Pid, DatabaseName, CollectionName, QueryDocument, UpdateDocument, Options) ->
  gen_server:call(Pid, {u, DatabaseName, CollectionName, QueryDocument, UpdateDocument, Options, get_timeout()}).

%% @doc Find a single document
-spec find_one(pid(), binary(), binary(), ctx()) -> {ok, ctx()} | {error, term()}.
find_one(Pid, DatabaseName, CollectionName, Document) when is_pid(Pid), is_binary(DatabaseName), is_binary(CollectionName), is_list(Document) ->
  % Fetch the result
  case gen_server:call(Pid, {q, <<DatabaseName/binary, <<".">>/binary, CollectionName/binary>>, Document, get_timeout(), 0, -1, []}, infinity) of
    {reply, MongoReply} ->
      % Return the first document
      lists:nth(1, proplists:get_value(docs, MongoReply));
    Error -> Error
  end.

%% ====================================================================
%% handle calls via server
%% ====================================================================

% Stop the connection
handle_call(stop, _From, State) ->
  % erlang:display("----------------------------------- handle_call : stop"),
  % Disconnect the socket
  _ = disconnect(State),
  % return the state that the socket is properly closed
  {stop, normal, ok, State};

%
% Handle query commands
%
handle_call({q, Collection, Document, Timeout, NumberToSkip, NumberToReturn, FlagsList}, _From, State) ->
  % serialize the document to a bson object
  BsonDocument = bson:serialize(Document),
  % create a query binary query message
  QueryBinary = mongodb_wire:create_query(mongopool:next_requestid(), Collection, NumberToSkip, NumberToReturn, FlagsList, BsonDocument, <<>>),
  % send the message
  send_and_receive(QueryBinary, false, Timeout, State);

%
% Handle insert commands
%
handle_call({i, DatabaseName, CollectionName, Document, Options, Timeout}, _From, State) when is_binary(DatabaseName), is_binary(CollectionName), is_list(Document), is_list(Options), is_integer(Timeout) ->
  FullCollectionName = <<DatabaseName/binary, <<".">>/binary, CollectionName/binary>>,
  % serialize the document to a bson object
  BsonDocument = bson:serialize(Document),
  % create a insert binary message
  InsertBinary = mongodb_wire:create_insert(mongopool:next_requestid(), FullCollectionName, 0, BsonDocument),
  % send the message
  case send_and_receive(InsertBinary, true, Timeout, State) of
    {reply, {reply, ok}, State} ->
      execute_write_concern(Timeout, State, DatabaseName, Options);
    Error -> Error
  end;

%
% Handle update commands
%
handle_call({u, DatabaseName, CollectionName, QueryDocument, UpdateDocument, Options, Timeout}, _From, State) when is_binary(DatabaseName), is_binary(CollectionName), is_list(QueryDocument), is_list(UpdateDocument), is_list(Options) ->
  FullCollectionName = <<DatabaseName/binary, <<".">>/binary, CollectionName/binary>>,
  % serialie the documents
  BsonQueryDocument = bson:serialize(QueryDocument),
  BsonUpdateDocument = bson:serialize(UpdateDocument),

  % unpack upsert and multi options
  Upsert = case proplists:is_defined(upsert, Options) of
    true -> 1;
    _ -> 0
  end,
  Multi = case proplists:is_defined(multi, Options) of
    true -> 1;
    _ -> 0
  end,

  % create a update binary message
  UpdateBinary = mongodb_wire:create_update(mongopool:next_requestid(), FullCollectionName, 1, 1, BsonQueryDocument, BsonUpdateDocument),
  erlang:display(UpdateBinary),
  % send the message
  case send_and_receive(UpdateBinary, true, Timeout, State) of
    {reply, {reply, ok}, State} ->
      execute_write_concern(Timeout, State, DatabaseName, Options);
    Error -> Error
  end;

% Handle queries
handle_call(_CallName, _From, _State) ->
  % erlang:display("----------------------------------- handle_call"),
  % erlang:display(CallName),
  % erlang:display(From),
  % erlang:display(State),
  {noreply}.

%
% Handle the write concern settings for getLastError
%
execute_write_concern(Timeout, State, DatabaseName, [{safe, SafeOptions}|Options]) when is_boolean(SafeOptions) ->
  execute_write_concern(Timeout, State, DatabaseName, [{safe, []}|Options]);
execute_write_concern(Timeout, State, DatabaseName, [{safe, SafeOptions}|Options]) ->
  % User has requested a safe write, we need to build a getLastError command and execute it
  GetLastErrorCommand = [{<<"getLastError">>, 1}],
  % Merge in the Safe options
  Document = lists:merge(GetLastErrorCommand, SafeOptions),
  % serialize the document to a bson object
  BsonDocument = bson:serialize(Document),
  % create a query binary query message
  QueryBinary = mongodb_wire:create_query(mongopool:next_requestid(), <<DatabaseName/binary, <<".$cmd">>/binary>>, 0, -1, [], BsonDocument, <<>>),
  % send the message
  send_and_receive(QueryBinary, false, Timeout, State);
execute_write_concern(Timeout, State, Collection, [H|T]) -> execute_write_concern(Timeout, State, Collection, T);
execute_write_concern(_Timeout, State, _Collection, []) -> {reply, {reply, ok}, State}.

%
% Send the binary message
%
send_and_receive(Binary, SendOnly, Timeout, State) when is_binary(Binary), is_boolean(SendOnly), is_integer(Timeout) ->
  % fire off message and ensure we have no error sending the message
  case gen_tcp:send(State#state.sock, Binary) of
    ok ->
      % we are only sending not waiting for a response
      case SendOnly of
        true ->
          {reply, {reply, ok}, State};
        _ ->
          % read the size of the message packet
          case gen_tcp:recv(State#state.sock, 4, Timeout) of
            {ok, <<?get_int32u (N)>>} ->
              % read reminder of the message
              case gen_tcp:recv(State#state.sock, N - 4, Timeout) of
                {ok, BinaryResponse} ->
                  % Unpack the mongo reply
                  MongoReply = mongodb_wire:unpack_mongo_reply(BinaryResponse),
                  % Fetch the first document from the list of docs available
                  FirstDoc = lists:nth(1, proplists:get_value(docs, MongoReply)),
                  % If we have a list of docs get the first one
                  case proplists:is_defined(docs, MongoReply) of
                    true ->
                      % Fetch the first document from the list of docs available
                      FirstDoc = lists:nth(1, proplists:get_value(docs, MongoReply)),
                      % Check if we have an error (signaled by the errmsg field)
                      case proplists:get_value(bson:utf8("errmsg"), FirstDoc) of
                        undefined ->
                          {reply, {reply, MongoReply}, State};
                        Error ->
                          {reply, {error, Error}, State}
                      end;
                    false ->
                      {reply, {reply, MongoReply}, State}
                  end;
                Error ->
                  {reply, Error, State}
              end;
            Error ->
              {reply, Error, State}
          end
      end;
    Error ->
      {reply, Error, State}
  end.













