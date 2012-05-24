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
-export([is_master/1]).

%% ====================================================================
%% constants
%% ====================================================================
-define(INITIAL_RECONNECT_INTERVAL, 500).
-define(DEFAULT_TIMEOUT, 60000).

%% ====================================================================
%% defined type
%% ====================================================================
-type address() :: string() | atom() | inet:ip_address().
-type portnum() :: non_neg_integer().
-type ctx() :: any().
-type rpb_req() :: atom() | tuple().
-type connection_failure() :: {Reason::term(), FailureCount::integer()}.

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
	erlang:display("----------------------------------- start_link 0"),	
	start_link(Address, Port, []).

start_link(Address, Port, Options) when is_list(Options) ->
	erlang:display("----------------------------------- start_link 1"),	
	gen_server:start_link(?MODULE, [Address, Port, Options], []).

stop(Pid) ->
	erlang:display("----------------------------------- stop"),	
	gen_server:call(Pid, stop).

%% @private
code_change(_OldVsn, State, _Extra) -> 
	erlang:display("----------------------------------- code_change"),
	{ok, State}.

%% @private
handle_cast(_Msg, State) ->
	erlang:display("----------------------------------- handle_cast"),
	{noreply, State}.

handle_info(_, State) ->
	erlang:display("----------------------------------- handle_info"),
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
	erlang:display("----------------------------------- terminate"),
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
	erlang:display("----------------------------------- disconnect"),
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
	% Call the server process for this given socket
	gen_server:call(Pid, {q, [{bson:utf8("isMaster"), 1}], get_timeout()}, infinity).

%% ====================================================================
%% handle calls via server
%% ====================================================================

% Stop the connection
handle_call(stop, _From, State) ->
	erlang:display("----------------------------------- handle_call : stop"),
	% Disconnect the socket
	_ = disconnect(State),
	% return the state that the socket is properly closed
	{stop, normal, ok, State};

% 
% Handle any queries
%
handle_call({q, Document, Timeout}, _From, State) ->
	% serialize the document to a bson object
	BsonDocument = bson:serialize(Document),
	% create a query binary query message
  QueryBinary = mongodb_wire:create_query(mongopool:next_requestid(), <<"admin.$cmd">>, 0, -1, [], BsonDocument, <<>>), 
	% fire off message and ensure we have no error sending the message
	case gen_tcp:send(State#state.sock, QueryBinary) of
	  ok ->
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
	            % Check if we have an error (signaled by the errmsg field)
              case proplists:get_value(bson:utf8("errmsg"), FirstDoc) of
                undefined -> {reply, {reply, MongoReply}, State};
	              Error -> {reply, {error, Error}, State}
	            end;
	          Error ->
        	    {reply, Error, State}
	        end;
	      Error ->
    	    {reply, Error, State}
	    end;
	  Error ->
	    {reply, Error, State}
	end;

% Handle queries
handle_call(CallName, From, State) -> 
	erlang:display("----------------------------------- handle_call"),
	erlang:display(CallName),
	erlang:display(From),
	erlang:display(State),
	{noreply}.
	
	
	
	
	
	
	
	
	
	
	
	
