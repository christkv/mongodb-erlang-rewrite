-module(mongo_socket).
% Include all the socket functions
-include_lib("kernel/include/inet.hrl").
% Run the socket as a general otp process
-behaviour(gen_server).
% -compile(export_all).

-export([start_link/2, start_link/3, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ====================================================================
%% constants
%% ====================================================================
-define(INITIAL_RECONNECT_INTERVAL, 500).

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
	erlang:display("----------------------------------- init"),
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
             [binary, {active, once}, {packet, 4}, {header, 1}],
             State#state.connect_timeout) of
		{ok, Sock} ->
			{ok, State#state{sock = Sock, connects = Connects+1,
			                 reconnect_interval = ?INITIAL_RECONNECT_INTERVAL}};
		Error ->
			Error
	end.

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


%% ====================================================================
%% handle calls via server
%% ====================================================================
handle_call(_, _, _) -> 
	erlang:display("----------------------------------- handle_call"),
	{noreply}.
