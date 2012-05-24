%% @author Christian Kvalheim
%% Based on riakpool by David Weldon
%% @doc Based on the riakpool using the same pattern where an execute will check out a connection, 
%% execute the operation and then check the connection back in.

-module(mongopool).
-behaviour(gen_server).

-export([count/0,
         execute/1,
         start_link/0,
         start_pool/0,
         start_pool/2,
         stop/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([next_requestid/0]).

-record(state, {host, port, pids}).

%% @type host() = string() | atom().

%% @spec count() -> integer()
%% @doc Returns the number of connections as seen by the supervisor.
count() ->
	Props = supervisor:count_children(mongopool_connection_sup),
	case proplists:get_value(active, Props) of
		N when is_integer(N) -> N;
		undefined -> 0
	end.

%% @spec execute(Fun) -> {ok, Value::any()} | {error, any()}
%%       Fun = function(pid())
%% @doc Finds the next available connection pid from the pool and calls
%% `Fun(Pid)'. Returns `{ok, Value}' if the call was successful, and
%% `{error, any()}' otherwise. If no connection could be found, a new connection
%% will be established.
%% ```
%% > riakpool:execute(fun(C) -> riakc_pb_socket:ping(C) end).
%% {ok,pong}
%% '''
execute(Fun) ->
	case gen_server:call(?MODULE, check_out) of
		{ok, Pid} ->
		  try {ok, Fun(Pid)}
		  catch _:E -> {error, E}
		  after gen_server:cast(?MODULE, {check_in, Pid}) end;
	  {error, E} -> {error, E}
	end.

%% @spec start_link() -> {ok, pid()} | {error, any()}
%% @doc Starts the server.
start_link() -> 
  % erlang:display("==================================================== INIT"),
  % erlang:display(?MODULE),
  % setup counter
  ets:new (?MODULE, [named_table, public]),	
  % insert some counters
  ets:insert (?MODULE, [
  	{oid_counter, 0},
  	{oid_machineprocid, oid_machineprocid()},
  	{max_bson_object_size, 0},
  	{requestid_counter, 0} ]),
  % execute isMaster to get the details about the connection
  % erlang:display("------------------------------------------------------- start_link() -- 0"),
  % start the pool
	Result = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
	% if it's ok execute is_master
	case Result of
	  {ok, Pid} ->
	    % Execute is_master
      Fun = fun(C) -> mongo_socket:is_master(C) end,
      % The result from calling isMaster
	    IsMasterResult = mongopool:execute(Fun),
      % erlang:display(IsMasterResult),
	    % Unpack the ismaster command
	    case IsMasterResult of
	      {ok, {reply, Reply}} ->
          % grab the first returned doc
          FirstDoc = lists:nth(1, proplists:get_value(docs, Reply)),
          % grab the max bson size for documents
          MaxBsonObjectSize = proplists:get_value(bson:utf8("maxBsonObjectSize"), FirstDoc),
          % erlang:display("------------------------------------------------------- start_link() :: 0 -- 0"),
          % add the max object size to the ets table
        	ets:insert(?MODULE, [{max_bson_object_size, MaxBsonObjectSize}]),
          % erlang:display("------------------------------------------------------- start_link() :: 1 -- 0"),
        	% return the result
          Result;
	      {ok, Error} ->
          % erlang:display("======================================================= ERROR"),
          % erlang:display(Error),
	        Error
	    end;
	  _ ->
	    Result
	end.

%% @spec start_pool() -> ok | {error, any()}
%% @doc Starts a connection pool to a server listening on {"127.0.0.1", 27017}.
%% Note that a pool can only be started once.
start_pool() -> 
	start_pool("127.0.0.1", 27017).

%% @spec start_pool(host(), integer()) -> ok | {error, any()}
%% @doc Starts a connection pool to a server listening on {`Host', `Port'}.
%% Note that a pool can only be started once.
start_pool(Host, Port) when is_integer(Port) ->
  % start the pool
	Result = gen_server:call(?MODULE, {start_pool, Host, Port}),
	Result.

%% @spec stop() -> ok
%% @doc Stops the server.
stop() -> 
  % clear out the ets table
  ets:delete(?MODULE),
  % close the pool
	gen_server:cast(?MODULE, stop).

%% @hidden
init([]) ->
	% process flags
	process_flag(trap_exit, true),
		case [application:get_env(P) || P <- [mongopool_host, mongopool_port]] of
		  [{ok, Host}, {ok, Port}] when is_integer(Port) ->
			  {ok, new_state(Host, Port)};
		  _ -> {ok, undefined}
		end.

%% @hidden
oid_machineprocid() ->
	OSPid = list_to_integer (os:getpid()),
	{ok, Hostname} = inet:gethostname(),
	<<MachineId:3/binary, _/binary>> = erlang:md5 (Hostname),
	<<MachineId:3/binary, OSPid:16/big>>.

-spec next_requestid () -> mongo_protocol:requestid(). % IO
%@doc Fresh request id
next_requestid() -> ets:update_counter (?MODULE, requestid_counter, 1).

%% @hidden
handle_call({start_pool, Host, Port}, _From, undefined) ->
	case new_state(Host, Port) of
		State=#state{} -> {reply, ok, State};
		undefined -> {reply, {error, connection_error}, undefined}
	end;
handle_call({start_pool, _Host, _Port}, _From, State=#state{}) ->
	{reply, {error, pool_already_started}, State};
handle_call(check_out, _From, undefined) ->
	{reply, {error, pool_not_started}, undefined};
handle_call(check_out, _From, State=#state{host=Host, port=Port, pids=Pids}) ->
	case next_pid(Host, Port, Pids) of
	  {ok, Pid, NewPids} -> {reply, {ok, Pid}, State#state{pids=NewPids}};
	  {error, NewPids} ->
		  {reply, {error, connection_error}, State#state{pids=NewPids}}
	end;
handle_call(_Request, _From, State) -> {reply, ok, State}.

%% @hidden
handle_cast({check_in, Pid}, State=#state{pids=Pids}) ->
	NewPids = queue:in(Pid, Pids),
	{noreply, State#state{pids=NewPids}};
handle_cast(stop, State) -> 
	{stop, normal, State};
handle_cast(_Msg, State) -> 
	{noreply, State}.

%% @hidden
handle_info(_Info, State) -> {noreply, State}.

%% @hidden
terminate(_Reason, undefined) -> ok;
terminate(_Reason, #state{pids=Pids}) ->
	% stop the pool
	StopFun =
	  fun(Pid) ->
		  case is_process_alive(Pid) of
		    true -> mongo_socket:stop(Pid);
		    false -> ok
		  end
	  end,
	[StopFun(Pid) || Pid <- queue:to_list(Pids)], ok.

%% @hidden
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @spec new_state(host(), integer()) -> state() | undefined
%% @doc Returns a state with a single pid if a connection could be established,
%% otherwise returns undefined.
new_state(Host, Port) ->
	case new_connection(Host, Port) of
	  {ok, Pid} ->
		  #state{host=Host, port=Port, pids=queue:in(Pid, queue:new())};
	  error -> undefined
	end.

%% @spec new_connection(host(), integer()) -> {ok, Pid} | error
%% @doc Returns {ok, Pid} if a new connection was established and added to the
%% supervisor, otherwise returns error.
new_connection(Host, Port) ->
	case supervisor:start_child(mongopool_connection_sup, [Host, Port]) of
	  {ok, Pid} when is_pid(Pid) -> 
	    {ok, Pid};
	  {ok, Pid, _} when is_pid(Pid) -> 
	    {ok, Pid};
	  _ -> 
	    error
	end.

%% @spec next_pid(host(), integer(), queue()) -> {ok, pid(), queue()} |
%%                                               {error, queue()}
%% @doc Recursively dequeues Pids in search of a live connection. Dead
%% connections are removed from the queue as it is searched. If no connection
%% pid could be found, a new one will be established. Returns {ok, Pid, NewPids}
%% where NewPids is the queue after any necessary dequeues. Returns error if no
%% live connection could be found and no new connection could be established.
next_pid(Host, Port, Pids) ->
	case queue:out(Pids) of
	  {{value, Pid}, NewPids} ->
      % erlang:display("================================================ have pid"),
		  case is_process_alive(Pid) of
		    true -> {ok, Pid, NewPids};
		    false -> next_pid(Host, Port, NewPids)
		  end;
	  {empty, _} ->
      % erlang:display("================================================ don't pid"),
		  case new_connection(Host, Port) of
		    {ok, Pid} -> {ok, Pid, Pids};
		    error -> {error, Pids}
		  end
	end.