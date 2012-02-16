%%%----------------------------------------------------------------------
%%% File    : simple_demo.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : 
%%% Created : 03 Apr. 2010
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.monit.cn
%%%----------------------------------------------------------------------
-module(simple_demo).

-author('ery.lee@gmail.com').

-behavior(gen_server).

-export([start_link/0]).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {conn, chan}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
	{ok, Conn} = amqp:connect_link([]),
	{ok, Chan} = amqp:open_channel(Conn),
	{ok, _} = amqp:queue(Chan, "demo"),
	{ok, _} = amqp:queue(Chan, "rpc"),
	{ok, _, _} = amqp:consume(Chan, "demo"),
	{ok, _, _} = amqp:consume(Chan, "rpc"),
	erlang:monitor(process, Chan),
    {ok, #state{conn = Conn, chan = Chan}}.
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Req, _From, State) ->
    {reply, {error, badreq}, State}.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({deliver, <<"demo">>, Header, Payload}, State) ->
	io:format("demo header: ~p~n",[Header]),
	io:format("demo payload: ~p~n", [Payload]),
    {noreply, State};

handle_info({deliver, <<"rpc">>, Header, Payload}, #state{chan = Channel} = State) ->
	io:format("rpc header: ~p~n",[Header]),
	io:format("rpc payload: ~p~n", [Payload]),
	ReplyTo = proplists:get_value(reply_to, Header),
	ReqId = proplists:get_value(correlation_id, Header),
	if
	(ReplyTo == undefined) or (ReqId == undefined) ->
		ignore;
	true ->
		amqp:reply(Channel, ReplyTo, ReqId, "reply")
	end,
    {noreply, State};
	
handle_info(Info, State) ->
	io:format("unexpected info: ~p~n", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.
%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

