%%%----------------------------------------------------------------------
%%% File    : amqp.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : amqp client api
%%% Created : 03 Aug 2009
%%% License : http://www.opengoss.com
%%% Descr   : implement api like ruby amqp library tmm1
%%% 
%%% Copyright (C) 2007-2009, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(amqp).

-include("amqp_client.hrl").

-import(proplists, [get_value/3]).

-export([start_link/1,
         start_link/2, 
         start_link/4, 
         stop/0, 
         stop/1]).

%%api 
-export([request/3, reply/4,
        queue/1, queue/2, queue/3,
        exchange/2, exchange/3, exchange/4,
        direct/2, direct/3,
        topic/2, topic/3,
        fanout/2, fanout/3,
        bind/3, bind/4, 
        unbind/3,
        send/3,
        publish/3, publish/4, publish/5,
        delete/3,
        consume/2, consume/3, 
        get/2,
        ack/2, 
        cancel/2, %,
		open/1,
		close/1
        ]).

%%callbacks
-export([init/1, 
        handle_call/3, 
        handle_cast/2, 
        handle_info/2, 
        terminate/2, 
        code_change/3]).

-record(state, {params, realm, connection, channel, ticket, reply_queue, reply_consumer_tag, dict}).

-define(RPC_TIMEOUT, 3000).

%% @spec start_link(Opts) -> Result
%%  Opts = [tuple()]
%%  Result = {ok, pid()}  | {error, Error}  
%% @doc stop amqp client
start_link(Opts) ->
    start_link(?MODULE, Opts).

%% @spec start_link(Name, Opts) -> Result
%%  Name = atom()
%%  Opts = [tuple()]
%%  Result = {ok, pid()}  | {error, Error}  
%% @doc stop amqp client
start_link(Name, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Opts], []).

start_link(Name, Opts, Succ, Fail) ->
    case start_link(Name, Opts) of
    {ok, Pid} ->
        Succ(Pid),
        {ok, Pid};
    {error, Error} ->
        Fail(Error),
        {ok, undefined}
    end.

%% @spec stop() -> ok
%% @doc stop amqp client
stop() ->
    stop(?MODULE).

%% @spec stop(Name) -> ok
%%  Name = atom()
%% @doc stop amqp client
stop(Name) ->
    gen_server:call(Name, stop).

%% @spec request(Pid, To, Request) -> Result
%%  Pid = pid() | atom()
%%  To = iolist()
%%  Request = term()
%%  Result = {ok, Reply} | {error,Error}
%%  Reply = term()
%%  Error = term() 
%% @doc rpc request
request(Pid, To, Request) ->
    call(Pid, {request, To, Request}).

reply(Pid, To, Id, Reply) ->
    call(Pid, {reply, To, Id, Reply}).

%% @spec queue(Pid) -> Result
%%  Pid = pid() | atom()
%%  Result = {ok, Q, Props} | {error,Error}
%%  Q = iolist()
%%  Props = [tuple()]
%% @doc declare temporary queue
queue(Pid) ->
    call(Pid, queue).

%% @spec queue(Pid, Name) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp queue
queue(Pid, Name) ->
    cast(Pid, {queue, binary(Name)}).
    %call(Pid, {queue, binary(Name)}).

%% @spec queue(Pid, Name, Opts) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp queue
queue(Pid, Name, Opts) ->
    call(Pid, {queue, binary(Name), Opts}).

%% @spec exchange(Pid, Name) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp exchange with default type 'direct'
exchange(Pid, Name) ->
    call(Pid, {exchange, binary(Name)}).

%% @spec exchange(Pid, Name, Type) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Type = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp exchange
exchange(Pid, Name, Type) ->
    call(Pid, {exchange, binary(Name), Type}).

%% @spec exchange(Pid, Name, Type, Opts) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Type = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp exchange
exchange(Pid, Name, Type, Opts) ->
    call(Pid, {exchange, binary(Name), Type, Opts}).

%% @spec direct(Pid, Name) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp direct exchange
direct(Pid, Name) ->
    call(Pid, {direct, binary(Name)}).

%% @spec direct(Pid, Name, Opts) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp direct exchange
direct(Pid, Name, Opts) ->
    call(Pid, {direct, binary(Name), Opts}).

%% @spec topic(Pid, Name) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp topic exchange
topic(Pid, Name) ->
    call(Pid, {topic, binary(Name)}).

%% @spec topic(Pid, Name, Opts) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp topic exchange
topic(Pid, Name, Opts) ->
    call(Pid, {topic, binary(Name), Opts}).

%% @spec fanout(Pid, Name) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Result = ok | {error,Error}
%% @doc declare amqp fanout
fanout(Pid, Name) ->
    call(Pid, {fanout, binary(Name)}).

%% @spec fanout(Pid, Name, Opts) -> Result
%%  Pid = pid() | atom()
%%  Name = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc declare amqp fanout
fanout(Pid, Name, Opts) ->
    call(Pid, {fanout, binary(Name), Opts}).

%% @spec bind(Pid, Exchange, Queue) -> Result
%%  Pid = pid() | atom()
%%  Exchange = iolist()
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc amqp bind
bind(Pid, Exchange, Queue) ->
    call(Pid, {bind, binary(Exchange), binary(Queue)}).

%% @spec bind(Pid, Exchange, Queue, RouteKey) -> Result
%%  Pid = pid() | atom()
%%  Exchange = iolist()
%%  Queue = iolist()
%%  Opts = list()
%%  Result = ok | {error,Error}
%% @doc amqp bind
bind(Pid, Exchange, Queue, RoutingKey) ->
    call(Pid, {bind, binary(Exchange), binary(Queue), binary(RoutingKey)}).

%% @spec unbind(Pid, Exchange, Queue) -> Result
%%  Pid = pid() | atom()
%%  Exchange = iolist()
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc amqp bind
unbind(Pid, Exchange, Queue) ->
    call(Pid, {unbind, binary(Exchange), binary(Queue)}).

%% @spec send(Pid, Queue, Payload) -> Result
%%  Pid = pid() | atom()
%%  Queue = iolist()
%%  Payload = binary()
%% @doc send directly to queue
send(Pid, Queue, Payload) ->
    cast(Pid, {send, binary(Queue), binary(Payload)}).

%% @spec publish(Pid, Exchange, Payload) -> Result
%%  Pid = pid() | atom()
%%  Exchange = iolist()
%%  Payload = binary()
%% @doc amqp publish message
publish(Pid, Exchange, Payload) ->
    cast(Pid, {publish, binary(Exchange), binary(Payload), <<"">>}).

%% @spec publish(Pid, Exchange, Payload, RoutingKey) -> Result
%%  Pid = pid() | atom()
%%  Exchange = iolist()
%%  Payload = binary()
%%  RoutingKey = iolist()
%% @doc amqp publish message
publish(Pid, Exchange, Payload, RoutingKey) ->
    cast(Pid, {publish, binary(Exchange), binary(Payload), binary(RoutingKey)}).

%% @spec publish(Pid, Exchange, Properties, Payload, RoutingKey) -> Result
%%  Pid = pid() | atom()
%%  Exchange = iolist()
%%  Properties = [tuple()]
%%  Payload = binary()
%%  RoutingKey = iolist()
%% @doc amqp publish message
publish(Pid, Exchange, Properties, Payload, RoutingKey) ->
    cast(Pid, {publish, binary(Exchange), Properties, binary(Payload), binary(RoutingKey)}).

%% @spec get(Pid, Queue) -> Result
%%  Pid = pid() | atom()
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc subscribe to a queue
get(Pid, Queue) ->
    call(Pid, {get, binary(Queue)}).

%% @spec consume(Pid, Queue) -> Result
%%  Pid = pid() | atom()
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc subscribe to a queue
consume(Pid, Queue) ->
    call(Pid, {consume, binary(Queue), self()}).

%% @spec consume(Pid, Queue, Consumer) -> Result
%%  Pid = pid() | atom()
%%  Queue = iolist()
%%  Consumer = pid() | fun()
%%  Result = ok | {error,Error}
%% @doc subscribe to a queue
consume(Pid, Queue, Consumer) ->
    call(Pid, {consume, binary(Queue), Consumer}).

%% @spec delete(Pid, queue, Queue) -> Result
%%  Pid = pid() | atom()
%%  Queue = iolist()
%%  Result = ok | {error,Error}
%% @doc delete a queue
delete(Pid, queue, Queue) ->
    call(Pid, {delete, queue, binary(Queue)});

%% @spec delete(Pid, exchange, Exchange) -> Result
%%  Pid = pid() | atom()
%%  Exchange = iolist()
%%  Result = ok | {error,Error}
%% @doc delete an exchange
delete(Pid, exchange, Exchange) ->
    call(Pid, {delete, exchange, binary(Exchange)}).

%% @spec ack(Pid, DeliveryTag) -> Result
%%  Pid = pid() | atom()
%%  DeliveryTag = iolist()
%%  Result = ok | {error,Error}
%% @doc ack a message
ack(Pid, DeliveryTag) ->
    cast(Pid, {ack, binary(DeliveryTag)}).

%% @spec cancel(Pid, ConsumerTag) -> Result
%%  Pid = pid() | atom()
%%  ConsumerTag = iolist()
%%  Result = ok | {error,Error}
%% @doc cancel a consumer
cancel(Pid, ConsumerTag) ->
    cast(Pid, {cancel, binary(ConsumerTag)}).

open(Pid) ->
	call(Pid, open).

%%  close() -> Result
%%  close a channel
close(Pid) ->
    call(Pid, close).

call(Pid, Req) ->
    gen_server:call(Pid, Req).

cast(Pid, Msg) ->
    gen_server:cast(Pid, Msg).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Opts]) ->
    case do_init(Opts) of
    {ok, State} ->
        {ok, State#state{dict = dict:new()}};
    {error, Error} ->
        {stop, Error};
    {'EXIT', Reason} ->
        {stop, Reason}
    end.

do_init(Opts) ->
    process_flag(trap_exit, true),
    Host = get_value(host, Opts, "localhost"),
    Port = get_value(port, Opts, 5672),
    VHost = get_value(vhost, Opts, <<"/">>),
    Realm = get_value(realm, Opts, <<"/">>),
    User = get_value(user, Opts, <<"guest">>),
    Password = get_value(password, Opts, <<"guest">>),
    %%Start a connection to the server
    Params = #amqp_params_network{host = Host, port = Port, virtual_host = VHost, username = User, password = Password},
    {Conn, Chan, Ticket} = connect(Params, Realm),
	link(Conn),
	link(Chan),
    {ok, #state{params = Params, realm = Realm, ticket = Ticket, connection = Conn, channel = Chan}}.

connect(Params, Realm) ->
    {ok, Conn} = amqp_connection:start(Params),
    %link(Conn),
    %% Once you have a connection to the server, you can start an AMQP channel gain access to a realm
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Access = #'access.request'{realm = Realm,
                               exclusive = false,
                               passive = true,
                               active = true,
                               write = true,
                               read = true},
    #'access.request_ok'{ticket = Ticket} = amqp_channel:call(Chan, Access),
    {Conn, Chan, Ticket}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({request, To, Req}, From, #state{reply_queue = RepQ} = State) ->
    NewState =
    case RepQ of
    undefined ->
        {ok, Q} = declare_queue(State),
        {ok, ConsumeTag} = basic_consume(Q, State),
        %?INFO("rpc ConsumerTag: ~p", [ConsumeTag]),
        State#state{reply_queue = Q, reply_consumer_tag = ConsumeTag};
    _ -> 
        State
    end,
    %TODO: should monitor From.
    ReqId = basic_request(To, term_to_binary(Req), NewState), 
    Timer = erlang:send_after(?RPC_TIMEOUT, self(), {rpc_timeout, ReqId}),
    put(ReqId, {From, Timer}),
    {noreply, NewState};

handle_call({reply, To, ReqId, Req}, _From, State) ->
    Payload = term_to_binary(Req),
    basic_reply(To, ReqId, Payload, State),
    {reply, ok, State};

handle_call(queue, _From, State) ->
    {ok, Q} = declare_queue(State),
    {reply, {ok, Q}, store({queue, Q}, true, State)};

handle_call({queue, Name}, _From, State) ->
    {ok, Q} = declare_queue(Name, [], State),
    {reply, {ok, Q}, store({queue, Name}, true, State)};

handle_call({queue, Name, Opts}, _From, State) ->
    {ok, Q} = declare_queue(Name, Opts, State),
    {reply, {ok, Q}, store({queue, Name}, true, State)};

handle_call({exchange, Name}, _From, State) ->
    declare_exchange(Name, <<"direct">>, [], State),
    NewState = store({exchange, Name}, {direct, []}, State),
    {reply, ok, NewState};

handle_call({exchange, Name, Type}, _From, State) ->
    declare_exchange(Name, Type, [], State),
    NewState = store({exchange, Name}, {Type, []}, State),
    {reply, ok, NewState};

handle_call({exchange, Name, Type, Opts}, _From, State) ->
    declare_exchange(Name, Type, Opts, State),
    NewState = store({exchange, Name}, {Type, []}, State),
    {reply, ok, NewState};

handle_call({direct, Name}, _From, State) ->   
    declare_exchange(Name, <<"direct">>, [], State),
    NewState = store({exchange, Name}, {direct, []}, State),
    {reply, ok, NewState};

handle_call({direct, Name, Opts}, _From, State) ->   
    declare_exchange(Name, <<"direct">>, Opts, State),
    NewState = store({exchange, Name}, {direct, Opts}, State),
    {reply, ok, NewState};

handle_call({topic, Name}, _From, State) ->   
    declare_exchange(Name, <<"topic">>, [], State),
    NewState = store({exchange, Name}, {topic, []}, State),
    {reply, ok, NewState};

handle_call({topic, Name, Opts}, _From, State) ->   
    declare_exchange(Name, <<"topic">>, Opts, State),
    NewState = store({exchange, Name}, {topic, Opts}, State),
    {reply, ok, NewState};

handle_call({fanout, Name}, _From, State) ->   
    declare_exchange(Name, <<"fanout">>, [], State),
    NewState = store({exchange, Name}, {fanout, []}, State),
    {reply, ok, NewState};

handle_call({fanout, Name, Opts}, _From, State) ->   
    declare_exchange(Name, <<"fanout">>, Opts, State),
    NewState = store({exchange, Name}, {fanout, Opts}, State),
    {reply, ok, NewState};

handle_call({bind, Exchange, Queue}, _From, State) -> 
    bind_queue(Exchange, Queue, <<"">>, State),
    {reply, ok, State};

handle_call({bind, Exchange, Queue, RoutingKey}, _From, State) -> 
    bind_queue(Exchange, Queue, RoutingKey, State),
    {reply, ok, State};

handle_call({unbind, Exchange, Queue}, _From, State) -> 
    unbind_queue(Exchange, Queue, <<"">>, State),
    {reply, ok, State};

handle_call({get, Queue}, _From, State) ->
    case basic_get(Queue, State) of
    {ok, Reply} -> 
        {reply, {ok, Reply}, State};
    {error, Reason} ->
        {reply, {error, Reason}, State}
    end;

handle_call({consume, Queue, Consumer}, _From, State) ->
    case basic_consume(Queue, State) of
    {ok, ConsumerTag} -> 
        MonRef = erlang:monitor(process, Consumer),
        NewState = store({consumer, ConsumerTag}, {Consumer, MonRef}, State),
        {reply, {ok, ConsumerTag}, NewState};
    {error, Reason} ->
        {reply, {error, Reason}, State}
    end;

handle_call({cancel, ConsumerTag}, _From, #state{dict = D} = State) ->
    basic_cancel(ConsumerTag, State),
    NewDict = dict:erase({consumer, ConsumerTag}, D),
    {reply, ok, State#state{dict = NewDict}};

handle_call({delete, queue, Queue}, _From, #state{dict = D} = State) ->
    delete_queue(Queue, State),
    NewDict = dict:erase({queue, Queue}, D),
    {reply, ok, State#state{dict = NewDict}};

handle_call({delete, exchange, Exchange}, _From, #state{dict = D} = State) ->
    delete_exchange(Exchange, State),
    NewDict = dict:erase({exchange, Exchange}, D),
    {reply, ok, State#state{dict = NewDict}};

handle_call(open, _From, #state{connection = Conn, channel = _} = State) ->
    {ok, Chann} = amqp_connection:open_channel(Conn),
    {reply, {ok, Chann}, State#state{channel = Chann}};

handle_call(close, _From, #state{channel = Channel} = State) ->
    amqp_channel:close(Channel),
    {reply, ok, State#state{channel = undefined}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    {stop, {unexpected_req, Req}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({queue, Name}, State) ->
    {ok, Q} = declare_queue(Name, [], State),
    {noreply, store({queue, Name}, true, State)};

handle_cast({send, Queue, Payload}, State) ->
    basic_send(Queue, Payload, State),
    {noreply, State};

handle_cast({publish, Exchange, Payload, RoutingKey}, State) ->
    basic_publish(Exchange, none, Payload, RoutingKey, State),
    {noreply, State};

handle_cast({publish, Exchange, Properties, Payload, RoutingKey}, State) ->
    basic_publish(Exchange, Properties, Payload, RoutingKey, State),
    {noreply, State};

handle_cast({ack, DeliveryTag}, State) ->
    basic_ack(DeliveryTag, State),
    {noreply, State};

handle_cast(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({#'basic.deliver'{consumer_tag = ConsumerTag,
    delivery_tag = _DeliveryTag, 
    redelivered = _Redelivered,
    exchange = _Exchange,
    routing_key = _RoutingKey}, #amqp_msg{props = Properties, payload = Payload} = _Msg},
    #state{reply_consumer_tag = ConsumerTag} = State) ->
    %?INFO("amqp reply got", []),
    #'P_basic'{content_type = _ContentType, correlation_id = ReqId} = Properties,
    case get(ReqId) of
    {From, Timer} ->
        cancel_timer(Timer),
        gen_server:reply(From, binary_to_term(Payload));
    undefined ->
		ignore
        %?ERROR("unexpected rpc reply: ~p", [binary_to_term(Payload)])    
    end,
    {noreply, State};

handle_info({#'basic.deliver'{consumer_tag = ConsumerTag,
    delivery_tag = _DeliveryTag, 
    redelivered = _Redelivered, 
    exchange = _Exchange, 
    routing_key = RoutingKey}, #amqp_msg{props = Properties, payload = Payload} = _Msg}, 
    #state{dict = Dict} = State) ->
    #'P_basic'{content_type = ContentType, correlation_id = CorrelationId, reply_to = ReplyTo} = Properties,
    %?INFO("get amqp message :~p, ~p, ~p", [ConsumerTag, binary_to_term(Payload), dict:to_list(Dict)]),
    case dict:find({consumer, ConsumerTag}, Dict) of
    {ok, {Consumer, _Ref}} ->
        %?INFO("get consumer message :~p", [Consumer]),
        Consumer ! {deliver, RoutingKey, [{content_type, ContentType}, 
            {correlation_id, CorrelationId}, {reply_to, ReplyTo}], Payload};
    error -> 
		ignore
        %?ERROR("no available consumer for: ~p", [ConsumerTag])
    end,
    {noreply, State};

handle_info({'DOWN', MonRef, _Type, _Object, _Info}, #state{dict = Dict} = State) ->
    L = lists:filter(
        fun({{consumer, _Tag}, {_Pid, Ref}}) -> 
            Ref == MonRef;
           ({_K, _V}) ->
            false
    end, dict:to_list(Dict)),
    NewDict = lists:foldl(fun({{consumer, Tag}, _Val}, Acc) -> 
        basic_cancel(Tag, State), 
        dict:erase({consumer, Tag}, Acc)
    end, Dict, L),
    {noreply, State#state{dict = NewDict}};

handle_info({rpc_timeout, ReqId}, State) ->
    case get(ReqId) of
    {From, Timer} ->
        cancel_timer(Timer),
        gen_server:reply(From, {error, rpc_timeout}),
        erase(ReqId);
    undefined -> 
		ignore
        %?ERROR("cannot find timeout req: ~p", [ReqId])
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
terminate(_Reason, #state{connection = Conn} = _State) ->
    amqp_connection:close(Conn),
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
store(Key, Val, #state{dict = Dict} = State) ->
    Dict1 = dict:store(Key, Val, Dict),
    State#state{dict = Dict1}.

declare_queue( #state{channel = Channel, ticket = Ticket}  = _State) ->
    QueueDeclare = #'queue.declare'{ticket = Ticket, auto_delete = true},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, QueueDeclare),
    {ok, Q}.

declare_queue(Name, Opts, #state{channel = Channel, ticket = Ticket}  = _State) ->
    Q = binary(Name),
	try
    Durable = get_value(durable, Opts, false),
    Exclusive = get_value(exclusive, Opts, false),
    AutoDelete = get_value(auto_delete, Opts, true),
    QueueDeclare = #'queue.declare'{ticket = Ticket, queue = Q,
                                    passive = false, durable = Durable,
                                    exclusive = Exclusive, auto_delete = AutoDelete,
                                    nowait = false, arguments = []},
     #'queue.declare_ok'{queue = Q,
                        message_count = _MessageCount,
                        consumer_count = _ConsumerCount}
                        = amqp_channel:call(Channel, QueueDeclare)
	catch
	_:Err -> io:format("~p~n", [Err])
	end,
    {ok, Q}.
    %io:format("~n~s: queue '~p' message_count: ~p~n", [node(), Name, MessageCount]),
    %io:format("~n~s: queue '~p' consumer_count: ~p~n", [node(), Name, ConsumerCount]).

declare_exchange(Name, Type, _Opts, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Name),
    ExchangeDeclare = #'exchange.declare'{ticket = Ticket,
                                          exchange = X, type = Type,
                                          passive = false, durable = true,
                                          auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare).

bind_queue(Exchange, Queue, RoutingKey, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    Q = binary(Queue),
    R = binary(RoutingKey),
    QueueBind = #'queue.bind'{ticket = Ticket,
                              queue = Q,
                              exchange = X,
                              routing_key = R,
                              nowait = false, arguments = []},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).

unbind_queue(Exchange, Queue, RoutingKey, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    Q = binary(Queue),
    QueueUnbind = #'queue.unbind'{ticket = Ticket,
                              queue = Q,
                              exchange = X,
                              routing_key = RoutingKey,
                              arguments = []},
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, QueueUnbind).

basic_request(ReqQ, Payload, #state{channel = Channel, ticket = Ticket, 
    reply_queue = RepQ} = _State) ->
    K = binary(ReqQ),
    BasicPublish = #'basic.publish'{ticket = Ticket,
                                    %exchange = X,
                                    routing_key = K,
                                    mandatory = false,
                                    immediate = false},
    CorrelationId = uuid:v4(),
    Props = #'P_basic'{content_type = <<"application/octet-stream">>,
                 delivery_mode = 1,
                 priority = 3,
                 correlation_id = CorrelationId,
                 reply_to = RepQ},
    Msg = #amqp_msg{props = Props, payload = Payload},
    amqp_channel:cast(Channel, BasicPublish, Msg),
    CorrelationId.

basic_reply(ReqQ, ReqId, Payload, #state{channel = Channel, ticket = Ticket} = _State) ->
    K = binary(ReqQ),
    BasicPublish = #'basic.publish'{ticket = Ticket,
                                    %exchange = X,
                                    routing_key = K,
                                    mandatory = false,
                                    immediate = true},
    Props = #'P_basic'{content_type = <<"application/octet-stream">>,
                 delivery_mode = 1,
                 priority = 3,
                 correlation_id = ReqId},
    Msg = #amqp_msg{props = Props, payload = Payload},
    amqp_channel:cast(Channel, BasicPublish, Msg).

basic_send(Queue, Payload0, #state{channel = Channel, ticket = Ticket} = _State) ->
    K = binary(Queue),
    Payload = binary(Payload0),
    BasicPublish = #'basic.publish'{ticket = Ticket,
                                    %exchange = X,
                                    routing_key = K,
                                    mandatory = false,
                                    immediate = false},
    Msg = #amqp_msg{props = basic_properties(), payload = Payload},
    amqp_channel:cast(Channel, BasicPublish, Msg).

basic_publish(Exchange, _Properties, Payload0, RoutingKey, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    K = binary(RoutingKey),
    Payload = binary(Payload0),
    BasicPublish = #'basic.publish'{ticket = Ticket,
                                    exchange = X,
                                    routing_key = K,
                                    mandatory = false,
                                    immediate = false},
    Msg = #amqp_msg{props = basic_properties(), payload = Payload},

    amqp_channel:cast(Channel, BasicPublish, Msg).

basic_get(Queue, #state{channel = Channel, ticket = Ticket} = _State) ->
    Q = binary(Queue),
    %% Basic get
    BasicGet = #'basic.get'{ticket = Ticket, queue = Q},
    case amqp_channel:call(Channel, BasicGet) of
    {#'basic.get_ok'{exchange = _E, routing_key = RoutingKey, message_count = _C},  
     #'amqp_msg'{props = Properties, payload = Payload}} ->
        #'P_basic'{content_type = ContentType} = Properties,
        {ok, {RoutingKey, [{content_type, ContentType}], Payload}};
    #'basic.get_empty'{cluster_id = _} ->
        {ok, []}
    end.

basic_consume(Queue, #state{channel = Channel, ticket = Ticket} = _State) ->
    Q = binary(Queue),
    %% Register a consumer to listen to a queue
    BasicConsume = #'basic.consume'{ticket = Ticket,
                                    queue = Q,
                                    consumer_tag = <<"">>,
                                    no_local = false,
                                    no_ack = true,
                                    exclusive = false,
                                    nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag}
                     = amqp_channel:subscribe(Channel, BasicConsume, self()),
    %% If the registration was sucessful, then consumer will be notified
    receive
    #'basic.consume_ok'{consumer_tag = ConsumerTag} -> 
        {ok, ConsumerTag}
    after 1000 -> 
        {error, consume_timeout}
    end.

basic_cancel(ConsumerTag, #state{channel = Channel} = _State) ->
    BasicCancel = #'basic.cancel'{consumer_tag = ConsumerTag, nowait = false},
    #'basic.cancel_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicCancel).

basic_ack(DeliveryTag, #state{channel = Channel} = _State) ->
    BasicAck = #'basic.ack'{delivery_tag = DeliveryTag},
    amqp_channel:cast(Channel,BasicAck).

delete_queue(Queue, #state{channel = Channel, ticket = Ticket} = _State) ->
    Q = binary(Queue),
    QueueDelete = #'queue.delete'{ticket = Ticket, queue = Q},
    #'queue.delete_ok'{message_count = _MessageCount} = amqp_channel:call(Channel, QueueDelete).

delete_exchange(Exchange, #state{channel = Channel, ticket = Ticket} = _State) ->
    X = binary(Exchange),
    ExchangeDelete = #'exchange.delete'{ticket = Ticket, exchange= X},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete).


binary(L) when is_list(L) ->
    list_to_binary(L);

binary(B) when is_binary(B) ->
    B.

basic_properties() ->
  #'P_basic'{content_type = <<"application/octet-stream">>,
             delivery_mode = 1,
             priority = 1}.

cancel_timer(undefined) -> ok;
cancel_timer(Timer) -> erlang:cancel_timer(Timer).
