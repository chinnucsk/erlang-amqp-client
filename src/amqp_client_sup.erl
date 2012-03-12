%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_client_sup).

-include("amqp_client.hrl").

-behaviour(supervisor2).

-export([start_link/0]).

-export([init/1]).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%---------------------------------------------------------------------------
%% supervisor2 callbacks
%%---------------------------------------------------------------------------

init([]) ->
	ReconnPolicy = 
	case application:get_env(reconn_policy) of
	{ok, Policy} -> Policy;
	undefined -> [{interval, 30}]
	end,
	Amqp = {amqp, {amqp, start_link, [ReconnPolicy]}, 
		permanent, 5000, worker, [amqp]},
	Sup = {amqp_sup, {amqp_sup, start_link, []},
        permanent, infinity, supervisor, [amqp_sup]},
    {ok, {{one_for_all, 10, 100}, [Sup, Amqp]}}.


