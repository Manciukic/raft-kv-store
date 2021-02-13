
%% @private
-module(raft_kv_store_app).
-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% API.

start(_Type, _Args) ->
	raft_core:start().


stop(_State) ->
	true.
