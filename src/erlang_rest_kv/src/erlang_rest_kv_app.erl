%% Feel free to use, reuse and abuse the code in this file.

%% @private
-module(erlang_rest_kv_app).
-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% API.

start(_Type, _Args) ->
	Dispatch = cowboy_router:compile([
		{'_', [
			{"/", handler, []}
		]}
	]),
	{ok, _} = cowboy:start_clear(http, [{port, 8080}], #{
		env => #{dispatch => Dispatch}
	}),
	raft_core:start(),
	erlang_rest_kv_sup:start_link().


stop(_State) ->
	ok = cowboy:stop_listener(http).
