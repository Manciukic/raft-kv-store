-module(kv_store).
-behaviour(gen_server).


-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2]).

% RAFT API
-export([commit_entry/1]).

% Client API
-export([get/1, get_all/0, set/2, delete/1, delete_all/0]).


%%% RAFT API %%%
commit_entry(Entry) ->
    logger:notice("(commit_entry) ~p~n", [Entry]),
    gen_server:call(?MODULE, {sync, Entry}, infinity).

%%% Client API %%%
get(Key) ->
    logger:notice("(get) ~p~n", [Key]),
    gen_server:call(?MODULE, {execute, { get, Key }}, infinity).

get_all() ->
    logger:notice("(get_all) ~n"),
    gen_server:call(?MODULE, {execute, { get_all }}, infinity).

set(Key, Value) ->
    logger:notice("(set) ~p:~p~n", [Key, Value]),
    gen_server:call(?MODULE, {execute, { set, Key, Value } }, infinity).

delete(Key) ->
    logger:notice("(delete) ~p~n", [Key]),
    gen_server:call(?MODULE, {execute, { delete, Key }}, infinity).

delete_all() ->
    logger:notice("(delete_all) ~n", []),
    gen_server:call(?MODULE, {execute, { delete_all }}, infinity).

%%% Gen_server behaviour %%%
start_link() ->
    logger:notice("starting key-value store ~n", []),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    {ok, dict:new()}.

handle_call({Type, Action}, _From, State) ->
    case Action of
        {get, Key} ->
            Result = handle_get(Key, State),
            {reply, Result, State};
        {get_all} ->
            Result = handle_get(State),
            {reply, Result, State};
        {set, Key, Value} ->
            if Type == execute ->
                raft_statem:add_entry(Action)
            end,
            NewState = handle_set(Key, Value, State),
            {reply, ok, NewState};
        {delete, Key} ->
            if Type == execute ->
                raft_statem:add_entry(Action)
            end,
            NewState = handle_delete(Key, State),
            {reply, ok, NewState};
        {delete_all} ->
            if Type == execute ->
                raft_statem:add_entry(Action)
            end,
            NewState = handle_delete(State),
            {reply, ok, NewState};
        _ -> {reply, error, State}
    end.

handle_cast(_, State) ->
    {noreply, State}.

%%% GET %%%
handle_get(Dict) ->
    logger:notice("--- Get all ---~n", [ ]),
    dict:to_list(Dict).

handle_get(Key, Dict) ->
    logger:notice("--- Get ~p ---~n", [ Key ]),
    Result = dict:find(Key, Dict),
    case Result of
        {ok, Value} -> Value;
        error -> error;
        _ -> error
    end.


%%% SET %%%
handle_set(Key, Value, Dict) ->
    logger:notice("--- Set ~p:~p ---~n", [ Key, Value ]),
    dict:store(Key, Value, Dict).


%%% DELETE %%%
handle_delete(Key, Dict) ->
    logger:notice("--- Delete ~p ---~n", [ Key ]),
    dict:erase(Key, Dict).

handle_delete(Dict) ->
    logger:notice("--- Delete all ---~n", [ ]),
    dict:new().
