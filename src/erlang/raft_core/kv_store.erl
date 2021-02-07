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
    gen_server:call(?MODULE, {commit_entry, Entry}, infinity).

%%% Client API %%%
get(Key) ->
    logger:notice("(get) ~p~n", [Key]),
    gen_server:call(?MODULE, {get, Key}, infinity).

get_all() ->
    logger:notice("(get_all) ~n"),
    gen_server:call(?MODULE, {get_all}, infinity).

set(Key, Value) ->
    logger:notice("(set) ~p:~p~n", [Key, Value]),
    gen_server:call(?MODULE, { set, Key, Value }, infinity).

delete(Key) ->
    logger:notice("(delete) ~p~n", [Key]),
    gen_server:call(?MODULE, {delete, Key}, infinity).

delete_all() ->
    logger:notice("(delete_all) ~n", []),
    gen_server:call(?MODULE, {delete_all}, infinity).

%%% Gen_server behaviour %%%
start_link() ->
    logger:notice("starting key-value store ~n", []),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    {ok, dict:new()}.


handle_call({commit_entry, Entry}, _From, State) ->
    % NewState = set({set, Key, Value}, State),
    {reply, ok, State};

handle_call({get, Key}, _From, State) ->
    Result = handle_get(Key, State),
    {reply, Result, State};

handle_call({get_all}, _From, State) ->
    Result = handle_get(State),
    {reply, Result, State};

handle_call({set, Key, Value}, _From, State) ->
    %raft_statem:add_entry({set, Key, Value}),
    NewState = handle_set(Key, Value, State),
    {reply, ok, NewState};

handle_call({delete, Key}, _From, State) ->
    NewState = handle_delete(Key, State),
    {reply, ok, NewState};

handle_call({delete_all}, _From, State) ->
    NewState = handle_delete(State),
    {reply, ok, NewState}.
    
handle_cast(Request, State) ->
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
