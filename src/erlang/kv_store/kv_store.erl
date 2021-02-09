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
    gen_server:call(?MODULE, {execute, { get, { Key } }}, infinity).

get_all() ->
    logger:notice("(get_all) ~n"),
    gen_server:call(?MODULE, {execute, { get_all, { } }}, infinity).

set(Key, Value) ->
    logger:notice("(set) ~p:~p~n", [Key, Value]),
    gen_server:call(?MODULE, {execute, { set, { Key, Value } } }, infinity).

delete(Key) ->
    logger:notice("(delete) ~p~n", [Key]),
    gen_server:call(?MODULE, {execute, { delete, { Key } }}, infinity).

delete_all() ->
    logger:notice("(delete_all) ~n", []),
    gen_server:call(?MODULE, {execute, { delete_all, { } }}, infinity).

%%% Gen_server behaviour %%%
start_link() ->
    logger:notice("starting key-value store ~n", []),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    {ok, dict:new()}.

handle_call({execute, { Operation, Parameters } = Action}, From, State)
    when ( Operation == set ) or ( Operation == delete ) or ( Operation == delete_all ) ->
        %Node = raft_statem:get_leader(),
        %gen_server:call({ kv_store, Node }, Action)
        %raft_statem:add_entry(Node, Action),
        handle_action(Action, From, State);

handle_call({execute, Action}, From, State) ->
    handle_action(Action, From, State);

handle_call({sync, Action}, From, State) ->
    handle_action(Action, From, State);

handle_call(Action, From, State) ->
    handle_call({execute, Action}, From, State).

handle_action({ get, { Key } }, _From, Dict) ->
    logger:notice("--- Get ~p ---~n", [ Key ]),
    Result = dict:find(Key, Dict),
    ResultValue = case Result of
        {ok, Value} -> Value;
        error -> error;
        _ -> error
    end,
    {reply, ResultValue, Dict};

handle_action({get_all, { } }, _From, Dict) ->
    logger:notice("--- Get all ---~n", [ ]),
    Result = dict:to_list(Dict),
    {reply, Result, Dict};

handle_action({set, { Key, Value } }, _From, Dict) ->
    logger:notice("--- Set ~p:~p ---~n", [ Key, Value ]),
    NewState = dict:store(Key, Value, Dict),
    {reply, ok, NewState};

handle_action({delete, { Key }}, _From, Dict) ->
    logger:notice("--- Delete ~p ---~n", [ Key ]),
    NewState = dict:erase(Key, Dict),
    {reply, ok, NewState};

handle_action({delete_all, { } }, _From, Dict) ->
    logger:notice("--- Delete all ---~n", [ ]),
    NewState = dict:new(),
    {reply, ok, NewState}.


handle_cast(_, State) ->
    {noreply, State}.