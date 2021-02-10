-module(kv_store).
-behaviour(gen_server).


-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2]).

% RAFT API
-export([commit_entry/2]).

% Client API
-export([get/1, get_all/0, set/2, delete/1, delete_all/0]).


%%% RAFT API %%%
commit_entry(Index, Entry) ->
    logger:notice("(commit_entry) ~p~n", [Entry]),
    gen_server:call(?MODULE, {sync, Entry, Index}, infinity).

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
    { ok, { 0, dict:new() } }.

add_raft_entry({ Operation, _ } = Action)
    when ( Operation == set ) or ( Operation == delete ) or ( Operation == delete_all ) ->
        raft_statem:add_entry(Action);

add_raft_entry(_) -> ok.

handle_call({execute, Action}, _From, State) ->
    Leader = raft_statem:get_leader(),
    Node = node(self()),
    case Leader of
        Node ->
            case add_raft_entry(Action) of
                error -> {reply, error, State};
                { ok, NewIndex} ->
                    {_, Dict} = State,
                    {Result, NewDict} = handle_action(Action, Dict),
                    {reply, Result, {NewIndex, NewDict}}
            end;
        _ ->
            Reply = gen_server:call({ kv_store, Leader}, Action),
            {reply, Reply, State}
    end;

handle_call({sync, _, NewIndex}, _From, {Index, _} = State)
    when ( NewIndex =/= Index + 1 ) ->
        {reply, { error, Index }, State };

handle_call({sync, Action, NewIndex}, _From, {_, Dict}) ->
        {_, NewDict} = handle_action(Action, Dict),
        {reply, ok, {NewIndex, NewDict}};


handle_call(Action, From, State) ->
    handle_call({execute, Action}, From, State).

handle_action({ get, { Key } }, Dict) ->
    logger:notice("--- Get ~p ---~n", [ Key ]),
    Result = dict:find(Key, Dict),
    ResultValue = case Result of
        {ok, Value} -> Value;
        error -> error;
        _ -> error
    end,
    {ResultValue, Dict};

handle_action({get_all, { } }, Dict) ->
    logger:notice("--- Get all ---~n", [ ]),
    Result = dict:to_list(Dict),
    {Result, Dict};

handle_action({set, { Key, Value } }, Dict) ->
    logger:notice("--- Set ~p:~p ---~n", [ Key, Value ]),
    NewDict = dict:store(Key, Value, Dict),
    {ok, NewDict};

handle_action({delete, { Key }}, Dict) ->
    logger:notice("--- Delete ~p ---~n", [ Key ]),
    NewDict = dict:erase(Key, Dict),
    {ok, NewDict};

handle_action({delete_all, { } }, _) ->
    logger:notice("--- Delete all ---~n", [ ]),
    NewDict = dict:new(),
    {ok, NewDict}.


handle_cast(_, State) ->
    {noreply, State}.