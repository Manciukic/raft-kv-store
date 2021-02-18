-module(kv_store).
-behaviour(gen_server).


-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2]).

% RAFT API
-export([commit_entry/3]).

% Client API
-export([get/1, get_all/0, set/2, delete/1, delete_all/0]).

-include("kv_store_config.hrl").

%%% RAFT API %%%
commit_entry(Index, Term, Entry) ->
    logger:notice("(commit_entry) ~p~n", [Entry]),
    try 
        gen_server:call(?MODULE, {sync, Entry, {Index, Term}}, ?COMMIT_ENTRY_TIMEOUT)
    catch 
        Error:Reason -> {error, {Error, Reason}} 
    end.

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
    { ok, { 0, dict:new(), dict:new()} }.

add_raft_entry(Node, { Operation, _ } = Action)
    when ( Operation == set ) or ( Operation == delete ) or ( Operation == delete_all ) ->
        raft_core:add_entry(Node, Action);

add_raft_entry(Node, _) -> 
    NodeSelf = node(self()),
    if 
        Node == NodeSelf -> % reply locally
            { ok, no_state_change, false };
        true ->
            { ok, no_state_change, ?STRONGLY_CONSISTENT }
    end.
            

handle_call({execute, Action}, From, {Index, Dict, PendingRequests} = State) ->
    Leader = raft_core:get_leader(),
    case Leader of
        null ->
            {reply, {error, leader_not_connected}, State};
        _ ->
            case add_raft_entry(Leader, Action) of
                { ok, no_state_change, false } -> % handle get locally
                    {Result, _} = handle_action(Action, Dict),
                    {reply, Result, State};
                { ok, no_state_change, true } -> 
                    % stronger consistency, have leader reply
                    Result = gen_server:call({?MODULE, Leader},
                                             {execute, Action}),
                    {reply, Result, State};
                { ok, RaftRef} ->
                    % enqueue request, waiting for its commit on local kv store
                    NewPendingRequests = dict:store(RaftRef, From,
                                                     PendingRequests),
                    {noreply, {Index, Dict, NewPendingRequests}};
                _ -> 
                    {reply, error, State}
            end
    end;

handle_call({sync, _, {NewIndex, _}}, _From, {Index, _, _} = State)
    when ( NewIndex =/= Index + 1 ) ->
        {reply, { error, Index }, State };

handle_call({sync, Action, {NewIndex, _}=RaftRef}, _From, 
            {_, Dict, PendingRequests}) ->
    {_, NewDict} = handle_action(Action, Dict),
    NewPendingRequests = reply_clients(RaftRef, PendingRequests),
    {reply, ok, {NewIndex, NewDict, NewPendingRequests}};


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

reply_clients(RaftRef, PendingRequests) ->
    case dict:find(RaftRef, PendingRequests) of
        {ok, From} ->
            gen_server:reply(From, ok),
            dict:erase(RaftRef, PendingRequests);
        error ->   
            PendingRequests
    end.
