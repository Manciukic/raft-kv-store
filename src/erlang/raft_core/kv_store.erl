-module(kv_store).
-behaviour(gen_server).


-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2]).

% RAFT API
-export([commit_entry/1]).

% Client API
-export([push_entry/1]).


%%% RAFT API %%%
commit_entry(Entry) ->
    logger:notice("(commit_entry) ~p~n", [Entry]),
    gen_server:call(?MODULE, {commit_entry, Entry}, infinity).

%%% Client API %%%
push_entry(Entry) -> 
    logger:notice("(push_entry) ~p~n", [Entry]),
    gen_server:call(?MODULE, {push_entry, Entry}, infinity).

%%% Gen_server behaviour %%%
start_link() ->
    logger:notice("starting key-value store ~n", []),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    {ok, dict:new()}.


handle_call({commit_entry, Entry}, _From, State) ->
    NewState = applyEntry(Entry, State),
    {reply, ok, NewState};

handle_call({push_entry, Entry}, _From, State) ->
    raft_statem:add_entry(Entry),
    NewState = applyEntry(Entry, State),
    {reply, ok, NewState}.

handle_cast({commit_entry, Entry}, State) ->
    NewState = applyEntry(Entry, State),
    {noreply, NewState};

handle_cast({push_entry, Entry}, State) ->
    raft_statem:add_entry(Entry),
    NewState = applyEntry(Entry, State),
    {noreply, NewState}.        


%%% Utility functions %%%
applyEntry({add, Key, Value}, Dict) ->
    logger:notice("--- Add ~p:~p ---~n", [ Key, Value ]),
    dict:store(Key, Value, Dict);

applyEntry({delete, Key, Value}, Dict) ->
    logger:notice("--- Delete ~p:~p ---~n", [ Key, Value ]),
    dict:erase(Key, Dict);

applyEntry({update, Key, Value}, Dict) ->
    logger:notice("--- Update ~p:~p ---~n", [ Key, Value ]),
    dict:store(Key, Value, Dict).

