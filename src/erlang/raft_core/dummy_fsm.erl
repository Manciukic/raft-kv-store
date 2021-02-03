-module(dummy_fsm).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3]).

% RAFT API
-export([commit_entry/1]).

% Client API
-export([push_entry/1]).


start_link() ->
    logger:notice("starting dummy FSM~n", []),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    {ok, true}.

commit_entry(Entry) -> 
    logger:notice("(commit_entry) ~p~n", [Entry]),
    gen_server:call(?MODULE, {commit_entry, Entry}, infinity).

push_entry(Entry) -> 
    logger:notice("(push_entry) ~p~n", [Entry]),
    gen_server:call(?MODULE, {push_entry, Entry}, infinity).


handle_call({commit_entry, Entry}, _From, _State) ->
    apply(Entry),
    {reply, ok, _State};

handle_call({push_entry, Entry}, _From, _State) ->
    raft_statem:add_entry(Entry),
    apply(Entry),
    {reply, ok, _State}.

apply(Entry) ->
    logger:notice("--- ~p ---~n", [Entry]).
