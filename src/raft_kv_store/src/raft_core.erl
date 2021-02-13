%%%-------------------------------------------------------------------
%%% @doc
%%% Core module for the implementation of the RAFT FSM.
%%% This module allows the replication of a FSM among different Erlang
%%% nodes. The FSM to be replicated should be a gen_server module 
%%% indicated by FSM_MODULE macro in the configuration header.
%%% @end
%%%-------------------------------------------------------------------

-module(raft_core).

-behaviour(gen_statem).

% gen_statem callbacks
-export([
         init/1,
         %format_status/2,
         %handle_event/4,
         terminate/3,
         %code_change/4,
         callback_mode/0
        ]).

% State functions
-export([
         follower/3,
         candidate/3,
         leader/3
        ]).

% External/client API
-export([
         start/0,
         get_leader/0,
         add_entry/1,
         add_entry/2,
         stop/0
        ]).

-include("raft_config.hrl").

-define(SERVER, ?MODULE).

% data held between states
% TODO: use different records for each state to save memory ?
-record(state, {
    % Persistent state on all servers
    % (Updated on stable storage before responding to RPCs)
    % TODO persist this data :D

    % latest term server has seen 
    % (initialized to 0 on first boot, increases monotonically)
    current_term, 

    % candidate_id that received vote in current term or null if none)
    voted_for, 

    % log entries; each entry contains command for state machine, and term 
    % when entry was received by leader (first index is 1)
    log, 

    % Volatile state on all server

    % index of highest log entry known to be committed (initialized to 0, 
    % increases monotonically)
    commit_index, 

    % index of highest log entry applied to state machine 
    % (initialized to 0, increases monotonically)
    last_applied, 

    % current known leader node
    leader, 

    % list of all other nodes
    % (initialized at the beginning from configuration, removing local node)
    nodes,

    % Volatile state on leaders 
    % (reinitialized after election)

    % for each server, index of the next log entry to send to that server
    % (initialized to leader last log index + 1)
    % This is a map (key is node name, value is next_index)
    next_index, 

    % for each server, index of highest log entry known to be replicated 
    % (initialized to 0, increases monotonically)
    % This is a map (key is node name, value is next_index)
    match_index, 

    % Volatile state on candidates 
    % (reinitialized at new election)

    % set of nodes that have voted for this node
    votes_for
}).

% structure of a log entry
-record(log_entry, {
    % the term this entry has been added
    term, 

    % the content of the log entry, containing the command to the FSM
    content
}).

%% request_vote RPC
%% Invoked by candidates to gather votes

% arguments of request_vote RPC
-record(request_vote_req, {
    % candidate's term
    term, 

    % candidate requesting vote, aka node(self())
    candidate_id, 

    % index of candidate's last log entry
    last_log_index, 

    % term of candidate's last log entry
    last_log_term
}).

% results of request_vote RPC
-record(request_vote_rpy, {
    % current_term, for candidate to update itself
    term, 

    % true means candidate received vote
    vote_granted
}).

%% append_entries RPC
%% Invoked by leader to replicate log entries; also used as heartbeat

% arguments of append_entries RPC
-record(append_entries_req, {
    % leader's term
    term, 

    % so follower can redirect clients
    leader_id, 

    % index of log entry immediately preceding new ones
    prev_log_index, 

    % term of prev_log_index entry
    prev_log_term, 

    % list of log entrues to store
    % (empty for heartbeat; may send more than one for efficiency)
    entries, 

    % leader's commit_index
    leader_commit
}).

% results of append_entries RPC
-record(append_entries_rpy, {
    % current_term, for leader to update itself
    term, 

    % true if follower contained entry matching prev_log_index 
    % and prev_log_term
    success, 

    % prev_log_index, so leaders can match replies with requests
    match_index
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_statem process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    logger:notice("starting RAFT~n", []),
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------

% Start both the underlying FSM and this module. 
start() ->
    ?FSM_MODULE:start_link(),
    start_link().

%%--------------------------------------------------------------------

% append an entry to the log and replicate it among all nodes
% this RPC returns immediately after leader starts replication
% and returns the index and term of this entry. The client will
% receive notification of the commit of the entry with the commit_entry
% RPC.
add_entry(Entry) ->
    gen_statem:call(?SERVER, {add_entry, Entry}).

add_entry(Node, Entry) ->
    gen_statem:call({?SERVER, Node}, {add_entry, Entry}).

%%--------------------------------------------------------------------

% return the node which currently holds the leader process
get_leader() ->
    gen_statem:call(?SERVER, get_leader).

%%--------------------------------------------------------------------

% stop this process and the FSM process
stop() ->
    gen_statem:stop(?SERVER),
    ?FSM_MODULE:stop().

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_statem is started using gen_statem:start/[3,4] or
%% gen_statem:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {CallbackMode, StateName, State} |
%%                     {CallbackMode, StateName, State, Actions} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    % TODO load persisted data from disk

    {ok, follower, #state{
        current_term=0, 
        voted_for=null, 
        log=[], 
        commit_index=0, 
        last_applied=0, 
        next_index=maps:new(),
        match_index=maps:new(),
        leader=null,
        nodes=lists:delete(node(self()), ?NODES)
    },
    [{state_timeout, rand_election_timeout(), electionTimeout}]}.

%%--------------------------------------------------------------------

%% Return the callback mode.
callback_mode() ->
    % Define a separate handler function for each state.
    state_functions.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name.  If callback_mode is statefunctions, one of these
%% functions is called when gen_statem receives and event from
%% call/2, cast/2, or as a normal process message.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Actions} |
%%                   {stop, Reason, NewState} |
%%    				 stop |
%%                   {stop, Reason :: term()} |
%%                   {stop, Reason :: term(), NewData :: data()} |
%%                   {stop_and_reply, Reason, Replies} |
%%                   {stop_and_reply, Reason, Replies, NewState} |
%%                   {keep_state, NewData :: data()} |
%%                   {keep_state, NewState, Actions} |
%%                   keep_state_and_data |
%%                   {keep_state_and_data, Actions}
%% @end
%%--------------------------------------------------------------------

%~ state_name(_EventType, _EventContent, State) ->
%~     NextStateName = next_state,
%~     {next_state, NextStateName, State}.

%%--------------------------------------------------------------------
%% follower
%% Followers only respond to requests from other servers. If a follower
%% receives no communication, it becomes a candidate and initiates an 
%% election.

% follower received no communication within election timeout, thus it 
% becomes a candidate and starts a new election.
follower(state_timeout, electionTimeout, #state{}=State) ->
    logger:debug("follower: electionTimeout~n", []),
    {next_state, candidate, State,
        % election is started immediately after becoming candidate (timeout=0)
        [{state_timeout, 0, electionTimeout}]
    };

%% apend_entries RPC
%% check validity and add the new entries from the log and reply to the leader

% reply false if term < current_term
follower(cast, {append_entries, From, #append_entries_req{
    term=Term
}}, #state{
    current_term=CurrentTerm
} = State) 
when Term < CurrentTerm ->
    logger:debug("follower: append_entries: old term~n", []),
    reply_append_entries(From, CurrentTerm, false, 0),
    % reset the election timeout
    {keep_state, State, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

% reply false if log doesn't contain an entry at prev_log_index
% i.e. local log is out-of-sync, signal it to leader
follower(cast, {append_entries, From, #append_entries_req{
    term=Term,
    prev_log_index=PrevLogIndex
}}, #state{
    log=Log
} = State)
when PrevLogIndex > length(Log) -> 
    logger:debug("follower: append_entries: prev_log_index not in log (~p)~n", 
                [PrevLogIndex]),
    reply_append_entries(From, Term, false, length(Log)),
    % reset the election timeout
    {keep_state, State#state{current_term=Term}, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

% reply false if log doesn't contain an entry at prev_log_index whose
% term matches prev_log_term, otherwise append the entry
follower(cast, {append_entries, From, #append_entries_req{
    term=Term,
    prev_log_index=PrevLogIndex, 
    prev_log_term=PrevLogTerm
}=Req}, #state{
    log=Log
} = State) ->
    NthLogTerm = nth_log_entry_term(PrevLogIndex, Log),
    NewState = if
        PrevLogTerm =/= NthLogTerm -> 
            % entry is incompatible with local log, deny it
            logger:debug("follower: append_entries: wrong nth log term~n", []),
            reply_append_entries(From, Term, false, PrevLogIndex),
            State#state{current_term=Term};
        true -> % ok
            logger:debug("follower: append_entries: ok~n", []),
            NewState2 = do_append_entries(Req, State),
            #state{log=NewLog} = NewState2,
            reply_append_entries(From, Term, true, length(NewLog)),
            NewState2
    end,
    % reset the election timeout
    {keep_state, NewState, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

%% request_vote RPC
%% check if candidate log is updated and reply to its vote request

% reply false if term < current_term
follower(cast, {request_vote, From, #request_vote_req{
    term = Term
}}, #state{
    current_term = CurrentTerm
} = State)
when Term < CurrentTerm -> 
    logger:debug("follower: request_vote: wrong term~n", []),
    reply_request_vote(From, CurrentTerm, false),
    % reset election timeout
    {keep_state, State,
        [{state_timeout, rand_election_timeout(), electionTimeout}]};
      
% if voted_for is null or candidate_id, and candidate's log is at least 
% as up-to-date as receiver's log, grant vote
follower(cast, {request_vote, From, #request_vote_req{
    term = Term, 
    candidate_id = CandidateId, 
    last_log_index = LastLogIndex, 
    last_log_term = LastLogTerm
}}, #state{
    current_term = CurrentTerm,
    voted_for = VotedFor,
    log = Log
} = State)
when (VotedFor == null) or (VotedFor == CandidateId) -> 
    NewState = case log_up_to_date(Log, LastLogIndex, LastLogTerm) of
        true ->
            logger:debug("follower: request_vote: voting for ~p~n", 
                        [CandidateId]),
            reply_request_vote(From, CurrentTerm, true),
            % remember who I voted for
            State#state{voted_for=CandidateId};
        false -> % leader log is out-dated, deny vote
            logger:debug("follower: request_vote: outdated log~n", []),
            reply_request_vote(From, CurrentTerm, false),
            State
    end,
    % update current term and reset election timeout
    {keep_state, NewState#state{current_term=max(CurrentTerm,Term)},
        [{state_timeout, rand_election_timeout(), electionTimeout}]};
  
% already voted, deny vote
follower(cast, {request_vote, From, _Req}, #state{
    current_term = CurrentTerm
} = State) -> 
    logger:debug("follower: request_vote: voted already~n", []),
    reply_request_vote(From, CurrentTerm, false),
    % reset election timeout
    {keep_state, State,
        [{state_timeout, rand_election_timeout(), electionTimeout}]};


% for any other event, use the common handler
follower(EventType, EventContent, Data) ->
    handle_common(follower, EventType, EventContent, Data).

%%--------------------------------------------------------------------
%% candidate
%% A candidate asks other nodes to vote for him and, if it receives a 
%% majority of the votes, it becomes the new leader.

% start a new election (either electionTimeout on follower or not enough 
% replies within time)
candidate(state_timeout, electionTimeout, #state{
    current_term=CurrentTerm,
    log=Log,
    nodes=Nodes
}=State) ->
    logger:info("candidate: starting election~n", []),
    NewState = State#state{
        current_term=CurrentTerm+1, % TODO check
        voted_for=node(self()),
        votes_for=sets:from_list([node(self())])
    },
    {LastLogIndex, LastLogTerm} = last_log_index_term(Log),
    % request votes from all other nodes
    request_votes(Nodes, CurrentTerm+1, node(self()), 
                  LastLogIndex, LastLogTerm),
    % start a new election timeout
    {keep_state, NewState, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};


%% request_vote RPC reply

% term is higher, convert to follower
candidate(cast, {request_vote_rpy, _From, #request_vote_rpy{
    term=Term
}}, #state{
    current_term=CurrentTerm
}=State) when Term > CurrentTerm->
    logger:debug("candidate: request_vote_rpy: term is higher~n"),
    {next_state, follower, State#state{current_term=Term},
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

% vote is in favour. If votes received from majority of servers, then
% become leader
candidate(cast, {request_vote_rpy, From, #request_vote_rpy{
    vote_granted=VoteGranted
}}, #state{
    votes_for=VotesFor,
    log=Log,
    nodes=Nodes
}=State) 
when VoteGranted ->
    NewVotesFor = sets:add_element(node(From), VotesFor),
    Quorum = quorum(),
    NVotes = sets:size(NewVotesFor),
    if 
        NVotes >= Quorum -> % reached quorum, become leader
            logger:debug("candidate: request_vote_rpy: reached quorum (~p)~n", 
                        [NVotes]),
            logger:info("elected as leader~n", []),
            {LastLogIndex, _LastLogTerm} = last_log_index_term(Log),
            {next_state, leader, State#state{
                    leader=node(self()),
                    next_index=maps:from_list([
                        {Node, LastLogIndex+1}
                        || Node <- Nodes
                    ]),
                    match_index=maps:from_list([
                        {Node, 0}
                        || Node <- Nodes
                    ])
                }, 
                % send heartbeat to notify election end
                [{state_timeout, 0, heartBeatTimeout}]
            };
        true -> % not enough votes, update state and wait
            logger:debug("candidate: request_vote_rpy: not enough votes: ~p~n", 
                [NVotes]),
            {keep_state, State#state{votes_for=NewVotesFor}}
    end;

% vote is against, ignore
candidate(cast, {request_vote_rpy, From, _Req}, _State) ->
    logger:debug("candidate: request_vote_rpy: vote rejected from ~p~n", 
                [node(From)]),
    keep_state_and_data;


%% request_vote RPC
%% receive a request to vote from another candidate

% his term is higher, revert to follower and vote for him
candidate(cast, {request_vote, _From, 
        #request_vote_req{term=Term}}, 
    #state{current_term=CurrentTerm}=State) 
when Term > CurrentTerm -> 
    logger:debug("candidate: request_vote: term is higher~n", []),
    {next_state, follower, State#state{
        current_term=Term,
        voted_for=null
    }, 
    % request_vote handled in follower state
    [postpone]};

% otherwise deny the vote
candidate(cast, {request_vote, From, _Req}, #state{current_term=CurrentTerm}) ->
    logger:debug("candidate: request_vote: refused~n", []),
    reply_request_vote(From, CurrentTerm, false),
    keep_state_and_data;


%% append_entries RPC

% received append_entries from new leader, revert to follower and handle it
candidate(cast, {append_entries, _From, _Req}, State) ->
    %TODO check correctness. should I check the term?
    logger:debug("candidate: append_entries: backing to follower~n", []),
    {next_state, follower, State, [postpone]};

% for any other event, use the common handler
candidate(EventType, EventContent, Data) ->
    handle_common(candidate, EventType, EventContent, Data).

%%--------------------------------------------------------------------
%% leader
%% A leader receives new entries from clients and commits them to the 
%% replicated log. Leaders operate until they fail.

% send a periodic heartbeat
leader(state_timeout, heartBeatTimeout, State) ->
    logger:debug("leader: heartBeatTimeout~n", []),
    send_heartbeat(State),
    % start a new heartbeat timeout
    {keep_state_and_data, [
        {state_timeout, ?HEART_BEAT_TIMEOUT, heartBeatTimeout}
    ]};


%% append_entries RPC reply
%% client sent a reply to append entries

% reply refers to an older term, ignore
leader(cast, {append_entries_rpy, _From, 
        #append_entries_rpy{term=Term}
    }, #state{current_term=CurrentTerm}) 
when Term < CurrentTerm ->
    logger:debug("leader: append_entries_rpy: old term~n", []),
    keep_state_and_data;

% reply is from a newer term, revert to follower
% === this should NEVER happen ===
leader(cast, {append_entries_rpy, _From, 
        #append_entries_rpy{term=Term}
    }, #state{current_term=CurrentTerm}=State) 
when Term > CurrentTerm -> 
    logger:error("leader: append_entries_rpy: newer term, backing off ~n", []),
    {next_state, follower, State#state{
        current_term=Term,
        voted_for=null
    }, [postpone]};

% append_entries was successful, update internal view of follower
leader(cast, {append_entries_rpy, From, #append_entries_rpy{
    success=Success,
    match_index=MatchIndex}
}, #state{
    current_term=CurrentTerm,
    next_index=NextIndexMap,
    match_index=MatchIndexMap,
    commit_index=CommitIndex,
    log=Log,
    last_applied=LastApplied
}=State)
when Success ->  
    Node = node(From),
    logger:debug("leader: append_entries_rpy: success~n", []),
    NewNextIndexMap = maps:put(Node, MatchIndex+1, NextIndexMap),
    NewMatchIndexMap = maps:put(Node, MatchIndex, MatchIndexMap),
    % check whether new entries can be commited
    NewCommitIndex = update_commit_index(CommitIndex, NewMatchIndexMap, 
                                        Log, CurrentTerm),
    % if so, do those commits
    NewLastApplied = do_commit_entries(LastApplied+1, 
                                            NewCommitIndex, Log),
    {keep_state, State#state{
        next_index=NewNextIndexMap,
        match_index=NewMatchIndexMap,
        commit_index=NewCommitIndex,
        last_applied=NewLastApplied
    }};

% append_entries failed, decrease next_index if possible and retry 
leader(cast, {append_entries_rpy, From, _Req}, 
    #state{next_index=NextIndexMap}=State
) ->
    Node = node(From),
    OldNextIndex = maps:get(Node, NextIndexMap),
    if 
        OldNextIndex == 0 -> 
            % cannot decrease next_index further, it means that the 
            % follower is misbehaving, ignore it
            logger:debug("leader: append_entries_rpy: next_index already 0~n", 
                        []),
            {keep_state, State};
        true -> % decrease next_index and retry
            NewNextIndexMap = maps:put(Node, OldNextIndex-1, 
                                        NextIndexMap),
            NewState = State#state{
                next_index=NewNextIndexMap
            },
            send_append_entries(node(From), NewState),
            {keep_state, NewState}
    end;


%% add_entry RPC (client)

% add the new entry to the log and send append entries to all other nodes
leader({call, From}, {add_entry, Entry}, #state{
    log=Log,
    current_term=CurrentTerm
}=State) ->
    logger:debug("leader: add_entry~n", []),
    NewState=persist_entries([Entry], State),
    send_all_append_entries(NewState),
    gen_statem:reply(From, {ok, {length(Log)+1, CurrentTerm}}),
    {keep_state, NewState, [
        % reset heartbeat timeout
        {state_timeout, ?HEART_BEAT_TIMEOUT, heartBeatTimeout}
    ]};


% for any other event, use the common handler
leader(EventType, EventContent, Data) ->
    handle_common(leader, EventType, EventContent, Data).

%%--------------------------------------------------------------------
%% common handler
%% Events not handled directly in state functions are handled here.

% return the current known leader
handle_common(State, {call, From}, get_leader, #state{leader=Leader}) ->
    logger:debug("~p: get_leader: leader is ~p~n", [State]),
    gen_statem:reply(From, Leader),
    keep_state_and_data;

% reply that this node is not the leader
handle_common(State, {call, From}, {add_entry, _}, _State) ->
    logger:debug("~p: add_entry: not a leader~n", [State]),
    gen_statem:reply(From, false),
    keep_state_and_data;

% any other cast action must be ignored
handle_common(State, cast, {Operation, _From, _Data}, _State) ->
    logger:debug("~p: ~p: not allowed in this state~n", [State, Operation]),
    keep_state_and_data;

% any other event must be ignored
handle_common(State, EventType, _EventContent, _Data) ->
    logger:debug("~p: event not handled: ~p~n", [State, EventType]),
    keep_state_and_data.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    logger:notice("(terminate) stopping~n", []),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% Communication functions
%%--------------------------------------------------------------------

% request votes from all other nodes
request_votes(Nodes, Term, CandidateId, LastLogIndex, LastLogTerm) ->
    logger:debug("(request_votes) term=~p, candidate=~p, last_log_index=~p, "
                 "last_log_term=~p~n", 
                 [Term, CandidateId, LastLogIndex, LastLogTerm]),
    send_all(Nodes, {request_vote, self(), #request_vote_req{
            term=Term, 
            candidate_id=CandidateId, 
            last_log_index=LastLogIndex, 
            last_log_term=LastLogTerm
        }}).

%%--------------------------------------------------------------------

% send heartbeat to all other nodes
send_heartbeat(#state{
    current_term=CurrentTerm,
    commit_index=CommitIndex,
    log=Log,
    nodes=Nodes
}) -> 
    {LastLogIndex, LastLogTerm} = last_log_index_term(Log),
    logger:debug("(send_heartbeat) term=~p, commit_index=~p, "
                 "last_log_index=~p, last_log_term=~p~n", 
                 [CurrentTerm, CommitIndex, LastLogIndex, LastLogTerm]),
    send_all(Nodes, {append_entries, self(), #append_entries_req{
        term=CurrentTerm, 
        leader_id=node(self()), 
        prev_log_index=LastLogIndex, 
        prev_log_term=LastLogTerm, 
        entries=[], 
        leader_commit=CommitIndex
    }}).

%%--------------------------------------------------------------------

% send given request to specified nodes
send_all([], _Req) ->
    true;
send_all([HNode | TNodes], Req) ->
    gen_statem:cast({?SERVER, HNode}, Req),
    send_all(TNodes, Req).

%%--------------------------------------------------------------------

% send append entries to all other nodes
send_all_append_entries(#state{nodes=Nodes}=State) ->
    send_all_append_entries(Nodes, State).

% send append entries to all other nodes (internal)
send_all_append_entries([], _State) ->
    true;
send_all_append_entries([HNode | TNodes], State) ->
    send_append_entries(HNode, State),
    send_all_append_entries(TNodes, State).

%%--------------------------------------------------------------------

% Build node-specific append_entries message and send it.
% The message should contain all log entries that the leader believes
% the follower not to know about (through the next_index map).
send_append_entries(Node, #state{
    next_index=NextIndexMap,
    commit_index=CommitIndex,
    current_term=CurrentTerm,
    log=Log
}) ->
    NextIndex = maps:get(Node, NextIndexMap),
    PrevLogIndex = max(0, NextIndex-1),
    PrevLogTerm = nth_log_entry_term(PrevLogIndex, Log),
    logger:debug("(send_append_entries) node=~p, term=~p, prev_log_index=~p,"
                 " prev_log_term=~p leader_commit=~p~n", 
                 [Node, CurrentTerm, PrevLogIndex, PrevLogTerm, CommitIndex]),

    gen_statem:cast({?SERVER, Node}, {append_entries, self(), 
        #append_entries_req{
            term=CurrentTerm, 
            leader_id=node(self()), 
            prev_log_index=PrevLogIndex, 
            prev_log_term=PrevLogTerm, 
            entries=log_sublist(Log, NextIndex), 
            leader_commit=CommitIndex
        }
    }).

%%--------------------------------------------------------------------

% reply to an append entries request
reply_append_entries(To, Term, Success, MatchIndex) ->
    logger:debug("(reply_append_entries) node=~p, term=~p, success=~p,"
                 " index=~p~n", 
                 [To, Term, Success, MatchIndex]),
    gen_statem:cast({?SERVER, node(To)}, {append_entries_rpy, self(), 
        #append_entries_rpy{
            term=Term,
            success=Success,
            match_index=MatchIndex
        }
    }).
    
%%--------------------------------------------------------------------

% reply to a request_vote request
reply_request_vote(To, Term, VoteGranted) ->
    logger:debug("(reply_request_vote) node=~p, term=~p, vote_granted=~p~n",
                [To, Term, VoteGranted]),
    gen_statem:cast({?SERVER, node(To)}, {request_vote_rpy, self(), 
                    #request_vote_rpy{
                        term=Term,
                        vote_granted=VoteGranted
                    }
                }
    ).

%%--------------------------------------------------------------------
%% request_vote-related functions
%%--------------------------------------------------------------------

% check whether other log (indicated by LastLogIndex and LastLogTerm) is 
% as up-to-date as the local log
log_up_to_date(Log, LastLogIndex, LastLogTerm) ->
    {MyLastLogIndex, MyLastLogTerm} = last_log_index_term(Log),
    log_up_to_date(MyLastLogIndex, MyLastLogTerm, LastLogIndex, LastLogTerm).


% empty log, anything would be as up to date as it
log_up_to_date(MyLastLogIndex, _MyLastLogTerm, _LastLogIndex, _LastLogTerm)
when MyLastLogIndex =< 0 ->
    true;

% term is higher, then it is more up-to-date
log_up_to_date(_MyLastLogIndex, MyLastLogTerm, _LastLogIndex, LastLogTerm)
when LastLogTerm > MyLastLogTerm ->
    true;

% term is same but index is higher, then it is more up-to-date
log_up_to_date(MyLastLogIndex, MyLastLogTerm, LastLogIndex, LastLogTerm)
when (LastLogTerm == MyLastLogTerm) and (LastLogIndex >= MyLastLogIndex) ->
    true;

% otherwise it is out-dated
log_up_to_date(_MyLastLogIndex, _MyLastLogTerm, _LastLogIndex, _LastLogTerm) ->
    false.

%%--------------------------------------------------------------------
%% append_entries-related functions (appending and committing)
%%--------------------------------------------------------------------

% add the new entries to the log and commit any uncommitted entries
% - If an existing entry conflicts with a new one (same index but different
%   terms), delete the existing entry and all that follow it.
% - Append any new entries not already in the log
% - If leader_commit > commit_index, set 
%   commit_index = min(leader_commit, index of last new entry)
do_append_entries(#append_entries_req{
    term=Term,
    leader_id=Leader, 
    prev_log_index=PrevLogIndex,  
    entries=Entries, 
    leader_commit=LeaderCommit
}, #state{
    commit_index=CommitIndex,
    log=Log,
    last_applied=LastApplied
}=State) ->
    case Entries of
        [] ->
            pass;
        _ -> logger:info("(do_append_entries) prev_log_index=~p, "
                         "size(entries)=~p~n", 
                         [PrevLogIndex, length(Entries)])
    end,
    % build new log, overwriting any conflicting entry
    NewLog = lists:sublist(Log, PrevLogIndex+1) ++ Entries,
    % update commit index
    NewCommitIndex = if 
        LeaderCommit > CommitIndex ->
            min(LeaderCommit, length(NewLog));
        true ->
            CommitIndex
    end,
    logger:debug("(do_append_entries) committing entries from ~p to ~p~n",
        [LastApplied+1, NewCommitIndex]),
    logger:debug("(do_append_entries) log=~p~n", [NewLog]),
    NewLastApplied = do_commit_entries(LastApplied+1, NewCommitIndex, 
                        NewLog),
    State#state{
        leader=Leader,
        commit_index=NewCommitIndex,
        log=NewLog,
        current_term=Term,
        last_applied=NewLastApplied
    }.

%%--------------------------------------------------------------------

% leader-only: find the commit index given the status of the followers.
% An entry can be committed iff replicated on majority of nodes and is 
% related to current term (cannot commit entries from other leaders 
% directly).

% if log is empty, do nothing
update_commit_index(OldCommitIndex, _MatchIndexMap, [], _CurrentTerm) ->
    logger:debug("(update_commit_index) empty log~n", []),
    OldCommitIndex;

update_commit_index(OldCommitIndex, MatchIndexMap, Log, CurrentTerm) ->
    NextCommitIndex = find_next_commit_index(OldCommitIndex+1, MatchIndexMap),
    NthTerm = nth_log_entry_term(NextCommitIndex, Log),
    if 
        (NextCommitIndex > OldCommitIndex) and (NthTerm == CurrentTerm) ->
            % ok
            logger:debug("(update_commit_index) new commit index is ~p~n", 
                        [NextCommitIndex]),
            NextCommitIndex;
        true -> % entry was from another leader, ignore
            logger:debug("(update_commit_index) commit index is ~p~n", 
                        [OldCommitIndex]),
            OldCommitIndex
    end.

%%--------------------------------------------------------------------

% tries TryIndex: if ok, tries next one, otherwise stops and returns 
% the last working index (previous one).
find_next_commit_index(TryIndex, MatchIndexMap) ->
    Quorum = quorum(),
    % count how many nodes are known to have the TryIndex-th entry
    NGreaterThan = length(
        lists:filter(
            fun(X) -> X >= TryIndex end, 
            maps:values(MatchIndexMap)
        )
    ),
    
    if 
        NGreaterThan+1 >= Quorum -> % count this node too
            % if index is ok, try next one
            find_next_commit_index(TryIndex+1, MatchIndexMap);
        true ->
            % otherwise return the last correct one
            TryIndex-1
    end.
        
%%--------------------------------------------------------------------

% Commits all entries from From to To (included) in the Log.
% If FSM is waiting for a reply, reply instead of issuing a 
% commit on the FSM (FSM will commit as a consequence of the reply).
% This function can be called in either leader or follower states.

% in case indices are invalid, do nothing
do_commit_entries(From, To, _Log) when From > To ->
    To;

do_commit_entries(From, To, Log) ->
    % send replies if FSM is waiting
    Result = commit_entry(From, nth_log_entry(From, Log)),
    case Result of 
        ok ->
    % commit next index
            do_commit_entries(From+1, To, Log);
        {error, FsmIndex} ->
            if 
                FsmIndex =< To -> % FSM is either more updated or outdated
                    % send all missing entries
                    do_commit_entries(FsmIndex+1, To, Log);
                true -> 
                    % FSM is more updated than RAFT, wtf?
                    logger:warning("(do_commit_entries) FSM more updated than"
                                    " RAFT", []),
                    From-1
            end
    end.

%%--------------------------------------------------------------------

% send a commit_entry RPC to FSM
commit_entry(Index, #log_entry{term=Term, content=Entry}) ->
    logger:info("(commit_entry) ~p (~p)~n", [Index, Entry]),
    ?FSM_MODULE:commit_entry(Index, Term, Entry).

%%--------------------------------------------------------------------
%% Log-related functions
%%--------------------------------------------------------------------

% Save new entries in log and persist on disk
% TODO: this is only done in leader, not in follower!
persist_entries(Entries, #state{
    log=Log,
    current_term=CurrentTerm
}=State) ->
    NewLogEntries = [#log_entry{
        term=CurrentTerm,
        content=Entry
    } || Entry <- Entries],
    %TODO persist newly added entries
    logger:debug("(persist_entries) adding ~p entries~n", [length(Entries)]),
    State#state{log=Log ++ NewLogEntries}.

%%--------------------------------------------------------------------

% return the index and term of the last log entry, 0 if none
last_log_index_term([]) ->
    {0,0};

last_log_index_term(Log) ->
    #log_entry{term=MyLastLogTerm} = lists:last(Log),
    {length(Log), MyLastLogTerm}.

%%--------------------------------------------------------------------

% return the nth log entry, if any or null
nth_log_entry(_N, []) ->
    null;

nth_log_entry(0, _Log) ->
    null;

nth_log_entry(N, Log) ->
    if 
        N > length(Log) ->
            null;
        true ->
            lists:nth(N, Log)
    end.

%%--------------------------------------------------------------------

% return the term of the nth log entry, if any or 0.
nth_log_entry_term(N, Log) ->
    extract_term(nth_log_entry(N, Log)).

%%--------------------------------------------------------------------

% extract term from entry. If null, return 0.
extract_term(null) ->
    0;
extract_term(#log_entry{term=Term}) ->
    Term.

%%--------------------------------------------------------------------

% return all entries in Log from From.
log_sublist([], _From) ->
    [];

log_sublist(Log, 0) ->
    Log;

log_sublist(Log, From) ->
    lists:sublist(Log, From, length(Log)).

%%--------------------------------------------------------------------
%% Misc functions
%%--------------------------------------------------------------------

% return a random election timeout
rand_election_timeout() ->
    From = ?ELECTION_TIMEOUT_MIN-1,
    Offset = rand:uniform(?ELECTION_TIMEOUT_MAX-?ELECTION_TIMEOUT_MIN),
    From+Offset.

%%--------------------------------------------------------------------

% return the required quorum for the given nodes
quorum() ->
    floor((length(?NODES))/2+1).
