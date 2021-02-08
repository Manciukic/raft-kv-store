%%%-------------------------------------------------------------------
%%% @doc
%%% Core module for the implementation of the RAFT FSM.
%%% This module allows the replication of a FSM among different Erlang
%%% nodes. The FSM to be replicated should be a gen_server module 
%%% indicated by FSM_MODULE macro in the configuration header.
%%% @end
%%%-------------------------------------------------------------------

-module(raft_statem).

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
         stop/0
        ]).

-include("raft_config.hrl").
-include("raft_interface.hrl").

-define(SERVER, ?MODULE).

% data held between states
% TODO: use different records for each state to save memory ?
-record(state, {currentTerm, votedFor, log, commitIndex, lastApplied, nextIndex, matchIndex, electionTimeout, leader, votesFor, pendingRequests, nodes}).

% structure of a log entry
-record(log_entry, {term, content}).

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
add_entry(Entry) ->
    gen_statem:call(?SERVER, {add_entry, Entry}).

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
        currentTerm=0, 
        votedFor=null, 
        log=[], 
        commitIndex=0, 
        lastApplied=0, 
        nextIndex=maps:new(),
        matchIndex=maps:new(),
        leader=null,
        pendingRequests=[],
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

% check validity and add the new entries from the log and reply to the leader
follower(cast, {append_entries, From, #append_entries_req{
    term=Term,
    prevLogIndex=PrevLogIndex, 
    prevLogTerm=PrevLogTerm
}=Req}, #state{
    currentTerm=CurrentTerm,
    log=Log
} = State) ->
    NewState = if 
        Term < CurrentTerm -> % the request is from an older term, deny it
            logger:debug("follower: append_entries: old term~n", []),
            reply_append_entries(From, CurrentTerm, false, 0),
            State;
        PrevLogIndex > length(Log) -> % local log is out-of-sync, signal it
            logger:debug("follower: append_entries: prevLogIndex not in log (~p)~n", [PrevLogIndex]),
            reply_append_entries(From, Term, false, length(Log)),
                                 State#state{currentTerm=Term};
        true -> 
            NthLogTerm = nth_log_entry_term(PrevLogIndex, Log),
            if
                PrevLogTerm =/= NthLogTerm -> 
                    % entry is incompatible with local log, deny it
                    logger:debug("follower: append_entries: wrong nth log term~n", []),
                    reply_append_entries(From, Term, false, PrevLogIndex),
                    State#state{currentTerm=Term};
                true -> % ok
                    logger:debug("follower: append_entries: ok~n", []),
                    NewState_ = do_append_entries(Req, State),
                    #state{log=NewLog} = NewState_,
                    reply_append_entries(From, Term, true, length(NewLog)),
                    NewState_
            end
    end,
    % reset the election timeout
    {keep_state, NewState, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

% check if candidate log is updated and reply to its vote request
follower(cast, {request_vote, From, #request_vote_req{
    term = Term, 
    candidateId = CandidateId, 
    lastLogIndex = LastLogIndex, 
    lastLogTerm = LastLogTerm
}}, #state{
    currentTerm = CurrentTerm,
    votedFor = VotedFor,
    log = Log
} = State) ->
    NewState = if
        Term < CurrentTerm -> % candidate has an old term, deny vote
            logger:debug("follower: request_vote: wrong term~n", []),
            reply_request_vote(From, CurrentTerm, false),
            State;
        (VotedFor == null) or (VotedFor == CandidateId) ->
            % no vote casted or repeated request_vote
            % update current term
            NewState_ = State#state{currentTerm=max(CurrentTerm,Term)},
            {MyLastLogIndex, MyLastLogTerm} = last_log_index_term(Log),
            if 
                MyLastLogIndex > 0 -> % log is not empty
                    if 
                        LastLogTerm > MyLastLogTerm -> 
                            % leader log is more updated than mine, vote for 
                            % him
                            logger:debug("follower: request_vote: voting for ~p (higher term)~n", [CandidateId]),
                            reply_request_vote(From, CurrentTerm, true),
                            % remember who I voted for
                            NewState_#state{votedFor=CandidateId};
                        (LastLogTerm == MyLastLogTerm) and (LastLogIndex >= MyLastLogIndex) ->
                            % leader log is more updated than mine, vote for 
                            % him
                            logger:debug("follower: request_vote: voting for ~p (higher index)~n", [CandidateId]),
                            reply_request_vote(From, CurrentTerm, true),
                            % remember who I voted for
                            NewState_#state{votedFor=CandidateId};
                        true -> % leader log is out-dated, deny vote
                            logger:debug("follower: request_vote: outdated log~n", []),
                            reply_request_vote(From, CurrentTerm, false),
                            NewState_
                    end;
                true -> % empty log, vote for the candidate
                    logger:debug("follower: request_vote: voting for ~p (empty log)~n", [CandidateId]),
                    reply_request_vote(From, CurrentTerm, true),
                    % remember who I voted for
                    NewState_#state{votedFor=CandidateId}
            end;
        true -> % already voted, deny vote
            logger:debug("follower: request_vote: voted already~n", []),
            reply_request_vote(From, CurrentTerm, false),
            State
    end,
    % reset election timeout
    {keep_state, NewState,
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

% for any other event, use the common handler
follower(EventType, EventContent, Data) ->
    handle_common(follower, EventType, EventContent, Data).

%%--------------------------------------------------------------------
%% candidate
%% A candidate asks other nodes to vote for him and, if it receives a 
%% majority of the votes, it becomes the new leader.

% not enough replies within time, start a new election
candidate(state_timeout, electionTimeout, #state{
    currentTerm=CurrentTerm,
    log=Log,
    nodes=Nodes
}=State) ->
    logger:info("candidate: starting election~n", []),
    NewState = State#state{
        currentTerm=CurrentTerm+1, % TODO check
        votedFor=node(self()),
        votesFor=sets:from_list([node(self())])
    },
    {LastLogIndex, LastLogTerm} = last_log_index_term(Log),
    % request votes from all other nodes
    request_votes(Nodes, CurrentTerm+1, node(self()), 
                  LastLogIndex, LastLogTerm),
    % start a new election timeout
    {keep_state, NewState, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

% reply from other node
candidate(cast, {request_vote_rpy, From, #request_vote_rpy{
    term=Term,
    voteGranted=VoteGranted
}}, #state{
    votesFor=VotesFor,
    currentTerm=CurrentTerm,
    log=Log,
    nodes=Nodes
}=State) ->
    if 
        VoteGranted -> 
            NewVotesFor = sets:add_element(node(From), VotesFor),
            Quorum = quorum(),
            NVotes = sets:size(NewVotesFor),
            if 
                NVotes >= Quorum -> % reached quorum, become leader
                    logger:debug("candidate: request_vote_rpy: reached quorum (~p)~n", [NVotes]),
                    logger:info("elected as leader~n", []),
                    {LastLogIndex, _LastLogTerm} = last_log_index_term(Log),
                    {next_state, leader, State#state{
                            leader=node(self()),
                            nextIndex=maps:from_list([
                                {Node, LastLogIndex+1}
                                || Node <- Nodes
                            ]),
                            matchIndex=maps:from_list([
                                {Node, 0}
                                || Node <- Nodes
                            ]),
                            pendingRequests=[]   
                        }, 
                        % send heartbeat to notify election end
                        [{state_timeout, 0, heartBeatTimeout}]
                    };
                true -> % not enough votes, update state and wait
                    logger:debug("candidate: request_vote_rpy: not enough votes (~p)~n", [NVotes]),
                    {keep_state, State#state{votesFor=NewVotesFor}}
            end;
        true -> % candidate has been rejected
            logger:debug("candidate: request_vote_rpy: vote rejected from ~p~n", [node(From)]),
            {keep_state, State#state{currentTerm=max(Term,CurrentTerm)}}
    end;

% receive a request to vote from another candidate
candidate(cast, {request_vote, From, #request_vote_req{
    term=Term
}}, #state{currentTerm=CurrentTerm}=State) ->
    if 
        Term > CurrentTerm -> % his term is higher, vote for him
            logger:debug("candidate: request_vote: term is higher~n", []),
            {next_state, follower, State#state{
                currentTerm=Term,
                votedFor=null
            }, 
            % request_vote handled in follower state
            [postpone]};
        true -> % otherwise, deny vote
            logger:debug("candidate: request_vote: refused~n", []),
            reply_request_vote(From, CurrentTerm, false),
            keep_state_and_data
    end;

% received append_entries from new leader, revert to follower and handle it
candidate(cast, {append_entries, _From, _Req}, State) ->
    %TODO check correctness
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

% client sent a reply to append entries
leader(cast, {append_entries_rpy, From, #append_entries_rpy{
    term=Term, 
    success=Success,
    matchIndex=MatchIndex}
}, #state{
    currentTerm=CurrentTerm,
    nextIndex=NextIndexMap,
    matchIndex=MatchIndexMap,
    commitIndex=CommitIndex,
    log=Log,
    pendingRequests=PendingRequests,
    lastApplied=LastApplied
}=State) ->
    Node = node(From),
    NewState = if
        Term < CurrentTerm -> % reply refers to an older term, ignore
            logger:debug("leader: append_entries_rpy: old term~n", []),
            State;
        Term > CurrentTerm -> % reply is from a newer term, revert to follower
            % this should not happen in practice
            logger:debug("leader: append_entries_rpy: newer term, backing off~n", []),
            {next_state, follower, State#state{
                currentTerm=Term,
                votedFor=null
            }, [postpone]};
        Success ->  
            % append_entries was successfull, update internal view of follower
            logger:debug("leader: append_entries_rpy: success~n", []),
            NewNextIndexMap = maps:put(Node, MatchIndex+1, NextIndexMap),
            NewMatchIndexMap = maps:put(Node, MatchIndex, MatchIndexMap),
            % check whether new entries can be commited
            NewCommitIndex = update_commit_index(CommitIndex, NewMatchIndexMap, Log, CurrentTerm),
            % if so, do those commits
            NewPendingRequests = do_commit_entries(LastApplied+1, NewCommitIndex, Log, PendingRequests),
            State#state{
                nextIndex=NewNextIndexMap,
                matchIndex=NewMatchIndexMap,
                commitIndex=NewCommitIndex,
                lastApplied=NewCommitIndex,
                pendingRequests=NewPendingRequests
            };
        true -> % Success is false
            OldNextIndex = maps:get(Node, NextIndexMap),
            if 
                OldNextIndex == 0 -> 
                    % cannot decrease next_index further, it means that the 
                    % follower is misbehaving, ignore it
                    logger:debug("leader: append_entries_rpy: next_index is already 0~n", []),
                    State;
                true -> % decrease next_index and retry
                    NewNextIndexMap = maps:put(Node, OldNextIndex-1, 
                                                NextIndexMap),
                    NewState_ = State#state{
                        nextIndex=NewNextIndexMap
                    },
                    send_append_entries(node(From), NewState_),
                    NewState_
            end
    end,
    {keep_state, NewState};

% add the new entry to the log and send append entries to all other nodes
leader({call, From}, {add_entry, Entry}, #state{
    pendingRequests=PendingRequests,
    log=Log
}=State) ->
    logger:debug("leader: add_entry~n", []),
    NewState=persist_entries([Entry], State),
    send_all_append_entries(NewState),
    NewPendingRequests = [{From, length(Log)+1} | PendingRequests],
    {keep_state, NewState#state{pendingRequests=NewPendingRequests}, [
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
    logger:debug("(request_votes) term=~p, candidate=~p, lastLogIndex=~p, lastLogTerm=~p~n", [Term, CandidateId, LastLogIndex, LastLogTerm]),
    send_all(Nodes, {request_vote, self(), #request_vote_req{
            term=Term, 
            candidateId=CandidateId, 
            lastLogIndex=LastLogIndex, 
            lastLogTerm=LastLogTerm
        }}).

%%--------------------------------------------------------------------

% send heartbeat to all other nodes
send_heartbeat(#state{
    currentTerm=CurrentTerm,
    commitIndex=CommitIndex,
    log=Log,
    nodes=Nodes
}) -> 
    {LastLogIndex, LastLogTerm} = last_log_index_term(Log),
    logger:debug("(send_heartbeat) term=~p, commitIndex=~p, lastLogIndex=~p, lastLogTerm=~p~n", [CurrentTerm, CommitIndex, LastLogIndex, LastLogTerm]),
    send_all(Nodes, {append_entries, self(), #append_entries_req{
        term=CurrentTerm, 
        leaderId=node(self()), 
        prevLogIndex=LastLogIndex, 
        prevLogTerm=LastLogTerm, 
        entries=[], 
        leaderCommit=CommitIndex
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
    nextIndex=NextIndexMap,
    commitIndex=CommitIndex,
    currentTerm=CurrentTerm,
    log=Log
}) ->
    NextIndex = maps:get(Node, NextIndexMap),
    PrevLogIndex = max(0, NextIndex-1),
    PrevLogTerm = nth_log_entry_term(PrevLogIndex, Log),
    logger:debug("(send_append_entries) node=~p, term=~p, prevLogIndex=~p, prevLogTerm=~p leaderCommit=~p~n", [Node, CurrentTerm, PrevLogIndex, PrevLogTerm, CommitIndex]),

    gen_statem:cast({?SERVER, Node}, {append_entries, self(), 
        #append_entries_req{
            term=CurrentTerm, 
            leaderId=node(self()), 
            prevLogIndex=PrevLogIndex, 
            prevLogTerm=PrevLogTerm, 
            entries=log_sublist(Log, NextIndex), 
            leaderCommit=CommitIndex
        }
    }).

%%--------------------------------------------------------------------

% reply to an append entries request
reply_append_entries(To, Term, Success, MatchIndex) ->
    logger:debug("(reply_append_entries) node=~p, term=~p, success=~p, index=~p~n", [To, Term, Success, MatchIndex]),
    gen_statem:cast({?SERVER, node(To)}, {append_entries_rpy, self(), 
        #append_entries_rpy{
            term=Term,
            success=Success,
            matchIndex=MatchIndex
        }
    }).
    
%%--------------------------------------------------------------------

% reply to a request_vote request
reply_request_vote(To, Term, VoteGranted) ->
    logger:debug("(reply_request_vote) node=~p, term=~p, voteGranted=~p~n", [To, Term, VoteGranted]),
    gen_statem:cast({?SERVER, node(To)}, {request_vote_rpy, self(), #request_vote_rpy{
        term=Term,
        voteGranted=VoteGranted
    }}).

%%--------------------------------------------------------------------
%% append_entries-related functions (appending and committing)
%%--------------------------------------------------------------------

% add the new entries to the log and commit any uncommitted entries
do_append_entries(#append_entries_req{
    term=Term,
    leaderId=Leader, 
    prevLogIndex=PrevLogIndex,  
    entries=Entries, 
    leaderCommit=LeaderCommit
}, #state{
    commitIndex=CommitIndex,
    log=Log,
    lastApplied=LastApplied
}=State) ->
    case Entries of
        [] ->
            pass;
        _ -> logger:info("(do_append_entries) prevLogIndex=~p, size(entries)=~p~n", [PrevLogIndex, length(Entries)])
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
    do_commit_entries(LastApplied+1, NewCommitIndex, NewLog, []),
    State#state{
        leader=Leader,
        commitIndex=NewCommitIndex,
        log=NewLog,
        currentTerm=Term,
        lastApplied=NewCommitIndex
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
            logger:debug("(update_commit_index) new commit index is ~p~n", [NextCommitIndex]),
            NextCommitIndex;
        true -> % entry was from another leader, ignore
            logger:debug("(update_commit_index) commit index did not change (~p)~n", [OldCommitIndex]),
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
do_commit_entries(From, To, _Log, PendingRequests) when From > To ->
    PendingRequests;

do_commit_entries(From, To, Log, PendingRequests) ->
    % send replies if FSM is waiting
    {Handled, NewPendingRequests} = acknowledge_request(From, PendingRequests),
    if 
        Handled -> % reply sent, do nothing
            pass;
        true -> % issue commit on FSM otherwise
            commit_entry(nth_log_entry(From, Log))
    end,
    % commit next index
    do_commit_entries(From+1, To, Log, NewPendingRequests).

%%--------------------------------------------------------------------

% send a commit_entry RPC to FSM
commit_entry(#log_entry{content=Entry}) ->
    logger:info("(commit_entry) ~p~n", [Entry]),
    gen_server:call(?FSM_MODULE, {commit_entry, Entry}).

%%--------------------------------------------------------------------

% reply to pending FSM add_entry RPCs.
acknowledge_request(CommitIndex, Requests) -> 
    acknowledge_request(CommitIndex, Requests, [], false).

%%--------------------------------------------------------------------

% Reply to pending FSM add_entry RPCs (internal).
% Check all pending requests and reply if index == commitIndex.

acknowledge_request(_CommitIndex, [], NewPendingRequests, Handled) -> 
    {Handled, NewPendingRequests};
    
acknowledge_request(CommitIndex, [{Node, Index} | TRequests], NewPendingRequests, _Handled) 
when Index == CommitIndex -> % reply
    logger:debug("(acknowledge_request) node=~p, index=~p~n", [Node, Index]),
    gen_statem:reply(Node, ok),
    % remove this item from list, set handled to true
    acknowledge_request(CommitIndex, TRequests, NewPendingRequests, true); 

acknowledge_request(CommitIndex, [HRequest | TRequests], NewPendingRequests, Handled) -> % no reply
    % pass list as is, handled as is
    acknowledge_request(CommitIndex, TRequests, [HRequest | NewPendingRequests], Handled).

%%--------------------------------------------------------------------
%% Log-related functions
%%--------------------------------------------------------------------

% Save new entries in log and persist on disk
% TODO: this is only done in leader, not in follower!
persist_entries(Entries, #state{
    log=Log,
    currentTerm=CurrentTerm
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
    