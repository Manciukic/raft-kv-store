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

-record(state, {currentTerm, votedFor, log, commitIndex, lastApplied, nextIndex, matchIndex, electionTimeout, leader, votesFor, pendingRequests, nodes}).
-record(log_entry, {term, content}).

start_link() ->
    logger:notice("starting RAFT~n", []),
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

start() ->
    ?FSM_MODULE:start_link(),
    start_link().

add_entry(Entry) ->
    gen_statem:call(?SERVER, {add_entry, Entry}).


get_leader() ->
    gen_statem:call(?SERVER, get_leader).


stop() ->
    gen_statem:stop(?SERVER).


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


callback_mode() ->
    state_functions.


follower(state_timeout, electionTimeout, #state{}=State) ->
    logger:debug("follower: electionTimeout~n", []),
    {next_state, candidate, State,
        [{state_timeout, 0, electionTimeout}]
    };

follower(cast, {append_entries, From, #append_entries_req{
    term=Term,
    prevLogIndex=PrevLogIndex, 
    prevLogTerm=PrevLogTerm
}=Req}, #state{
    currentTerm=CurrentTerm,
    log=Log
} = State) ->
    NewState = if 
        Term < CurrentTerm -> 
            logger:debug("follower: append_entries: old term~n", []),
            reply_append_entries(From, CurrentTerm, false, 0),
            State;
        PrevLogIndex > length(Log) ->
            logger:debug("follower: append_entries: prevLogIndex not in log (~p)~n", [PrevLogIndex]),
                    reply_append_entries(From, Term, false, length(Log)),
                    State#state{currentTerm=Term};
        true ->
            NthLogTerm = nth_log_entry_term(PrevLogIndex, Log),
            if
                PrevLogTerm =/= NthLogTerm ->
                    logger:debug("follower: append_entries: wrong nth log term~n", []),
                    reply_append_entries(From, Term, false, PrevLogIndex),
                    State#state{currentTerm=Term};
                true ->
                    logger:debug("follower: append_entries: ok~n", []),
                    NewState_ = do_append_entries(Req, State),
                    #state{log=NewLog} = NewState_,
                    reply_append_entries(From, Term, true, length(NewLog)),
                    NewState_
            end
    end,
    {keep_state, NewState, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

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
        Term < CurrentTerm ->
            logger:debug("follower: request_vote: wrong term~n", []),
            reply_request_vote(From, CurrentTerm, false),
            State;
        (VotedFor == null) or (VotedFor == CandidateId) ->
            NewState_ = State#state{currentTerm=max(CurrentTerm,Term)},
            {MyLastLogIndex, MyLastLogTerm} = last_log_index_term(Log),
            if 
                MyLastLogIndex > 0 ->
                    if 
                        LastLogTerm > MyLastLogTerm ->
                            logger:debug("follower: request_vote: voting for ~p (higher term)~n", [CandidateId]),
                            reply_request_vote(From, CurrentTerm, true),
                            NewState_#state{votedFor=CandidateId};
                        (LastLogTerm == MyLastLogTerm) and (LastLogIndex >= MyLastLogIndex) ->
                            logger:debug("follower: request_vote: voting for ~p (higher index)~n", [CandidateId]),
                            reply_request_vote(From, CurrentTerm, true),
                            NewState_#state{votedFor=CandidateId};
                        true ->
                            logger:debug("follower: request_vote: outdated log~n", []),
                            reply_request_vote(From, CurrentTerm, false),
                            NewState_
                    end;
                true -> % empty log
                    logger:debug("follower: request_vote: voting for ~p (empty log)~n", [CandidateId]),
                    reply_request_vote(From, CurrentTerm, true),
                    NewState_#state{votedFor=CandidateId}
            end;
        true ->
            % already voted
            logger:debug("follower: request_vote: vote already casted~n", []),
            reply_request_vote(From, CurrentTerm, false),
            State
    end,
    {keep_state, NewState,
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

follower(EventType, EventContent, Data) ->
    handle_common(follower, EventType, EventContent, Data).


candidate(state_timeout, electionTimeout, #state{
    currentTerm=CurrentTerm,
    log=Log,
    nodes=Nodes
}=State) ->
    logger:info("candidate: starting election~n", []),
    NewState = State#state{
        currentTerm=CurrentTerm+1,
        votedFor=node(self()),
        votesFor=sets:from_list([node(self())])
    },
    {LastLogIndex, LastLogTerm} = last_log_index_term(Log),
    request_votes(Nodes, CurrentTerm+1, node(self()), LastLogIndex, LastLogTerm),
    {keep_state, NewState, 
        [{state_timeout, rand_election_timeout(), electionTimeout}]};

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
                NVotes >= Quorum ->
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
                    }, [{state_timeout, 0, heartBeatTimeout}]};
                true ->
                    logger:debug("candidate: request_vote_rpy: not enough votes (~p)~n", [NVotes]),
                    {keep_state, State#state{votesFor=NewVotesFor}}
            end;
        true ->
            logger:debug("candidate: request_vote_rpy: vote rejected from ~p~n", [node(From)]),
            {keep_state, State#state{currentTerm=max(Term,CurrentTerm)}}
    end;

candidate(cast, {request_vote, From, #request_vote_req{
    term=Term
}}, #state{currentTerm=CurrentTerm}=State) ->
    if 
        Term > CurrentTerm ->
            logger:debug("candidate: request_vote: term is higher~n", []),
            {next_state, follower, State#state{
                currentTerm=Term,
                votedFor=null
            }, [postpone]};
        true ->
            logger:debug("candidate: request_vote: refused~n", []),
            reply_request_vote(From, CurrentTerm, false),
            keep_state_and_data
    end;

candidate(cast, {append_entries, _From, _Req}, State) ->
    %TODO check correctness
    logger:debug("candidate: append_entries: backing to follower~n", []),
    {next_state, follower, State, [postpone]};

candidate(EventType, EventContent, Data) ->
    handle_common(candidate, EventType, EventContent, Data).


leader(state_timeout, heartBeatTimeout, State) ->
    logger:debug("leader: heartBeatTimeout~n", []),
    send_heartbeat(State),
    {keep_state_and_data, [
        {state_timeout, ?HEART_BEAT_TIMEOUT, heartBeatTimeout}
    ]};

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
        Term < CurrentTerm ->
            % ignore
            logger:debug("leader: append_entries_rpy: old term~n", []),
            State;
        Term > CurrentTerm ->
            logger:debug("leader: append_entries_rpy: newer term, backing off~n", []),
            {next_state, follower, State#state{
                currentTerm=Term,
                votedFor=null
            }, [postpone]};
        Success ->
            logger:debug("leader: append_entries_rpy: success~n", []),
            NewNextIndexMap = maps:put(Node, MatchIndex+1, NextIndexMap),
            NewMatchIndexMap = maps:put(Node, MatchIndex, MatchIndexMap),
            NewCommitIndex = update_commit_index(CommitIndex, NewMatchIndexMap, Log, CurrentTerm),
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
                    logger:debug("leader: append_entries_rpy: next_index is already 0~n", []),
                    State;
                true ->
                    NewNextIndexMap = maps:put(Node, OldNextIndex-1, 
                                                NextIndexMap),
                    NewState_ = State#state{
                        nextIndex=NewNextIndexMap
                    },
                    send_append_entries(node(From), NewState_),
                    NewState_
            end
    end,
    {keep_state, NewState, [
        {state_timeout, ?HEART_BEAT_TIMEOUT, heartBeatTimeout}
    ]};

leader({call, From}, get_leader, _State) ->
    logger:debug("leader: get_leader: I am~n", []),
    gen_statem:reply(From, node(self())),
    keep_state_and_data;

leader({call, From}, {add_entry, Entry}, #state{
    pendingRequests=PendingRequests,
    log=Log
}=State) ->
    logger:debug("leader: add_entry~n", []),
    NewState=persist_entries([Entry], State),
    send_all_append_entries(NewState),
    NewPendingRequests = [{From, length(Log)+1} | PendingRequests],
    {keep_state, NewState#state{pendingRequests=NewPendingRequests}};

leader(EventType, EventContent, Data) ->
    handle_common(leader, EventType, EventContent, Data).


handle_common(State, {call, From}, get_leader, #state{leader=Leader}) ->
    logger:debug("~p: get_leader: leader is ~p~n", [State]),
    gen_statem:reply(From, Leader),
    keep_state_and_data;

handle_common(State, {call, From}, {add_entry, _}, _State) ->
    logger:debug("~p: add_entry: not a leader~n", [State]),
    gen_statem:reply(From, false),
    keep_state_and_data;

handle_common(State, cast, {Operation, _From, _Data}, _State) ->
    logger:debug("~p: ~p: not allowed in this state~n", [State, Operation]),
    keep_state_and_data;

handle_common(State, EventType, _EventContent, _Data) ->
    logger:debug("~p: event not handled: ~p~n", [State, EventType]),
    keep_state_and_data.


terminate(_Reason, _StateName, _State) ->
    logger:notice("(terminate) stopping~n", []),
    ok.

% internal functions

request_votes(Nodes, Term, CandidateId, LastLogIndex, LastLogTerm) ->
    logger:debug("(request_votes) term=~p, candidate=~p, lastLogIndex=~p, lastLogTerm=~p~n", [Term, CandidateId, LastLogIndex, LastLogTerm]),
    send_all(Nodes, {request_vote, self(), #request_vote_req{
            term=Term, 
            candidateId=CandidateId, 
            lastLogIndex=LastLogIndex, 
            lastLogTerm=LastLogTerm
        }}).


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

send_all([], _Req) ->
    true;
send_all([HNode | TNodes], Req) ->
    gen_statem:cast({?SERVER, HNode}, Req),
    send_all(TNodes, Req).


send_all_append_entries(#state{nodes=Nodes}=State) ->
    send_all_append_entries(Nodes, State).


send_all_append_entries([], _State) ->
    true;
send_all_append_entries([HNode | TNodes], State) ->
    send_append_entries(HNode, State),
    send_all_append_entries(TNodes, State).


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

% return a random election timeout
rand_election_timeout() ->
    From = ?ELECTION_TIMEOUT_MIN-1,
    Offset = rand:uniform(?ELECTION_TIMEOUT_MAX-?ELECTION_TIMEOUT_MIN),
    From+Offset.


% return the required quorum for the given nodes
quorum() ->
    floor((length(?NODES))/2+1).


reply_append_entries(To, Term, Success, MatchIndex) ->
    logger:debug("(reply_append_entries) node=~p, term=~p, success=~p, index=~p~n", [To, Term, Success, MatchIndex]),
    gen_statem:cast({?SERVER, node(To)}, {append_entries_rpy, self(), 
        #append_entries_rpy{
            term=Term,
            success=Success,
            matchIndex=MatchIndex
        }
    }).


reply_request_vote(To, Term, VoteGranted) ->
    logger:debug("(reply_request_vote) node=~p, term=~p, voteGranted=~p~n", [To, Term, VoteGranted]),
    gen_statem:cast({?SERVER, node(To)}, {request_vote_rpy, self(), #request_vote_rpy{
        term=Term,
        voteGranted=VoteGranted
    }}).


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
    NewLog = lists:sublist(Log, PrevLogIndex+1) ++ Entries,
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


last_log_index_term([]) ->
    {0,0};

last_log_index_term(Log) ->
    #log_entry{term=MyLastLogTerm} = lists:last(Log),
    {length(Log), MyLastLogTerm}.

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


update_commit_index(OldCommitIndex, _MatchIndexMap, [], _CurrentTerm) ->
    logger:debug("(update_commit_index) empty log~n", []),
    OldCommitIndex;
update_commit_index(OldCommitIndex, MatchIndexMap, Log, CurrentTerm) ->
    NextCommitIndex = find_next_commit_index(OldCommitIndex+1, MatchIndexMap),
    NthTerm = nth_log_entry_term(NextCommitIndex, Log),
    if 
        (NextCommitIndex > OldCommitIndex) and (NthTerm == CurrentTerm) ->
            logger:debug("(update_commit_index) new commit index is ~p~n", [NextCommitIndex]),
            NextCommitIndex;
        true ->
            logger:debug("(update_commit_index) commit index did not change (~p)~n", [OldCommitIndex]),
            OldCommitIndex
    end.


find_next_commit_index(TryIndex, MatchIndexMap) ->
    Quorum = quorum(),
    NGreaterThan = length(
        lists:filter(
            fun(X) -> X >= TryIndex end, 
            maps:values(MatchIndexMap)
        )
    ),
    
    if 
        NGreaterThan+1 >= Quorum ->
            % if index is ok, try next one
            find_next_commit_index(TryIndex+1, MatchIndexMap);
        true ->
            % otherwise return the last correct one
            TryIndex-1
    end.
        

do_commit_entries(From, To, _Log, PendingRequests) when From > To ->
    % do nothing
    PendingRequests;

do_commit_entries(From, To, Log, PendingRequests) ->
    {Handled, NewPendingRequests} = acknowledge_request(From, PendingRequests),
    if 
        Handled ->
            pass;
        true ->
            commit_entry(nth_log_entry(From, Log))
    end,
    do_commit_entries(From+1, To, Log, NewPendingRequests).


commit_entry(#log_entry{content=Entry}) ->
    logger:info("(commit_entry) ~p~n", [Entry]),
    gen_server:call(?FSM_MODULE, {commit_entry, Entry}).


acknowledge_request(CommitIndex, Requests) -> 
    acknowledge_request(CommitIndex, Requests, [], false).

acknowledge_request(_CommitIndex, [], NewPendingRequests, Handled) -> 
    {Handled, NewPendingRequests};
    
acknowledge_request(CommitIndex, [{Node, Index} | TRequests], NewPendingRequests, _Handled) 
when Index == CommitIndex -> 
    logger:debug("(acknowledge_request) node=~p, index=~p~n", [Node, Index]),
    gen_statem:reply(Node, ok),
    acknowledge_request(CommitIndex, TRequests, NewPendingRequests, true); 

acknowledge_request(CommitIndex, [HRequest | TRequests], NewPendingRequests, Handled) ->
    acknowledge_request(CommitIndex, TRequests, [HRequest | NewPendingRequests], Handled).


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


nth_log_entry_term(N, Log) ->
    extract_term(nth_log_entry(N, Log)).


extract_term(null) ->
    0;
extract_term(#log_entry{term=Term}) ->
    Term.


log_sublist([], _From) ->
    [];

log_sublist(Log, 0) ->
    Log;

log_sublist(Log, From) ->
    lists:sublist(Log, From, length(Log)).