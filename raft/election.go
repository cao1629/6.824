package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term        int
    CandidateId int

    LastLogIndex int
    LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term        int
    VoteGranted bool
}

//
// example RequestVote RPC handler.
//
// I receive a RequestVote RPC from a candidate.
// I could be a leader, candidate, or a follower.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    //LOG(dDebug, "S%d, Term: %d, Starting processing RequestVote from S%d", rf.me, rf.currentTerm, args.CandidateId)
    rf.mu.Lock()
    //defer LOG(dDebug, "S%d, Term: %d, Finished processing RequestVote from S%d", rf.me, rf.currentTerm, args.CandidateId)
    defer rf.mu.Unlock()

    // Your code here (2A, 2B).
    //LOG(dVote, "S%d, Term: %d, Voting for S%d", rf.me, rf.currentTerm, args.CandidateId)

    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    // I received a message from someone with a smaller term.
    // Ignore this message, send back my current term and false.
    if rf.currentTerm > args.Term {
        LOG(dVote, "S%d, Term %d, Vote false for S%d, Reason: Ignore messages with smaller term",
            rf.me, rf.currentTerm, args.CandidateId)
        return
    }

    // [raft-paper 5.4.1 3 the candidate's log is at least as up-to-date as the receiver's log]
    // Check if the candidate's log is at least as up-to-date as mine.
    upToDateCandidate := rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm)

    // 1. If I'm currently a leader or candidate with a smaller term, I've already voted for myself in this term.
    // Now I learn that there is a candidate with a higher term.
    // I need to update my term, become a follower, vote for this candidate, and reset my election timer.
    //
    // 2. If I'm currently a follower with a smaller term. I've probably voted for someone in this term.
    // I probably haven't voted for anyone.
    // In this case I just need to update my term, and vote for this candidate.
    if didUpdateTerm := rf.mayUpdateTerm(args.Term, args.CandidateId); didUpdateTerm {
        if upToDateCandidate {
            rf.votedFor = args.CandidateId
            reply.VoteGranted = true
        }

        // currentTerm has been updated
        // votedFor has probably been updated
        rf.persist()
        return
    }

    if !upToDateCandidate {
        LOG(dVote, "S%d, Term: %d, Vote false for S%d, Reason: Candidate's log is not up-to-date",
            rf.me, rf.currentTerm, args.CandidateId)
        return
    }

    // Another case: I'm a follower with the same term as the candidate, but I've already voted for someone in this term.
    if rf.votedFor == -1 {
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
    } else {
        // I have already voted for someone in this term.
        LOG(dVote, "S%d, Term: %d, Vote false for S%d, Reason: Already voted for S%d in this term",
            rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)
    }

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

// non-thread-safe
func (rf *Raft) isContextLost(expectedSeverState ServerState, term int) bool {
    if rf.currentTerm > term {
        return true
    }

    if rf.serverState != expectedSeverState {
        return true
    }

    return false
}

func (rf *Raft) StartElection() {
    rf.mu.Lock()
    oldTerm := rf.currentTerm
    oldState := rf.serverState
    rf.currentTerm++
    rf.serverState = Candidate
    rf.votedFor = rf.me
    LOG(dState, "S%d, Term: %d -> %d, State: %s -> %s, Reason: timeout", rf.me, oldTerm, rf.currentTerm, oldState, rf.serverState)
    rf.persist()
    rf.electionTicker.Reset(generateRandomTimeout())
    electionTerm := rf.currentTerm
    rf.mu.Unlock()

    voteGrantedCh := make(chan bool)

    // send RequestVote RPCs to all the other peers concurrently
    for i := range rf.peers {
        if i == rf.me {
            continue
        }

        peer := i
        go func() {
            LOG(dElection, "S%d, Term: %d, Request Vote from S%d", rf.me, rf.currentTerm, peer)
            voteGranted := rf.RequestVoteFrom(peer)
            LOG(dElection, "S%d, Term: %d, Vote from S%d: %t", rf.me, rf.currentTerm, peer, voteGranted)
            voteGrantedCh <- voteGranted
        }()
    }

    // While waiting for the replies, I may no longer be a candidate.
    // start a goroutine to process the replies
    go func() {
        votes := 1

        for {
            select {
            case voteGranted := <-voteGrantedCh:
                rf.mu.Lock()
                // At this point, I expect myself to be a candidate. However, I may have become a follower or leader.
                // 1. If I have become a follower, which means I have learned a higher term, then I should stop the election.
                // 2. If I have become a leader, which means I have received enough votes. But this case seems never to happen.
                if rf.isContextLost(Candidate, electionTerm) {
                    // This round of election is no longer valid.
                    rf.mu.Unlock()
                    break
                }

                if voteGranted {
                    votes++
                    // Just reach a majority of votes.
                    if votes > len(rf.peers)/2 {
                        // I have enough votes to become a leader.
                        // Stop the election ticker and start the heartbeat ticker.
                        // Break this "for select loop" to ignore the remaining votes.
                        LOG(dElection, "S%d, Term: %d, Won election", rf.me, rf.currentTerm)
                        LOG(dState, "S%d, Term: %d, State: %s -> %s", rf.me, rf.currentTerm, rf.serverState, Leader)
                        rf.serverState = Leader
                        for i := range rf.peers {
                            rf.nextIndex[i] = len(rf.log)
                            rf.matchIndex[i] = len(rf.log)
                        }
                        rf.electionTicker.Stop()
                        rf.heartbeatTicker.Reset(heartbeatInterval)

                        // I've received enough votes and become a leader. Terminate this goroutine to ignore the remaining votes.
                        rf.mu.Unlock()
                        break
                    }
                }
                rf.mu.Unlock()
            }
        }
    }()
}

// sync
// If the peer grants my vote, return true. Otherwise return false.
func (rf *Raft) RequestVoteFrom(peer int) bool {

    args := RequestVoteArgs{
        Term:         rf.currentTerm,
        CandidateId:  rf.me,
        LastLogIndex: len(rf.log) - 1,
        LastLogTerm:  rf.log[len(rf.log)-1].Term,
    }

    reply := RequestVoteReply{}
    if ok := rf.sendRequestVote(peer, &args, &reply); !ok {
        return false
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if didUpdate := rf.mayUpdateTerm(reply.Term, peer); didUpdate {
        return false
    }

    return reply.VoteGranted
}

// non-thread-safe
// I'm the receiver of the RequestVote RPC.
// I want to check if the candidate is an eligible candidate.
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogIndex, candidateLastLogTerm int) bool {
    myLastLogIndex := len(rf.log) - 1
    myLastLogTerm := rf.log[myLastLogIndex].Term

    if candidateLastLogTerm > myLastLogTerm {
        return true
    } else if candidateLastLogTerm < myLastLogTerm {
        return false
    } else {
        return candidateLastLogIndex >= myLastLogIndex
    }
}
