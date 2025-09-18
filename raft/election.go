package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term        int
    CandidateId int

    LastLogIndex int
    LastLogTerm  int

    // for debugging
    RpcId uint32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
    // Your data here (2A).
    Term        int
    VoteGranted bool

    // for debugging
    RpcId int
}

// example RequestVote RPC handler.
//
// I receive a RequestVote RPC from a candidate.
// I could be a leader, candidate, or a follower.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    //rf.logLockUnlock(true)
    defer func() {
        rf.mu.Unlock()
        //rf.logLockUnlock(false)
    }()

    detail := map[string]interface{}{
        "RpcId": args.RpcId,
    }
    rf.logRpc(args.CandidateId, rf.me, "REQUEST_VOTE ARGS", rf.currentTerm, args.RpcId, detail)

    // Your code here (2A, 2B).
    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    // I received a message from someone with a smaller term.
    // Ignore this message, send back my current term and false.
    if rf.currentTerm > args.Term {
        detail := map[string]interface{}{
            "Term":    rf.currentTerm,
            "Success": false,
            "Reason":  "Ignore messages with smaller term",
        }
        rf.logRpc(args.CandidateId, rf.me, "REQUEST_VOTE REPLY", rf.currentTerm, args.RpcId, detail)
        return
    }

    // [raft-paper 5.4.1 the candidate's log is at least as up-to-date as the receiver's log]
    // Check if the candidate's log is at least as up-to-date as mine.
    upToDateCandidate := rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm)

    // 1. If I'm currently a leader or candidate with a smaller term, I've already voted for myself in this term.
    // Now I learn that there is a candidate with a higher term.
    // I need to update my term, become a follower, vote for this candidate, and reset my election timer.
    //
    // 2. If I'm currently a follower with a smaller term. I've probably voted for someone in this term.
    // I probably haven't voted for anyone.
    // In this case I just need to update my term, and vote for this candidate.
    if didUpdateTerm := rf.mayUpdateTerm(args.Term); didUpdateTerm {
        if upToDateCandidate {
            rf.votedFor = args.CandidateId
            reply.VoteGranted = true
            // follower learned a higher term, cast the vote
            detail = map[string]interface{}{
                "Term":    rf.currentTerm,
                "Success": true,
            }
            rf.logRpc(args.CandidateId, rf.me, "REQUEST_VOTE REPLY", rf.currentTerm, args.RpcId, detail)
        } else {
            // follower learned a higher term, but reject the vote due to out-of-date log
            detail = map[string]interface{}{
                "Term":    rf.currentTerm,
                "Success": false,
                "Reason":  "Out-of-data log",
            }
            rf.logRpc(args.CandidateId, rf.me, "REQUEST_VOTE REPLY", rf.currentTerm, args.RpcId, detail)
        }

        // currentTerm has been updated
        // votedFor has probably been updated
        rf.persist()
        return
    }

    // follower with the up-to-date term, but reject the vote due to out-of-date log
    if !upToDateCandidate {
        detail = map[string]interface{}{
            "Term":    rf.currentTerm,
            "Success": false,
            "Reason":  "Out-of-data log",
        }
        rf.logRpc(args.CandidateId, rf.me, "REQUEST_VOTE REPLY", rf.currentTerm, args.RpcId, detail)
        return
    }

    // Another case: I'm a follower with the same term as the candidate, but I've already voted for someone in this term.
    if rf.votedFor == -1 {
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true

        detail = map[string]interface{}{
            "Term":    rf.currentTerm,
            "Success": true,
        }
        rf.logRpc(args.CandidateId, rf.me, "REQUEST_VOTE REPLY", rf.currentTerm, args.RpcId, detail)
    } else {
        // I have already voted for someone in this term.
        detail := map[string]interface{}{
            "Term":    rf.currentTerm,
            "Success": false,
            "Reason":  "Voted",
        }
        rf.logRpc(args.CandidateId, rf.me, "REQUEST_VOTE REPLY", rf.currentTerm, args.RpcId, detail)
    }
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

// non-thread-safe
func (rf *Raft) isContextLost(expectedSeverState State, term int) bool {
    if rf.currentTerm > term {
        return true
    }

    if rf.state != expectedSeverState {
        return true
    }

    return false
}

func (rf *Raft) StartElection() {
    rf.mu.Lock()
    //rf.logLockUnlock(true)
    rf.currentTerm++

    rf.logStateChange(rf.state, Candidate, rf.currentTerm, "election timeout")
    rf.state = Candidate
    rf.votedFor = rf.me
    rf.persist()

    electionTerm := rf.currentTerm
    rf.mu.Unlock()
    //rf.logLockUnlock(false)

    voteGrantedCh := make(chan bool)

    // send RequestVote RPCs to all the other peers concurrently
    for i := range rf.peers {
        if i == rf.me {
            continue
        }

        peer := i
        go func() {
            voteGranted := rf.RequestVoteFrom(peer)
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
                //rf.logLockUnlock(true)
                // At this point, I expect myself to be a candidate. However, I may have become a follower or leader.
                // 1. If I have become a follower, which means I have learned a higher term, then I should stop the election.
                // 2. If I have become a leader, which means I have received enough votes. But this case seems never to happen.
                if rf.isContextLost(Candidate, electionTerm) {
                    // This round of election is no longer valid.
                    rf.mu.Unlock()
                    //rf.logLockUnlock(false)
                    break
                }

                if voteGranted {
                    votes++
                    // Just reach a majority of votes.
                    if votes > len(rf.peers)/2 {
                        // I have enough votes to become a leader.
                        // Stop the election ticker and start the heartbeat ticker.
                        // Break this "for select loop" to ignore the remaining votes.
                        rf.logStateChange(Candidate, Leader, rf.currentTerm, "elected as a leader")
                        rf.state = Leader
                        for i := range rf.peers {
                            rf.nextIndex[i] = rf.raftLog.GetActualSize()
                            rf.matchIndex[i] = 0
                        }
                        rf.matchIndex[rf.me] = rf.raftLog.GetActualLastIndex()

                        // I've received enough votes and become a leader. Terminate this goroutine to ignore the remaining votes.
                        rf.mu.Unlock()
                        //rf.logLockUnlock(false)
                        break
                    }
                }
                rf.mu.Unlock()
                //rf.logLockUnlock(false)
            }
        }
    }()
}

// sync
// If the peer grants my vote, return true. Otherwise return false.
func (rf *Raft) RequestVoteFrom(peer int) bool {
    rf.mu.Lock()
    //rf.logLockUnlock(true)
    args := RequestVoteArgs{
        Term:         rf.currentTerm,
        CandidateId:  rf.me,
        LastLogIndex: rf.raftLog.GetActualLastIndex(),
        LastLogTerm:  rf.raftLog.GetLastTerm(),
        RpcId:        rpcId.Add(1),
    }

    reply := RequestVoteReply{}

    detail := map[string]interface{}{
        "LastLogIdx":  args.LastLogIndex,
        "LastLogTerm": args.LastLogTerm,
    }
    rf.logRpc(rf.me, peer, "REQUEST_VOTE ARGS", rf.currentTerm, args.RpcId, detail)

    rf.mu.Unlock()
    //rf.logLockUnlock(false)

    if ok := rf.sendRequestVote(peer, &args, &reply); !ok {
        return false
    }

    detail = map[string]interface{}{
        "Term":    reply.Term,
        "Success": reply.VoteGranted,
    }
    rf.logRpc(rf.me, peer, "REQUEST_VOTE REPLY", rf.currentTerm, args.RpcId, detail)

    rf.mu.Lock()
    //rf.logLockUnlock(true)
    defer func() {
        rf.mu.Unlock()
        //rf.logLockUnlock(false)
    }()

    if didUpdate := rf.mayUpdateTerm(reply.Term); didUpdate {
        return false
    }

    return reply.VoteGranted
}

// non-thread-safe
// I'm the receiver of the RequestVote RPC.
// I want to check if the candidate is an eligible candidate.
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogIndex, candidateLastLogTerm int) bool {
    myLastLogIndex := rf.raftLog.GetActualLastIndex()
    myLastLogTerm := rf.raftLog.GetLastTerm()

    if candidateLastLogTerm > myLastLogTerm {
        return true
    } else if candidateLastLogTerm < myLastLogTerm {
        return false
    } else {
        return candidateLastLogIndex >= myLastLogIndex
    }
}
