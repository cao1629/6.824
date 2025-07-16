package raft

import "log/slog"

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
    // Your code here (2A, 2B).

    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    // I received a message from someone with a smaller term.
    // Ignore this message, send back my current term and false.
    if rf.currentTerm > args.Term {
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
    if didUpdateTerm := rf.mayUpdateTerm(args.Term); didUpdateTerm {
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
        return
    }

    // Another case: I'm a follower with the same term as the candidate, but I've already voted for someone in this term.
    if rf.votedFor == -1 {
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
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
func (rf *Raft) isContextLost(term int) bool {
    if rf.currentTerm > term {
        return true
    }

    if rf.serverState != Candidate {
        return true
    }

    return false
}

// StartElection is running concurrently. There is a chance that the server loses the context. (no longer a candidate, a new term)
//
// no longer a candidate, and a new term
// 1. become a leader: an election with a higher term won the election
// 2. become a follower: a heartbeat from a leader with a higher term
//
// no longer a candidate, but the term is still the same
// 1. become a follower: send RequestVote RPCs to a leader with the same term
// 2. become a leader: this candidate has already received enough votes to become a leader. It does not need to process
// more votes from other servers with the same term.
//
func (rf *Raft) StartElection() {
    rf.mu.Lock()
    rf.currentTerm++
    rf.serverState = Candidate
    rf.votedFor = rf.me
    rf.persist()
    electionTerm := rf.currentTerm
    rf.mu.Unlock()

    slog.Info("StartElection", "server", rf.me, "term", rf.currentTerm)

    voteGrantedCh := make(chan bool)

    // send RequestVote RPCs to all the other peers concurrently
    for peer := range rf.peers {
        if peer == rf.me {
            continue
        }

        go func() {
            // Before sending RequestVote RPC, there is a chance that I'm no longer a candidate.
            voteGrantedCh <- rf.RequestVoteFrom(peer)
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
                if rf.isContextLost(electionTerm) {
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
                        rf.serverState = Leader
                        rf.electionTicker.Stop()
                        rf.heartbeatTicker.Reset(heartbeatInterval)
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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    slog.Info("Requesting vote from peer", "server", rf.me, "peer", peer, "term", rf.currentTerm)
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

    if didUpdate := rf.mayUpdateTerm(reply.Term); didUpdate {
        return false
    }

    slog.Info("Received vote reply", "server", rf.me, "peer", peer, "term", reply.Term, "vote_granted", reply.VoteGranted)
    return reply.VoteGranted
}

// I'm the receiver of the RequestVote RPC.
// I check if the candidate is an eligible candidate.
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
