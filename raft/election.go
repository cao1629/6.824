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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).

    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    // 直接ignore
    if rf.currentTerm > args.Term {
        return
    }

    // [raft-paper 5.4.1 3 the candidate's log is at least as up-to-date as the receiver's log]
    upToDateCandidate := rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm)

    // 我现在可能是leader, candidate, or follower with a smaller term
    // 我是leader with a smaller term, 我现在的voteFor就是我自己
    // 我的candidate with a smaller term, 我现在的voteFor也是我自己
    // 我是follower with a smaller term 我现在的voteFor是一个leader with a smaller term  或者voteFor是-1
    // 如果我确实update term了 那么现在我一定vote for 这个candidate with a higher term
    if didUpdate := rf.maybeUpdateTerm(args.Term); didUpdate {

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

    // 还有一种情况 就是我的term 跟candidate的term一样 但是这一个term里面 我已经投过票了
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

func (rf *Raft) isContextLost(term int) bool {
    if rf.currentTerm > term {
        return true
    }

    if rf.serverState != Candidate {
        return true
    }

    return false
}

// InitiateElection is running concurrently. There is a chance that the server loses the context. (no longer a candidate, a new term)
//
// no longer a candidate, and a new term
// 1. become a leader: an election with a higher term won the election
// 2. become a follower: a heartbeat from a leader with a higher term
//
// no longer a candidate, but the term is still the same
// 1. become a follower: send RequestVote RPCs to a leader with the same term
// 2. become a leader: this candidate has already received enough votes to become a leader. It does not need to process
// more votes from other servers with the same term.
func (rf *Raft) InitiateElection() {
    rf.currentTerm++
    rf.changeToCandidate()
    rf.persist()

    votes := 1
    for i := range rf.peers {
        if i == rf.me {
            continue
        }

        peer := i
        go func() {
            if ok := rf.askForVote(peer); ok {
                votes++
                if votes > len(rf.peers)/2 {
                    rf.changeToLeader()
                }
            }
        }()
    }
}

func (rf *Raft) askForVote(peer int) bool {
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

    if didUpdate := rf.maybeUpdateTerm(reply.Term); didUpdate {
        // currentTerm has been updated
        rf.persist()
        return false
    }

    return reply.VoteGranted
}

// I'm the receiver of the RequestVote RPC.
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
