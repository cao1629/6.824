package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term        int
    CandidateId int
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
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }

    rf.maybeUpdateTerm(args.Term)

    if rf.votedFor == -1 {
        rf.votedFor = args.CandidateId
    }

    reply.Term = rf.currentTerm
    if rf.votedFor == args.CandidateId {
        reply.VoteGranted = true
    } else {
        reply.VoteGranted = false
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
        Term:        rf.currentTerm,
        CandidateId: rf.me,
    }

    reply := RequestVoteReply{}
    if ok := rf.sendRequestVote(peer, &args, &reply); !ok {
        return false
    }

    if didUpdate := rf.maybeUpdateTerm(reply.Term); didUpdate {
        return false
    }

    return reply.VoteGranted
}
