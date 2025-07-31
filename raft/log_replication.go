package raft

import (
    "sort"
    "time"
)

type LogEntry struct {
    Term         int
    Command      interface{}
    CommandValid bool
}

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int

    // for debugging
    InvocationId uint32
}

type AppendEntriesReply struct {
    Term    int
    Success bool
    Check   bool

    // for debugging
    InvocationId uint32
}

// I could be a leader, candidate, or follower.
// Now I receive an AppendEntries RPC from a self-claimed leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    start := time.Now()
    LOG(dTime, "S%d, Term: %d, Starting AppendEntries, Sender: S%d, Invocation Id: %d, At: %v",
        rf.me, rf.currentTerm, args.LeaderId, args.InvocationId, start.UnixMilli())

    defer LOG(dTime, "S%d, Term: %d, Ending AppendEntries, Sender: S%d, Invocation Id: %d, Elapsed: %v",
        rf.me, rf.currentTerm, args.LeaderId, args.InvocationId, time.Since(start))

    rf.electionTicker.Reset(generateRandomTimeout())

    reply.Term = rf.currentTerm
    reply.Success = false
    reply.Check = true

    // I received a message from someone with a smaller term.
    // Ignore this message, send back my current term and false.
    if args.Term < rf.currentTerm {
        LOG(dLog2, "S%d, Term: %d, Ending AppendEntries, Sender: S%d, Invocation Id: %d, Invocation time: %v, "+
            "Failure, Reason: Ignore messages with smaller term",
            rf.me, rf.currentTerm, args.LeaderId, args.InvocationId, time.Since(start))
        return
    }

    if didUpdateTerm := rf.mayUpdateTerm(args.Term, args.LeaderId); !didUpdateTerm {
        // I received an AppendEntries RPC from someone with the same term.
        // What if I'm a candidate and I receive an AppendEntries RPC from a leader with the same term?
        // I should become a follower, reset my votedFor, and reset my election timer.
        rf.serverState = Follower
        rf.votedFor = -1

        rf.electionTicker.Reset(generateRandomTimeout())
    }

    //  Here I must be a follower with the most up-to-date term.

    // Check if log matches.
    // (a) my log is shorter than the leader's log
    if args.PrevLogIndex >= len(rf.log) {
        LOG(dLog2, "S%d, Term: %d, Ending AppendEntries, Sender: S%d, Invocation Id: %d, Invocation time: %v, "+
            "Failure, My log is shorter than the leader's log",
            rf.me, rf.currentTerm, args.LeaderId, args.InvocationId, time.Since(start))
        return
    }

    // (b) term does not match
    if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        LOG(dLog2, "S%d, Term: %d, Ending AppendEntries, Sender: S%d, Invocation Id: %d, Invocation time: %v, "+
            "Failure, Reason: Term does not match",
            rf.me, rf.currentTerm, args.LeaderId, args.InvocationId, time.Since(start))

        return
    }

    // Append entries to my log.
    if len(args.Entries) != 0 {
        LOG(dLog2, "S%d, Term: %d, Append %d Entries to my log: %v", rf.me, rf.currentTerm, len(args.Entries), args.Entries)
        rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
    }

    // Try to update commitIndex
    if args.LeaderCommit > rf.commitIndex {
        oldCommitIndex := rf.commitIndex
        if args.LeaderCommit > len(rf.log)-1 {
            rf.commitIndex = len(rf.log) - 1
        } else {
            rf.commitIndex = args.LeaderCommit
        }
        LOG(dLog2, "S%d, Term: %d, Commit Index: %d -> %d", rf.me, rf.currentTerm, oldCommitIndex, rf.commitIndex)

        // When commitIndex is updated, we need to apply log[lastApplied+1 : commitIndex] to the state machine
        go rf.Apply()
    }

    // Success
    reply.Success = true
    LOG(dLog2, "S%d, Term: %d, Ending AppendEntries, Sender: S%d, Invocation Id: %d, Invocation time %v, Success, Log: %v",
        rf.me, rf.currentTerm, args.LeaderId, args.InvocationId, time.Since(start), rf.log)
}

func (rf *Raft) findCommitIndex() int {
    LOG(dLog1, "S%d, Term: %d, Finding Commit Index, Match Index: %v", rf.me, rf.currentTerm, rf.matchIndex)
    tmpMatchIndex := make([]int, len(rf.matchIndex))
    copy(tmpMatchIndex, rf.matchIndex)
    sort.Ints(tmpMatchIndex)
    return tmpMatchIndex[(len(tmpMatchIndex)-1)/2]
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

// sync
// Now I'm a leader. I'm sending AppendEntries RPC to one peer.
func (rf *Raft) AppendEntriesTo(peer int) {
    start := time.Now()

    rf.mu.Lock()
    newInvocationId := appendEntriesInvocationId.Add(1)
    LOG(dLog1, "S%d, Term: %d, Starting Append EntriesTo to S%d, Invocation Id: %d, Log Length: %d, Commit Index: %d, PrevLogIndex: %d, NextIndex: %d, EntriesToSend: %v, Log: %v",
        rf.me, rf.currentTerm, peer, newInvocationId, len(rf.log), rf.commitIndex, rf.nextIndex[peer]-1, rf.nextIndex[peer], rf.log[rf.nextIndex[peer]:], rf.log)

    defer LOG(dLog1, "S%d, Term: %d, Ending AppendEntriesTo to S%d, InvocationId: %d, invocation time %v",
        rf.me, rf.currentTerm, peer, newInvocationId, time.Since(start))

    expectedTerm := rf.currentTerm

    args := AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        PrevLogIndex: rf.nextIndex[peer] - 1,
        PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
        Entries:      rf.log[rf.nextIndex[peer]:],
        LeaderCommit: rf.commitIndex,
        InvocationId: newInvocationId,
    }

    rf.mu.Unlock()

    reply := AppendEntriesReply{}

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if !reply.Check {
        LOG(dLog1, "S%d, Term: %d, Wrong invocation, Invocation Id: %d", rf.me, rf.currentTerm, newInvocationId)
        return
    }

    LOG(dLog1, "S%d, Term: %d, Reply of Append Entries to S%d, Reply: %v", rf.me, rf.currentTerm, peer, reply)

    if didUpdateTerm := rf.mayUpdateTerm(reply.Term, peer); didUpdateTerm {
        // I don't need to handle the reply anymore. Since I learned a higher term, and became a follower.
        // The RequestVote RPC I sent when I was a leader was meaningless.
        return
    }

    // I expect myself to be a leader here. However, it is possible that I am not a leader anymore.
    // how come? received a higher term from another peer.
    if rf.isContextLost(Leader, expectedTerm) {
        return
    }

    // Check reply.Success
    // how come reply.Success is false?
    // 1. The other peer has a higher term than me. We already handled this case above.
    //
    // 2. Log doesn't match.
    // (a) I'm a leader. My log is longer than the other peer's log.
    // (b) I'm a leader. The other peer's log is not shorter than mine, but its log at prevLogIndex has a different term than prevLogTerm.
    //
    // 3. Network failure
    // no need to decrement nextIndex
    if !reply.Success {
        // Only decrement
        rf.nextIndex[peer]--
        return
    }

    // reply.Success is true, which means replication to the other peer is successful.
    if rf.nextIndex[peer] == len(rf.log) {
        return
    }

    rf.nextIndex[peer] += len(args.Entries)
    rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)

    // We just updated matchIndex for peer. Maybe we can update commitIndex.
    newCommitIndex := rf.findCommitIndex()

    // it is possible that newCommitIndex = rf.commitIndex
    if newCommitIndex > rf.commitIndex {
        LOG(dLog1, "S%d, Term: %d, Update commit index %d -> %d", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
        rf.commitIndex = newCommitIndex
        go rf.Apply()
    }
}

// Now I'm a leader. I'm sending AppendEntries RPC to all other peers concurently.
func (rf *Raft) AppendEntriesToOthers() {
    for peer := range rf.peers {
        if peer == rf.me {
            continue
        }
        go rf.AppendEntriesTo(peer)
    }
}
