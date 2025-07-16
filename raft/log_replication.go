package raft

import (
    "log/slog"
    "sort"
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
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

// I could be a leader, candidate, or follower.
// Now I receive an AppendEntries RPC from a self-claimed leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

    reply.Term = rf.currentTerm
    reply.Success = false

    // I received a message from someone with a smaller term.
    // Ignore this message, send back my current term and false.
    if args.Term < rf.currentTerm {
        slog.Info("receiver of AppendEntries: ignore messages with a smaller term", "me", rf.me, "my_term", rf.currentTerm, "sender_term", args.Term)
        return
    }

    if didUpdateTerm := rf.mayUpdateTerm(args.Term); !didUpdateTerm {
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
        slog.Info("AppendEntries: my log is shorter than the leader's log", "me", rf.me, "leader", args.LeaderId)
        return
    }

    // (b) term does not match
    if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        slog.Info("AppendEntries: term does not match", "me", rf.me, "leader", args.LeaderId)
        return
    }

    // Append entries to my log.
    if len(args.Entries) == 0 {
        rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
    }

    // Success
    reply.Success = true
}

func (rf *Raft) findCommitIndex() int {
    tmpMatchIndex := make([]int, len(rf.matchIndex))
    copy(tmpMatchIndex, rf.matchIndex)
    sort.Ints(tmpMatchIndex)
    // index是:0 1 2 3 4 5  中位数的索引是(len-1)/2
    return tmpMatchIndex[(len(tmpMatchIndex)-1)/2]
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

// sync
// Now I'm a leader. I'm sending AppendEntries RPC to one peer.
func (rf *Raft) AppendEntriesTo(peer int) {
    slog.Info("AppendEntriesTo", "me", rf.me, "to", peer, "term", rf.currentTerm, "log.len", len(rf.log), "nextIndex", rf.nextIndex[peer])

    args := AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        PrevLogIndex: rf.nextIndex[peer] - 1,
        PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
        Entries:      rf.log[rf.nextIndex[peer]:],
    }

    reply := AppendEntriesReply{}

    rf.sendAppendEntries(peer, &args, &reply)

    if didUpdateTerm := rf.mayUpdateTerm(reply.Term); didUpdateTerm {
        // I don't need to handle the reply anymore. Since I learned a higher term, and became a follower.
        // The RequestVote RPC I sent when I was a leader was meaningless.
        return
    }

    // Check reply.Success
    // how come reply.Success is false?
    // 1. The other peer has a higher term than me. We already handled this case above.
    //
    // 2. Log doesn't match.
    // (a) I'm a leader. My log is longer than the other peer's log.
    // (b) I'm a leader. The other peer's log is not shorter than mine, but its log at prevLogIndex has a different term than prevLogTerm.
    // I need to decrease nextIndex for this peer.
    if !reply.Success {
        slog.Info("reply.Success is false")
        if rf.nextIndex[peer] != len(rf.log) {
            rf.nextIndex[peer]--
        }

        return
    }

    // reply.Success is true, which means replication to the other peer is successful.
    if rf.nextIndex[peer] == len(rf.log) {
        return
    }

    rf.nextIndex[peer]++
    rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) - 1
    newCommitIndex := rf.findCommitIndex()
    if newCommitIndex > rf.commitIndex {
        rf.commitIndex = newCommitIndex
        rf.logApplier.applySignalCh <- struct{}{}
    }
}

// Now I'm a leader. I'm sending AppendEntries RPC to all other peers concurently.
func (rf *Raft) AppendEntriesToOthers() {
    slog.Info("AppendEntriesToOthers", "leader", rf.me, "term", rf.currentTerm, "state", rf.serverState)
    for peer := range rf.peers {
        if peer == rf.me {
            continue
        }
        go rf.AppendEntriesTo(peer)

    }
}
