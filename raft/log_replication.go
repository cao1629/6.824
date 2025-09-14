package raft

import (
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

	// for debugging
	RpcId uint32
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for debugging
	RpcId uint32
}

// I could be a leader, candidate, or follower.
// Now I receive an AppendEntries RPC from a self-claimed leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.electionTicker.Reset(generateRandomTimeout())

	reply.Term = rf.currentTerm
	reply.Success = false

    detail := map[string]interface{}{
        "PrevLogIndex": args.PrevLogIndex,
        "PrevLogTerm":  args.PrevLogTerm,
        "Entries":      args.Entries,
        "LeaderCommit": args.LeaderCommit,
    }

    rf.logRpc(args.LeaderId, rf.me, "APPEND_ENTRIES ARGS", rf.currentTerm, args.RpcId, detail);



	// I received a message from someone with a smaller term.
	// Ignore this message, send back my current term and false.
	if args.Term < rf.currentTerm {
		detail = map[string]interface{}{
			"Term":    rf.currentTerm,
			"Success": false,
			"Reason":  "Ignore",
		}

		rf.logRpc(args.LeaderId, rf.me, "APPEND_ENTRIES REPLY", rf.currentTerm, args.RpcId, detail)
		return
	}

	if didUpdateTerm := rf.mayUpdateTerm(args.Term, args.LeaderId); !didUpdateTerm {
		// I received an AppendEntries RPC from someone with the same term.
		// What if I'm a candidate and I receive an AppendEntries RPC from a leader with the same term?
		// I should become a follower, reset my votedFor, and reset my election timer.
		rf.state = Follower
		rf.votedFor = -1

		rf.electionTicker.Reset(generateRandomTimeout())
	}

	//  Here I must be a follower with the most up-to-date term.

	// Check if log matches.
	// (a) my log is shorter than the leader's log
	if args.PrevLogIndex >= len(rf.log) {

		detail := map[string]interface{}{
			"Term":    rf.currentTerm,
			"Success": false,
			"Reason":  "Short log",
		}
		rf.logRpc(args.LeaderId, rf.me, "APPEND_ENTRIES REPLY", rf.currentTerm, args.RpcId, detail)
		return
	}

	// (b) term does not match
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		detail := map[string]interface{}{
			"Term":    rf.currentTerm,
			"Success": false,
			"Reason":  "Wrong term",
		}

		rf.logRpc(args.LeaderId, rf.me, "APPEND_ENTRIES REPLY", rf.currentTerm, args.RpcId, detail)

		return
	}

	// Append entries to my log.
	if len(args.Entries) != 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

        // detail := 
	}

	// Try to update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
            rf.logCommitIndexUpdate(rf.commitIndex, len(rf.log)-1)
			rf.commitIndex = len(rf.log) - 1
		} else {
            rf.logCommitIndexUpdate(rf.commitIndex, args.LeaderCommit)
			rf.commitIndex = args.LeaderCommit
		}

		// When commitIndex is updated, we need to apply log[lastApplied+1 : commitIndex] to the state machine
		go rf.Apply()
	}

	// Success
	reply.Success = true
	detail = map[string]interface{}{
		"Term":    rf.currentTerm,
		"Success": true,
		"Log":     rf.log,
        "CommitIdx": rf.commitIndex,
	}
	rf.logRpc(args.LeaderId, rf.me, "APPEND_ENTRIES REPLY", rf.currentTerm, args.RpcId, detail)
}

func (rf *Raft) findCommitIndex() int {
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

	rf.mu.Lock()
	expectedTerm := rf.currentTerm

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
		Entries:      rf.log[rf.nextIndex[peer]:],
		LeaderCommit: rf.commitIndex,
		RpcId:        rpcId.Add(1),
	}

	detail := map[string]interface{}{
		"LogLen":      len(rf.log),
        "Log":        rf.log,
		"CommitIdx":   rf.commitIndex,
        "MatchIndex": rf.matchIndex,
		"PrevLogIdx":  args.PrevLogIndex,
		"PrevLogTerm": args.PrevLogTerm,
		"NextIdx":     rf.nextIndex[peer],
		"ToSend":      rf.log[rf.nextIndex[peer]:],
	}
	rf.logRpc(rf.me, peer, "APPEND_ENTRIES ARGS", rf.currentTerm, args.RpcId, detail)

	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	// When messages get lost or the server is down, labrpc will stimulate a timeout and return a false reply.
	// We just ignore the reply.
	if ok := rf.sendAppendEntries(peer, &args, &reply); !ok {
		return
	}

	detail = map[string]interface{}{
		"Term":    reply.Term,
		"Success": reply.Success,
	}
	rf.logRpc(rf.me, peer, "APPEND_ENTRIES REPLY", rf.currentTerm, args.RpcId, detail)

	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	// (a) I'm a leader. My log is longer than the other peer's log. prevLogIndex doesn't make sense.
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
    // maybe other peer's log is already up-to-date. 
	// if rf.nextIndex[peer] == len(rf.log) {
	// 	return
	// }

	rf.nextIndex[peer] += len(args.Entries)
	rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)

	// We just updated matchIndex for peer. Maybe we can update commitIndex.
	newCommitIndex := rf.findCommitIndex()

	// it is possible that newCommitIndex = rf.commitIndex
	if newCommitIndex > rf.commitIndex {
        rf.logCommitIndexUpdate(rf.commitIndex, newCommitIndex)
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
