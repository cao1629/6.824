package raft

//
// A service wants to switch to Snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the Snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

    // Your code here (2D).

    return true
}

// the service says it has created a Snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 
// case 1: this Snapshot() call is invoked by InstallSnapshot RPC.
// case 2: this Snaphshot() call is passed from the app layer.
//
// "Snapshot": the Snapshot up to "index". Opaque to raft. Provided by the service. (raft, kv service)
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    // Your code here (2D).

    rf.mu.Lock()
    rf.logger.Printf("snapshot at index %d, log = %v\n", index, rf.raftLog.TailLog)

    if index > rf.commitIndex || index <= rf.raftLog.LastIncludedIndex {
        rf.logger.Printf("XXXXXXXXXXXXX")
        return
    }

    rf.raftLog.LastIncludedTerm = rf.raftLog.GetTermAt(index)

    // Trim the existing log
    // actual last index - index = numbers of entries to keep
    // +1: dummy head
    newLog := make([]LogEntry, 0, rf.raftLog.GetActualLastIndex()-index+1)
    newLog = append(newLog, LogEntry{rf.raftLog.LastIncludedTerm, 0, true})
    newLog = append(newLog, rf.raftLog.GetEntries(index+1, rf.raftLog.GetActualLastIndex())...)

    rf.raftLog.LastIncludedIndex = index
    rf.raftLog.Snapshot = snapshot
    rf.raftLog.TailLog = newLog

    rf.persist()
    rf.pendingSnapshot = true
    rf.mu.Unlock()
    rf.applyCond.Signal()

}

type InstallSnapshotArgs struct {
    Term     int
    LeaderId int

    LastIncludedIndex int
    LastIncludedTerm  int

    Snapshot []byte

    RpcId uint32
}

type InstallSnapshotReply struct {
    Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    return ok
}

// when prevLogIndex is LastIncludedIndex
// if we have
// [1] prevLogTerm != server.prevLogTerm
// [2] server.actualLastIndex < prevLogIndex
// then we should send InstallSnapshot RPC
func (rf *Raft) InstallSnapshotOn(server int) {
    rf.mu.Lock()
    args := InstallSnapshotArgs{
        Term:              rf.currentTerm,
        LeaderId:          rf.me,
        LastIncludedIndex: rf.raftLog.LastIncludedIndex,
        LastIncludedTerm:  rf.raftLog.LastIncludedTerm,
        Snapshot:          clone(rf.raftLog.Snapshot),
        RpcId:             rpcId.Add(1),
    }

    detail := map[string]interface{}{
        "LastIncludedIndex": args.LastIncludedIndex,
        "LastIncludedTerm":  args.LastIncludedTerm,
        "Log":               rf.raftLog.TailLog,
    }

    rf.logRpc(rf.me, server, "INSTALL_SNAPSHOT ARGS", rf.currentTerm, args.RpcId, detail)

    rf.mu.Unlock()
    reply := &InstallSnapshotReply{}

    if ok := rf.sendInstallSnapshot(server, &args, reply); !ok {
        detail = map[string]interface{}{
            "Success": false,
            "Reason":  "Timeout",
        }

        rf.logRpc(rf.me, server, "INSTALL_SNAPSHOT REPLY", rf.currentTerm, args.RpcId, detail)

        return
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if didUpdateTerm := rf.mayUpdateTerm(reply.Term); didUpdateTerm {
        detail = map[string]interface{}{
            "Term":   reply.Term,
            "Reason": "Update current term",
        }
        rf.logRpc(rf.me, server, "INSTALL_SNAPSHOT REPLY", rf.currentTerm, args.RpcId, detail)
        return
    }

    rf.nextIndex[server] = rf.raftLog.LastIncludedIndex + 1
    rf.matchIndex[server] = rf.raftLog.LastIncludedIndex

    // We just updated matchIndex for server. Maybe we can update commitIndex.
    newCommitIndex := rf.findCommitIndex()

    // it is possible that newCommitIndex = rf.commitIndex
    if newCommitIndex > rf.commitIndex {
        rf.logCommitIndexUpdate(rf.commitIndex, newCommitIndex)
        rf.commitIndex = newCommitIndex
        rf.applyCond.Signal()
    }

    detail = map[string]interface{}{
        "NextIndex":  rf.nextIndex[server],
        "MatchIndex": rf.matchIndex[server],
    }
    rf.logRpc(rf.me, server, "INSTALL_SNAPSHOT REPLY", rf.currentTerm, args.RpcId, detail)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()

    detail := map[string]interface{}{
        "LastIncludedIndex": args.LastIncludedIndex,
        "LastIncludedTerm":  args.LastIncludedTerm,
    }

    rf.logRpc(args.LeaderId, rf.me, "INSTALL_SNAPSHOT ARGS", rf.currentTerm, args.RpcId, detail)

    reply.Term = rf.currentTerm

    if rf.currentTerm > args.Term {
        rf.mu.Unlock()
        return
    }

    if didUpdateTerm := rf.mayUpdateTerm(args.Term); didUpdateTerm {

    }

    rf.raftLog.Snapshot = clone(args.Snapshot)
    rf.pendingSnapshot = true

    if args.LastIncludedIndex > rf.commitIndex {
        rf.commitIndex = args.LastIncludedIndex
        rf.raftLog.LastIncludedIndex = args.LastIncludedIndex
        rf.raftLog.SetTermAtZero(args.LastIncludedTerm)

        // Remove all logs coming before LastIncludedIndex
        newTailLog := append(rf.raftLog.TailLog[:1])
        rf.raftLog.TailLog = newTailLog

        rf.applyCond.Signal()
    }

    rf.logRpc(args.LeaderId, rf.me, "INSTALL_SNAPSHOT REPLY", rf.currentTerm, args.RpcId, map[string]interface{}{})

    rf.mu.Unlock()

}
