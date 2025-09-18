package raft

// Save both Raft state and K/V Snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    ps.raftstate = clone(state)
    ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return len(ps.snapshot)
}

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
// "index"
// if "index" > "commitIndex": try to Snapshot an index that is not committed yet.
// if "index" > "lastApplied": we haven't applied this index, so we do not know the state up to "index". We should apply it first.
// if "index"  <= "LastIncludedIndex": we try to Snapshot an index that has aleady been snapshotted.
//
// "Snapshot": the Snapshot up to "index". Opaque to raft. Provided by the service. (raft, kv service)
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    // Your code here (2D).

    rf.mu.Lock()
    defer rf.mu.Unlock()

    //if index > rf.commitIndex || index <= rf.LastIncludedIndex {
    //    return
    //}
    //
    //rf.LastIncludedIndex = index
    //rf.LastIncludedTerm = rf.log[index].Term
    //rf.Snapshot = Snapshot
    //
    //// Trim the existing log
    //newLog := make([]LogEntry, len(rf.log)-index) // len = 0, cap = len(rf.log) - index
    //newLog = append(newLog, rf.log[index+1:]...)
    //rf.log = newLog
}

type InstallSnapshotArgs struct {
    Term     int
    LeaderId int

    LastIncludedIndex int
    LastIncludedTerm  int

    Snapshot []byte
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
    }
    rf.mu.Unlock()
    reply := &InstallSnapshotReply{}

    ok := rf.sendInstallSnapshot(server, &args, reply)

    if !ok {
        return
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if didUpdateTerm := rf.mayUpdateTerm(reply.Term); didUpdateTerm {
        return
    }

    rf.nextIndex[server] = rf.raftLog.LastIncludedIndex + 1
    rf.matchIndex[server] = rf.raftLog.LastIncludedIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm

    if rf.currentTerm > args.Term {
        return
    }

    if didUpdateTerm := rf.mayUpdateTerm(args.Term); didUpdateTerm {
        //
    }

    rf.Snapshot(args.LastIncludedIndex, args.Snapshot)
}
