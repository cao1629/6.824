package raft

// Save both Raft state and K/V snapshot as a single atomic action,
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

    // Your code here (2D).

    return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 
// "index"
// if "index" > "commitIndex": try to snapshot an index that is not committed yet.
// if "index" > "lastApplied": we haven't applied this index, so we do not know the state up to "index". We should apply it first.
// if "index"  <= "lastIncludedIndex": we try to snapshot an index that has aleady been snapshotted.
//
// "snapshot": the snapshot up to "index". Opaque to raft. Provided by the service. (raft, kv service)
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    // Your code here (2D).

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if index > rf.commitIndex || index <= rf.lastIncludedIndex {
        return
    }

    rf.lastIncludedIndex = index
    rf.lastIncludedTerm = rf.log[index].Term
    rf.snapshot = snapshot

    // Trim the existing log
    newLog := make([]LogEntry, len(rf.log)-index) // len = 0, cap = len(rf.log) - index
    newLog = append(newLog, rf.log[index+1:]...)
    rf.log = newLog
}
