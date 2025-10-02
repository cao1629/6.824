package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
    "bytes"
    "sync"

    "6.824/labgob"
)

// Simulate disk storage. Each Raft server has a Persister.
// When a Raft server crashes, it loses its volatile states but things in its Persister.
// When this Raft server is back, it recovers its persistent state from its Persister.
type Persister struct {
    mu        sync.Mutex
    raftstate []byte
    snapshot  []byte
}

func MakePersister() *Persister {
    return &Persister{}
}

func clone(orig []byte) []byte {
    x := make([]byte, len(orig))
    copy(x, orig)
    return x
}

func (ps *Persister) Copy() *Persister {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    np := MakePersister()
    np.raftstate = ps.raftstate
    np.snapshot = ps.snapshot
    return np
}

// persist() -> []byte -> SaveRaftState() copys the []byte into persister.raftstate
func (ps *Persister) SaveRaftState(state []byte) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    ps.raftstate = clone(state)
}

// persister.raftstate -> []byte -> ReadPesist() reads the []byte and decodes it into raft server
func (ps *Persister) ReadRaftState() []byte {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return len(ps.raftstate)
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
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

// Save both Raft state and K/V Snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    ps.raftstate = clone(state)
    ps.snapshot = clone(snapshot)
}

func (rf *Raft) EncodeRaftState() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.raftLog.TailLog)
    data := w.Bytes()
    return data
}

func (rf *Raft) DecodeRaftState(data []byte) {
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.raftLog.TailLog)
}

//func (rf *Raft) DecodeSnapshot(data []byte) {
//    r := bytes.NewBuffer(data)
//    d := labgob.NewDecoder(r)
//
//    d.Decode(&rf.raftLog.Snapshot)
//}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// raft server -> encode -> []byte -> persister
func (rf *Raft) persist() {
    // Your code here (2C).
    // Example:
    // raftStateBytes := new(bytes.Buffer)
    // e := labgob.NewEncoder(raftStateBytes)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := raftStateBytes.Bytes()
    // rf.persister.SaveRaftState(data)

    raftState := rf.EncodeRaftState()
    rf.persister.SaveStateAndSnapshot(raftState, rf.raftLog.Snapshot)
}

//
// restore previously persisted state.
//
// []byte -> raft peer
func (rf *Raft) readPersist(state []byte, snapshot []byte) {
    //if data == nil || len(data) < 1 { // bootstrap without any state?
    //    return
    //}
    // Your code here (2C).
    // Example:
    // r := bytes.NewBuffer(data)
    // d := labgob.NewDecoder(r)
    // var xxx
    // var yyy
    // if d.Decode(&xxx) != nil ||
    //    d.Decode(&yyy) != nil {
    //   error...
    // } else {
    //   rf.xxx = xxx
    //   rf.yyy = yyy
    // }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.DecodeRaftState(state)
    rf.raftLog.Snapshot = snapshot

    r := bytes.NewBuffer(snapshot)
    d := labgob.NewDecoder(r)

    d.Decode(&rf.raftLog.LastIncludedIndex)

    lastIncludedTerm := rf.raftLog.TailLog[0].Term
    rf.raftLog.LastIncludedTerm = lastIncludedTerm

    if rf.raftLog.LastIncludedIndex > 0 {
        rf.commitIndex = rf.raftLog.LastIncludedIndex
        rf.lastApplied = rf.raftLog.LastIncludedIndex
    }

}
