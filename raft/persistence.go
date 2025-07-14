package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

// 用来模拟 disk  non-volatile的东西不存到disk上  存到Persister里面
// when a Raft peer crashes, it loses its volatile state, but retains its
// persistent state in Persister.
//
// When a Raft peer is restarted, it can recover its persistent state
// from the Persister.
//
// 每一个 Raft peer 都有一个 Persister
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

// move in c++
func (ps *Persister) Copy() *Persister {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    np := MakePersister()
    np.raftstate = ps.raftstate
    np.snapshot = ps.snapshot
    return np
}

func (ps *Persister) SaveRaftState(state []byte) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    ps.raftstate = clone(state)
}

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
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here (2C).
    // Example:
    // w := new(bytes.Buffer)
    // e := labgob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    // Your code here (2D).

}
