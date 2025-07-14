package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
    //	"bytes"
    "sync"
    "sync/atomic"
    "time"

    //	"6.824/labgob"
    "6.824/labrpc"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    // Lock to protect shared access to this peer's state
    mu sync.Mutex

    // RPC end points of all peers
    peers []*labrpc.ClientEnd

    // Object to hold this peer's persisted state
    persister *Persister

    // this peer's index into peers[]
    me int

    // set by Kill()
    dead int32

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    currentTerm int
    votedFor    int // -1 means no vote
    serverState ServerState

    // log
    log        []LogEntry
    nextIndex  []int
    matchIndex []int

    commitIndex int
    lastApplied int

    // Control channels
    // 从candidate变成了leader
    electedCh chan struct{}

    // 可以从leader变成follower 也可以从candidate变成follower
    becomeFollowerCh chan ServerState
    killCh           chan struct{}
    resetTimeoutCh   chan struct{}

    applyCh    chan ApplyMsg
    logApplier *LogApplier
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    var term int
    var isleader bool
    // Your code here (2A).
    return term, isleader
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    // Your code here (2B).
    if rf.serverState != Leader {
        return 0, 0, false
    }

    rf.log = append(rf.log, LogEntry{
        Term:         rf.currentTerm,
        Command:      command,
        CommandValid: true,
    })

    return len(rf.log) - 1, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
// Simulate a server's death.
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
    // Stop the election ticker when killed

}

func (rf *Raft) killed() bool {
    z := atomic.LoadInt32(&rf.dead)
    return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    rf.applyCh = applyCh
    rf.logApplier = NewLogApplier(applyCh)

    // Your initialization code here (2A, 2B, 2C).
    go func() {
        // heartbeatTicker在leader时运行
        heartbeatTicker := time.NewTicker(heartbeatInterval)
        defer heartbeatTicker.Stop()

        // electionTicker在follower和candidate时运行
        electionTicker := time.NewTicker(generateRandomTimeout())
        defer electionTicker.Stop()

        for {
            select {
            case <-heartbeatTicker.C:
                go rf.InitiateElection()
            case <-electionTicker.C:
                go rf.InitiateAppendEntries()
            case <-rf.electedCh:
                // 在rf.InitiateElection中会 rf.electedCh <- struct{}{}
                // 现在我从candidate变成了leader了
                electionTicker.Stop()
                heartbeatTicker = time.NewTicker(heartbeatInterval)
            case previousServerState := <-rf.becomeFollowerCh:
                // 在maybeUpdateTerm中会 rf.becomeFollowerCh <- struct{}{}

                // 如果是从candidate变成follower 本来heartbeatTicker就已经停掉了
                if previousServerState == Leader {
                    heartbeatTicker.Stop()
                }

                electionTicker = time.NewTicker(generateRandomTimeout())
            case <-rf.resetTimeoutCh:
                // follower收到leader的heartbeat就reset electionTicker
                electionTicker.Reset(generateRandomTimeout())
            case <-rf.killCh:
                return
            }
        }
    }()

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    return rf
}

// When a server learns of a higher term, it should update its term.
// A server state change may happen.
// Returns true if the term was updated, false otherwise.
func (rf *Raft) maybeUpdateTerm(term int) bool {
    if term > rf.currentTerm {
        rf.currentTerm = term

        if rf.serverState != Follower {
            rf.changeToFollower(rf.serverState)
        }

        return true
    }
    return false
}
