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
    "log/slog"
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

    // in the current term, I voted for whom.
    // -1 means no vote
    votedFor int

    serverState ServerState

    // log
    log        []LogEntry
    nextIndex  []int
    matchIndex []int

    commitIndex int
    lastApplied int

    // two tickers
    heartbeatTicker *time.Ticker
    electionTicker  *time.Ticker

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

    return rf.currentTerm, rf.serverState == Leader
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

    rf.persist()

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

    slog.Info("Make", "server", me, "peers_count", len(peers))

    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    rf.serverState = Follower
    rf.electionTicker = time.NewTicker(generateRandomTimeout())
    rf.heartbeatTicker = time.NewTicker(heartbeatInterval)
    rf.heartbeatTicker.Stop()

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    rf.log = make([]LogEntry, 1)

    rf.applyCh = applyCh
    rf.logApplier = NewLogApplier(applyCh)

    // Your initialization code here (2A, 2B, 2C).
    go func() {
        slog.Info("Raft server started", "server", me)

        for {
            select {
            case <-rf.heartbeatTicker.C:
                // How do I make sure that this ticker ticks only when I am a leader?
                // Once I'm not a leader, I stop this ticker atomically.
                slog.Info("Heartbeat ticks", "server", me, "term", rf.currentTerm, "state", rf.serverState)
                rf.AppendEntriesToOthers()

            case <-rf.electionTicker.C:
                // How to make sure that I am not a leader when receiving this tick?
                // equivalent to: after I become a leader, I stop the election ticker atomically
                slog.Info("Election ticks", "server", me, "term", rf.currentTerm, "state", rf.serverState)
                go rf.StartElection()

            case <-rf.killCh:
                slog.Info("Raft server ticker goroutine shutting down", "struct", "Raft", "method", "Make", "server", me)
                return
            }
        }
    }()

    // initialize from state persisted before a crash
    slog.Info("Reading persisted state", "struct", "Raft", "method", "Make", "server", me)
    rf.readPersist(rf.persister.ReadRaftState())

    slog.Info("Raft server initialization complete", "struct", "Raft", "method", "Make", "server", me, "term", rf.currentTerm, "state", rf.serverState)
    return rf
}

// non-thread-safe
// I could be a leader, a candidate, or a follower.
// If I learn a higher term, I update my term. If I'm not a follower, I become a follower.
// If I'm currently a leader, I stop my heartbeat ticker.
// If I'm currently a candidate, I reset the election ticker.
func (rf *Raft) mayUpdateTerm(term int) bool {

    if term > rf.currentTerm {
        rf.currentTerm = term

        if rf.serverState == Leader {
            rf.heartbeatTicker.Stop()
        } else if rf.serverState == Candidate {
            rf.electionTicker.Reset(generateRandomTimeout())
        }

        rf.serverState = Follower
        rf.votedFor = -1

        rf.persist()

        return true
    }
    return false
}

type ServerState int

const (
    Leader    ServerState = iota // 0
    Follower                     // 1
    Candidate                    // 2
)
