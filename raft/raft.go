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

    electionTicker *ElectionTicker

    applyCh chan ApplyMsg
    killCh  chan struct{}

    // data members for debugging

    // If I'm a leader, record the moment I call AppendEntriesToOthers
    // If I'm not a leader, record the moment I call AppendEntries
    heartbeatClock time.Time
    electionClock  time.Time
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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.serverState != Leader {
        LOG(dLog1, "S%d, Term: %d, Submit a command on a server which is not a leader", rf.me, rf.currentTerm)
        return 0, 0, false
    }

    rf.log = append(rf.log, LogEntry{
        Term:         rf.currentTerm,
        Command:      command,
        CommandValid: true,
    })

    rf.matchIndex[rf.me] = len(rf.log) - 1

    rf.persist()

    LOG(dLog1, "S%d, Term: %d, Submit command %v", rf.me, rf.currentTerm, command)
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
//
// Permantently shut down the server
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
    // Stop the election ticker when killed
    //rf.killCh <- struct{}{}
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

    LOG(dInfo, "S%d, Starting...", me)

    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me
    rf.currentTerm = 1

    rf.serverState = Follower

    rf.heartbeatTicker = time.NewTicker(heartbeatInterval)
    rf.heartbeatTicker.Stop()

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    rf.lastApplied = 0

    rf.log = []LogEntry{
        {0, 0, false},
    }

    rf.electionTicker = NewElectionTicker()
    rf.heartbeatClock = time.Now()
    rf.applyCh = applyCh

    // Your initialization code here (2A, 2B, 2C).
    go func() {
        rf.electionClock = time.Now()
        rf.electionTicker.Reset(generateRandomTimeout())
        for {
            select {
            case <-rf.heartbeatTicker.C:
                LOG(dTicker, "S%d, Term: %d, Heartbeat Timeout, elapse: %v", rf.me, rf.currentTerm, time.Since(rf.heartbeatClock))
                rf.heartbeatClock = time.Now()
                rf.AppendEntriesToOthers()

            case <-rf.electionTicker.C:
                LOG(dElection, "S%d, Term: %d, Election Timeout, elapse: %v", rf.me, rf.currentTerm, time.Since(rf.electionClock))
                go rf.StartElection()

            case <-rf.killCh:
                // When killed, this server will stop after its current work is done.
                return
            }
        }
    }()

    // initialize from state persisted before a crash
    rf.readPersist(rf.persister.ReadRaftState())

    return rf
}

// non-thread-safe
// I could be a leader, a candidate, or a follower.
// If I learn a higher term, I update my term. If I'm not a follower, I become a follower.
// If I'm currently a leader, I stop my heartbeat ticker.
// If I'm currently a candidate, I reset the election ticker.
func (rf *Raft) mayUpdateTerm(term int, from int) bool {
    if term > rf.currentTerm {
        oldTerm := rf.currentTerm
        oldState := rf.serverState

        rf.currentTerm = term

        if rf.serverState == Leader {
            rf.heartbeatTicker.Stop()
        }

        rf.electionClock = time.Now()
        rf.electionTicker.Reset(generateRandomTimeout())

        rf.serverState = Follower

        LOG(dState, "S%d, Term: %d -> %d, State: %s -> %s, Reason: learned a higher term from S%d",
            rf.me, oldTerm, rf.currentTerm, oldState, rf.serverState, from)

        rf.votedFor = -1

        rf.persist()
        return true
    }

    return false
}

type EntryInfo struct {
    index int
    term  int
}

// non-thread-safe
func (rf *Raft) getEntriesToSend(peer int) []EntryInfo {
    entriesInfo := make([]EntryInfo, len(rf.log)-rf.nextIndex[peer])
    for i := rf.nextIndex[peer]; i < len(rf.log); i++ {
        entriesInfo[i-rf.nextIndex[peer]] = EntryInfo{
            index: i,
            term:  rf.log[i].Term,
        }
    }
    return entriesInfo
}

func (rf *Raft) getLogInfo() []EntryInfo {
    entriesInfo := make([]EntryInfo, len(rf.log))
    for i, entry := range rf.log {
        entriesInfo[i] = EntryInfo{
            index: i,
            term:  entry.Term,
        }
    }
    return entriesInfo
}

type ServerState string

const (
    Leader    ServerState = "leader"
    Follower  ServerState = "follower"
    Candidate ServerState = "candidate"
)
