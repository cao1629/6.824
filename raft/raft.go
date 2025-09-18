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
    "fmt"
    "log"

    //	"bytes"
    "os"
    "sync"
    "sync/atomic"
    "time"

    "6.824/labrpc"
)

// A Go object implementing a single Raft peer.
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

    state State

    // log
    log        []LogEntry
    nextIndex  []int
    matchIndex []int

    // snapshot
    lastIncludedIndex int
    lastIncludedTerm  int
    snapshot          []byte

    commitIndex int
    lastApplied int

    lastTimeReceivedHeartbeat time.Time

    applyCh   chan ApplyMsg
    applyCond *sync.Cond

    killCh chan struct{}

    // debugging
    runtimeLogFile *os.File
    logger         *log.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    return rf.currentTerm, rf.state == Leader
}

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
// #1 I need to check if this one
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.logEnterStart()
    // Your code here (2B).
    rf.mu.Lock()
    rf.logLockUnlock(true)
    defer rf.logFinishStart()
    defer func() {
        rf.mu.Unlock()
        rf.logLockUnlock(false)
    }()

    if rf.state != Leader {
        return 0, 0, false
    }

    rf.log = append(rf.log, LogEntry{
        Term:         rf.currentTerm,
        Command:      command,
        CommandValid: true,
    })

    rf.matchIndex[rf.me] = len(rf.log) - 1

    rf.persist()

    return len(rf.log) - 1, rf.currentTerm, true
}

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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {

    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me
    rf.currentTerm = 1

    rf.state = Follower

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    rf.lastApplied = 0

    rf.lastIncludedIndex = 0
    rf.lastIncludedTerm = 0
    rf.log = []LogEntry{
        {0, 0, false},
    }

    rf.runtimeLogFile, _ = os.Create(fmt.Sprintf("raft-%d-%d.log", time.Now().Second(), rf.me))

    rf.runtimeLogFile.Truncate(0)

    rf.logger = log.New(rf.runtimeLogFile, "", log.Ltime|log.Lmicroseconds)

    rf.applyCh = applyCh
    rf.applyCond = sync.NewCond(&rf.mu)

    rf.lastTimeReceivedHeartbeat = time.Now()

    // Your initialization code here (2A, 2B, 2C).
    go func() {
        for {
            switch rf.getStateLocked() {
            case Follower, Candidate:
                rf.runFollowerOrCandidate()
            case Leader:
                rf.runLeader()
            default:
                panic("invalid state")
            }
        }
    }()

    // initialize from state persisted before a crash
    rf.readPersist(rf.persister.ReadRaftState())

    go func() {
        rf.runApply()
    }()

    return rf
}

// switch to read-write lock later on
func (rf *Raft) setStateLocked(state State) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.state = state
}

func (rf *Raft) getStateLocked() State {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.state
}

func (rf *Raft) runFollowerOrCandidate() {
    timeout := generateRandomTimeout()
    electionTimer := time.After(timeout)

    for rf.getStateLocked() != Leader {
        select {
        case <-electionTimer:
            if time.Since(rf.lastTimeReceivedHeartbeat) < timeout {
                timeout = generateRandomTimeout()
                electionTimer = time.After(timeout)
                continue
            }

            // Start Election
            timeout = generateRandomTimeout()
            electionTimer = time.After(timeout)
            rf.StartElection()

        default:
            continue
        }
    }
}

func (rf *Raft) runLeader() {
    heartbeat := time.NewTicker(heartbeatInterval)

    for rf.getStateLocked() == Leader {
        select {
        case <-heartbeat.C:
            // goroutine might be interrupted here
            // when AppendEntriesToOthers() is called, I might not be a leader anymore.
            rf.AppendEntriesToOthers()
        }
    }
}

// non-thread-safe
// I could be a leader, a candidate, or a follower.
// If I learn a higher term, I update my term. If I'm not a follower, I become a follower.
// If I'm currently a leader, I stop my heartbeat ticker.
// If I'm currently a candidate, I reset the election ticker.
func (rf *Raft) mayUpdateTerm(term int, from int) bool {

    // xxx -> follower
    if term > rf.currentTerm {

        rf.currentTerm = term

        if rf.state != Follower {
            rf.logStateChange(rf.state, Follower, rf.currentTerm, "learn a higher term")
        }

        rf.state = Follower
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

type State string

const (
    Leader    State = "leader"
    Follower  State = "follower"
    Candidate State = "candidate"
)
