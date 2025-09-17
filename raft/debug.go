package raft

import (
    "fmt"
    "sort"
    "strings"
    "sync/atomic"
)

var (
    rpcId        atomic.Uint32
    logicalClock atomic.Uint32
)

func (rf *Raft) logStateChange(oldState State, newState State, term int, detail string) {
    var b strings.Builder
    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))
    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, term))
    b.WriteString(fmt.Sprintf("[STATE %s -> *%s] ", oldState, newState))
    b.WriteString(detail)
    rf.logger.Println(b.String())
}

func (rf *Raft) logRpc(caller int, callee int, rpcName string, term int, rpcId uint32, detail map[string]interface{}) {

    var b strings.Builder

    var isCaller bool

    if rf.me == caller {
        isCaller = true
    } else {
        isCaller = false
    }

    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))

    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, term))
    if isCaller {
        b.WriteString(fmt.Sprintf("[%-15s %04d *%d -> %d CALLER] ", rpcName, rpcId, caller, callee))
    } else {
        b.WriteString(fmt.Sprintf("[%-15s %04d %d -> *%d CALLEE] ", rpcName, rpcId, caller, callee))
    }

    keys := make([]string, 0, len(detail))
    for k := range detail {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    for _, k := range keys {
        b.WriteString(fmt.Sprintf("%s{%v} ", k, detail[k]))
    }

    rf.logger.Println(b.String())
}

func (rf *Raft) logCommitIndexUpdate(oldCommitIndex int, newCommitIndex int) {
    var b strings.Builder
    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))
    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, rf.currentTerm))
    b.WriteString(fmt.Sprintf("[COMMIT_INDEX %d -> %d]", oldCommitIndex, newCommitIndex))
    rf.logger.Println(b.String())
}

func (rf *Raft) logApply(oldLastApplied int, newLastApplied int) {
    var b strings.Builder
    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))
    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, rf.currentTerm))
    b.WriteString(fmt.Sprintf("[APPLY %d -> %d] ", oldLastApplied, newLastApplied))
    b.WriteString(fmt.Sprintf("Applied: %v", rf.log[oldLastApplied+1:newLastApplied+1]))
    rf.logger.Println(b.String())
}

func (rf *Raft) logEnterStart() {
    var b strings.Builder
    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))
    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, rf.currentTerm))
    b.WriteString("[ENTER START]")
    rf.logger.Println(b.String())
}

func (rf *Raft) logFinishStart() {
    var b strings.Builder
    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))
    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, rf.currentTerm))
    b.WriteString("[FINISH START]")
    rf.logger.Println(b.String())
}

func (rf *Raft) logLockUnlock(isLock bool) {
    if isLock {
        rf.logger.Println("lock")
    } else {
        rf.logger.Println("unlock")
    }
}
