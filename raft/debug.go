package raft

import (
    "fmt"
    "strings"
    "sync/atomic"
    "sort"
)

var (
    rpcId  atomic.Uint32
    logicalClock atomic.Uint32
)

func (rf *Raft) logStateChange(oldState State, newState State, term int, detail string) {
    var b strings.Builder
    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))
    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, term))
    b.WriteString(fmt.Sprintf("[STATE %s -> *%s] ", oldState, newState))
    b.WriteString(detail)
    b.WriteString("\n")
    rf.runtimeLogFile.WriteString(b.String())
    rf.runtimeLogFile.Sync()
}

func (rf *Raft) logRpc(caller int, callee int, rpcName string, term int, rpcId uint32, detail map[string]interface{}) {
    
    var b strings.Builder

    var isCaller bool

    if (rf.me == caller) {
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


    b.WriteString("\n")
    rf.runtimeLogFile.WriteString(b.String())
    rf.runtimeLogFile.Sync()
}


func (rf *Raft)logCommitIndexUpdate(oldCommitIndex int, newCommitIndex int) {
    var b strings.Builder
    b.WriteString(fmt.Sprintf("%8d ", logicalClock.Add(1)))
    b.WriteString(fmt.Sprintf("[%d %02d] ", rf.me, rf.currentTerm))
    b.WriteString(fmt.Sprintf("[COMMIT_INDEX %d -> %d] \n", oldCommitIndex, newCommitIndex))
    rf.runtimeLogFile.WriteString(b.String())
    rf.runtimeLogFile.Sync()
}