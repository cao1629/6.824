package raft

import (
    "fmt"
    "log"
    "time"
)

// Debugging
//const Debug = false
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//    if Debug {
//        log.Printf(format, a...)
//    }
//    return
//}

type logTopic string

const (
    dClient  logTopic = "CLNT"
    dCommit  logTopic = "CMIT"
    dDrop    logTopic = "DROP"
    dError   logTopic = "ERRO"
    dInfo    logTopic = "INFO"
    dLeader  logTopic = "LEAD"
    dLog1    logTopic = "LOG1"
    dLog2    logTopic = "LOG2"
    dPersist logTopic = "PERS"
    dSnap    logTopic = "SNAP"

    dApply logTopic = "APLY"

    // Term changes
    dTerm  logTopic = "TERM"
    dTest  logTopic = "TEST"
    dTimer logTopic = "TIMR"
    dTrace logTopic = "TRCE"
    dVote  logTopic = "VOTE"
    dWarn  logTopic = "WARN"

    dDebug logTopic = "DEBG"

    // Server State changes
    dState logTopic = "STAT"

    dElection logTopic = "ELEC"

    dTicker logTopic = "TICK"

    dHeartbeat logTopic = "HERT"
)

var debugStart time.Time

func loggingInit() {
    debugStart = time.Now()
    log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func LOG(topic logTopic, format string, a ...interface{}) {
    time := time.Since(debugStart).Microseconds()
    time /= 100
    prefix := fmt.Sprintf("%06d %v ", time, string(topic))
    format = prefix + format
    log.Printf(format, a...)
}
