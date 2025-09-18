package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
    // indicates if this message contains a newly committed log entry
    // if true, this ApplyMsg contains a command message
    // if false, this ApplyMsg contains a snapshot message
    CommandValid bool

    // command to apply to state machine
    Command interface{}

    // log index
    CommandIndex int

    // For 2D:
    SnapshotValid bool
    Snapshot      []byte
    SnapshotTerm  int
    SnapshotIndex int
}

// Apply log[lastApplied+1 : commitIndex] to the state machine
// thread-safe
//func (rf *Raft) Apply() {
//    rf.mu.Lock()
//
//
//    rf.logger.Printf("apply from %d to %d\n", rf.lastApplied+1, rf.commitIndex)
//    for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
//        rf.logger.Printf("try to send index %d to applyCh\n", i)
//        rf.applyCh <- ApplyMsg{
//            CommandValid: true,
//            Command:      rf.log[i].Command,
//            CommandIndex: i,
//        }
//        rf.logger.Printf("already sent index %d to applyCh\n", i)
//    }
//
//    rf.lastApplied = rf.commitIndex
//}

func (rf *Raft) runApply() {
    for {
        rf.mu.Lock()
        rf.applyCond.Wait()
        var msgs []ApplyMsg

        for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
            msgs = append(msgs, ApplyMsg{
                CommandValid: true,
                Command:      rf.log[i].Command,
                CommandIndex: i,
            })
        }

        rf.mu.Unlock()

        for _, msg := range msgs {
            rf.applyCh <- msg
        }
    }
}
