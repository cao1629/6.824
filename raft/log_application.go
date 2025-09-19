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
    // if false, this ApplyMsg contains a Snapshot message
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

func (rf *Raft) runApply() {
    for {
        rf.mu.Lock()
        rf.applyCond.Wait()
        var msgs []ApplyMsg

        if rf.pendingSnapshot {
            snapshotMsg := ApplyMsg{
                SnapshotValid: true,
                Snapshot:      rf.raftLog.Snapshot,
                SnapshotTerm:  rf.raftLog.LastIncludedTerm,
                SnapshotIndex: rf.raftLog.LastIncludedIndex,
            }
            rf.pendingSnapshot = false
            rf.mu.Unlock()

            rf.applyCh <- snapshotMsg
        } else {
            for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
                msgs = append(msgs, ApplyMsg{
                    CommandValid: true,
                    Command:      rf.raftLog.GetCommandAt(i),
                    CommandIndex: i,
                })
            }

            rf.mu.Unlock()

            for _, msg := range msgs {
                rf.applyCh <- msg
            }
            // what if interrupted here?
            rf.mu.Lock()
            rf.lastApplied = rf.commitIndex
            rf.mu.Unlock()
        }
    }
}
