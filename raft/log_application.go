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

        for {
            if rf.pendingSnapshot && rf.lastApplied == rf.raftLog.LastIncludedIndex {
                msg := ApplyMsg{
                    SnapshotValid: true,
                    Snapshot:      rf.raftLog.Snapshot,
                    SnapshotTerm:  rf.raftLog.LastIncludedTerm,
                    SnapshotIndex: rf.raftLog.LastIncludedIndex,
                }
                rf.pendingSnapshot = false
                rf.mu.Unlock()
                rf.applyCh <- msg
                rf.mu.Lock()
            } else if rf.lastApplied < rf.commitIndex {
                msg := ApplyMsg{
                    CommandValid: true,
                    Command:      rf.raftLog.GetCommandAt(rf.lastApplied + 1),
                    CommandIndex: rf.lastApplied + 1,
                }
                rf.mu.Unlock()
                rf.applyCh <- msg
                rf.mu.Lock()
                rf.lastApplied++
            } else {
                break
            }
        }

        rf.mu.Unlock()
    }

}
