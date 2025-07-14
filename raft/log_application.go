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
// log已经commit了 可以apply了
type ApplyMsg struct {
    // indicates if this message contains a newly committed log entry
    // 如果是true 这个 ApplyMsg 就是一个command message
    // 如果是false 这个 ApplyMsg 就是一个snapshot message
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

// 每个peer都有一个LogApplier
type LogApplier struct {
    done          chan struct{}
    applyCh       chan ApplyMsg
    applySignalCh chan struct{}
    rf            *Raft
}

func NewLogApplier(applyCh chan ApplyMsg) *LogApplier {
    logApplier := &LogApplier{
        done:          make(chan struct{}),
        applyCh:       applyCh,
        applySignalCh: make(chan struct{}),
    }

    go func() {
        for {
            select {
            case <-logApplier.done:
                return
            case <-logApplier.applySignalCh:
                logApplier.Apply()
            }
        }
    }()

    return logApplier
}

func (logApplier *LogApplier) Apply() {

    for i := logApplier.rf.lastApplied + 1; i <= logApplier.rf.commitIndex; i++ {
        logApplier.applyCh <- ApplyMsg{
            CommandValid: true,
            Command:      logApplier.rf.log[i].Command,
            CommandIndex: i,
        }
    }
    logApplier.rf.lastApplied = logApplier.rf.commitIndex
}
