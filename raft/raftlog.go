package raft

type LogEntry struct {
    Term         int
    Command      interface{}
    CommandValid bool
}

type RaftLog struct {
    TailLog           []LogEntry
    Snapshot          []byte
    LastIncludedIndex int
    LastIncludedTerm  int
}

func NewRaftLog() *RaftLog {
    rl := &RaftLog{
        TailLog:           []LogEntry{{0, nil, true}},
        Snapshot:          []byte{},
        LastIncludedIndex: 0,
        LastIncludedTerm:  0,
    }
    return rl
}

// not thread-safe
// [from: until]  inclusive range
func (rl *RaftLog) GetEntries(from, until int) []LogEntry {
    if until < from {
        return []LogEntry{}
    }
    logLen := until - from + 1
    entries := make([]LogEntry, logLen)
    copy(entries, rl.TailLog[from-rl.LastIncludedIndex:until-rl.LastIncludedIndex+1])
    return entries
}

// not thread-safe
func (rl *RaftLog) Append(entries []LogEntry) {
    rl.TailLog = append(rl.TailLog, entries...)
}

func (rl *RaftLog) AppendAfter(entries []LogEntry, after int) {
    rl.TailLog = append(rl.TailLog[:after-rl.LastIncludedIndex+1], entries...)
}

// not thread-safe
func (rl *RaftLog) GetPrevTerm(index int) int {
    return rl.TailLog[index-rl.LastIncludedIndex-1].Term
}

func (rl *RaftLog) GetActualSize() int {
    return rl.LastIncludedIndex + len(rl.TailLog)
}

func (rl *RaftLog) GetActualLastIndex() int {
    return rl.LastIncludedIndex + len(rl.TailLog) - 1
}

func (rl *RaftLog) GetTermAt(index int) int {
    return rl.TailLog[index-rl.LastIncludedIndex].Term
}

func (rl *RaftLog) GetLastTerm() int {
    return rl.TailLog[len(rl.TailLog)-1].Term
}

func (rl *RaftLog) GetCommandAt(index int) interface{} {
    return rl.TailLog[index-rl.LastIncludedIndex].Command
}

func (rl *RaftLog) SetTermAtZero(term int) {
    rl.TailLog[0].Term = term
}
