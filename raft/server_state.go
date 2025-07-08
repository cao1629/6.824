package raft

// When calling this method, I'm sure that this server will be the leader.
func (rf *Raft) changeToLeader() {
    rf.electionTicker.Pause()
}

func (rf *Raft) changeToFollower() {
    rf.electionTicker.Reset()
}

func (rf *Raft) changeToCandidate() {}

type ServerState int

const (
    Leader    ServerState = iota // 0
    Follower                     // 1
    Candidate                    // 2
)
