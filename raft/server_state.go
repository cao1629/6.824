package raft

// When calling this method, I'm sure that this server will be the leader.
func (rf *Raft) changeToLeader() {}

func (rf *Raft) changeToFollower() {}

func (rf *Raft) changeToCandidate() {}

type ServerState int

const (
    Leader    ServerState = iota // 0
    Follower                     // 1
    Candidate                    // 2
)
