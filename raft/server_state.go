package raft

// When calling this method, I'm sure that this server will be the leader.
func (rf *Raft) changeToLeader() {
    rf.serverState = Leader

    for peer := range rf.peers {
        rf.nextIndex[peer] = len(rf.log)
        rf.matchIndex[peer] = 0
    }

    rf.electedCh <- struct{}{}
}

//
func (rf *Raft) changeToFollower() {
    rf.votedFor = -1
    rf.serverState = Follower
    rf.becomeFollowerCh <- struct{}{}
}

func (rf *Raft) changeToCandidate() {
    rf.votedFor = rf.me
    rf.serverState = Candidate
}

type ServerState int

const (
    Leader    ServerState = iota // 0
    Follower                     // 1
    Candidate                    // 2
)
