package raft

import "time"

const (
    heartbeatInterval time.Duration = 60 * time.Millisecond
)

type AppendEntriesArgs struct {
    Term     int
    LeaderId int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }

    rf.maybeUpdateTerm(args.Term)
    reply.Term = rf.currentTerm
    reply.Success = true
}

// 不是leader了  done<-chan struct{} 来停止
func (rf *Raft) Heartbeat() {
    ticker := time.NewTicker(heartbeatInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            go rf.startAppendEntries()
        case <-rf.noLongerLeader:
            break
        }
    }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) startAppendEntries() {
    for i := range rf.peers {
        if i == rf.me {
            continue
        }

        peerIdx := i
        go func() {
            args := AppendEntriesArgs{
                Term:     rf.currentTerm,
                LeaderId: rf.me,
            }
            reply := AppendEntriesReply{}

            rf.sendAppendEntries(peerIdx, &args, &reply)
            if toFollower := rf.maybeUpdateTerm(reply.Term); toFollower {
                return
            }
        }()
    }
}
