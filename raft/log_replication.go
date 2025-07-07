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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {}
