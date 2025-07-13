package raft

type LogEntry struct {
    Term    int
    Command interface{}
}

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    reply.Term = rf.currentTerm
    reply.Success = false

    // 直接ignore messages with a smaller term
    if args.Term < rf.currentTerm {
        return
    }

    // 我可能是leader或者candidate with a smaller term
    if didUpdate := rf.maybeUpdateTerm(args.Term); didUpdate {
        // 我现在从一个leader或者candidate变成follower了
        // 我要说明我变成了谁的follower 所以我要更新voteFor
        rf.votedFor = args.LeaderId
    }

    // leader现在的log比这个follower长
    // note that 这是index 从0开始
    if args.PrevLogIndex >= len(rf.log) {
        return
    }

    // 相同index上的log不match
    if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        return
    }

    // 顺便把后面的log给delete了
    rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

    // 怎么样才算Success?
    // log match上了 然后args.Entries也被append上了
    reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) InitiateAppendEntries() {

    for peer := range rf.peers {
        if peer == rf.me {
            continue
        }

        peerIdx := peer
        go func() {
            args := AppendEntriesArgs{
                Term:         rf.currentTerm,
                LeaderId:     rf.me,
                PrevLogIndex: rf.nextIndex[peerIdx] - 1,
                PrevLogTerm:  rf.log[rf.nextIndex[peerIdx]-1].Term,
                Entries:      rf.log[rf.nextIndex[peerIdx]:],
            }

            reply := AppendEntriesReply{}

            rf.sendAppendEntries(peerIdx, &args, &reply)

            if didUpdate := rf.maybeUpdateTerm(args.Term); didUpdate {
                // 我现在从leader变成follower了
                // 我要说明我变成了谁的follower 所以我要更新voteFor
                rf.votedFor = args.LeaderId

                // 现在我不再是leader了 所以我也不用处理reply了
                return
            }

            // reply不是Success的原因是log不match: leader上的log更长 或者 相同index上的term不同
            if reply.Success == false {
                rf.nextIndex[peerIdx]--
            } else {
                // replicate到对面成功了
                rf.nextIndex[peerIdx]++
                rf.matchIndex[peerIdx] = args.PrevLogIndex + len(args.Entries) - 1
            }
        }()
    }
}
