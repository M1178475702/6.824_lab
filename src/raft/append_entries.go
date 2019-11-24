package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	Entries      []Log //TODO 嵌套结构体的字段也是需要大写的
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendHeartbeat() {
	var prevLogTerm int
	if rf.commitIndex == 0 {
		prevLogTerm = 0
	} else {
		prevLogTerm = rf.log[rf.commitIndex].Term
	}
	reply := new(AppendEntriesReply)
	for i, _ := range rf.peers {
		if i != rf.me {
			//TODO  获取访问的i是同一个i，所以值都是最后的值，应该再用一个变量保存当前值
			go func(i int) {
				var args = AppendEntriesArgs{
					Term:        rf.currentTerm,
					LeaderId:    rf.me,
					PrevLogTerm: prevLogTerm,
					//Entries:      make([]Log,0,1),  //TODO 似乎只要参数有自定义结构体，就无法 encode
					LeaderCommit: rf.commitIndex,
				}
				ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
				if !ok {
					// timeout, delete the server
					//当超时时，是自己断开链接了，还是对方断开链接了
				} else {
					//if !reply.Success {
					//	rf.currentTerm = reply.Term
					//	rf.hbCh <- i
					//}

				}
			}(i)
		}
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	_, _ = DPrintf("%d cur Term is %d, %d leader Term is %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	//TODO 需要方法判断，该消息为过去任期的leader发送的，应该给予false回复
	//TODO 当有节点失去链接后，如何检测，以及如何更新alive peer
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//通知外部有消息进入，使重置选举时间
	if args.Entries == nil {
		// heartbeat, reply immediately

		//old leader
		if args.Term > rf.currentTerm {
			if args.LeaderId != rf.leaderId && rf.leaderId != -1 {
				rf.hbCh <- args.LeaderId
			}
			rf.currentTerm = args.Term
		}
		rf.aeCh <- args.LeaderId
		rf.leaderId = args.LeaderId
		rf.votedFor = -1
		reply.Success = true
	}

}
