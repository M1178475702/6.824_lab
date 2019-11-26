package raft

import "sync/atomic"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	prevLogIndex int
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
					//	rf.recvHbWhenLeaderCh <- i
					//}

				}
			}(i)
		}
	}
}

func (rf *Raft) sendAppendEntries(entries []Log) {
	// entries
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		prevLogIndex: len(rf.log) - 1,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	commitNeed := int32(len(rf.peers))/2 + 1
	maxIndex := len(rf.log) - 1
	rf.mu.Unlock()

	replicateCount := int32(0)
	errorCount := int32(0)
	canCommitCh := make(chan int) // 1 成功 2 网络原因失败 3 old leader 失败
	for i, p := range rf.peers {
		go func() {
			reply := new(AppendEntriesReply)
			ok := p.Call("Raft.AppendEntries", args, reply)
			if ok {
				if reply.Success {
					atomic.AddInt32(&replicateCount, 1)
					//更新next, match
					//index is ?
					rf.nextIndex[i] = maxIndex + 1
					rf.matchIndex[i] = maxIndex
					if atomic.LoadInt32(&replicateCount) == commitNeed {
						canCommitCh <- 1
					}
				} else {
					// 说明该leader是一个 old leader,更新自己的term
					rf.currentTerm = reply.Term
					canCommitCh <- 3
				}
			} else {
				atomic.AddInt32(&errorCount, 1)
				if atomic.LoadInt32(&errorCount) == commitNeed {
					canCommitCh <- 2
				}
			}
		}()
	}
	res := <-canCommitCh
	if res == 1 {

	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//_, _ = DPrintf("%d cur Term is %d, %d leader Term is %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	//TODO 需要方法判断，该消息为过去任期的leader发送的，应该给予false回复
	//TODO 当有节点失去链接后，如何检测，以及如何更新alive peer
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//通知外部有消息进入，使重置选举时间
	if args.Entries == nil {
		// heartbeat, reply immediately
		if args.Term > rf.currentTerm {
			//old leader: term is low, cur leaderId is self and different form args.leader
			if args.LeaderId != rf.leaderId && rf.leaderId == rf.me {
				rf.recvHbWhenLeaderCh <- args.LeaderId
			}
			rf.currentTerm = args.Term
		}
		rf.appendEntriesCh <- args.LeaderId
		rf.mu.Lock()
		rf.leaderId = args.LeaderId
		rf.votedFor = -1
		rf.mu.Unlock()
		reply.Success = true
	} else {
		//simulate persist msg
		//提交就是向applyCh 发送收到的命令(通知config收到了
		//config 会缓存收到的命令,（因为这样更方便config做检查）
		rf.mu.Lock()
		if len(rf.log)-1 >= args.prevLogIndex && rf.log[args.prevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		}

		//var index int =
		for _, e := range args.Entries {

			applyMsg := ApplyMsg{
				Index:       0,
				Command:     e.Command,
				UseSnapshot: false,
				Snapshot:    nil,
			}
			rf.mu.Lock()

			rf.applyCh <- applyMsg
			rf.lastApplied = rf.commitIndex
			rf.commitIndex = args.LeaderCommit
		}

	}

}
