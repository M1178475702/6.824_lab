package raft

import (
	"fmt"
	"math"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Log //TODO 嵌套结构体的字段也是需要大写的
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendHeartbeat() {
	var prevLogTerm int
	var prevLogIndex = len(rf.log) - 1
	prevLogTerm = rf.log[prevLogIndex].Term

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
					PrevLogIndex: prevLogIndex,
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

func (rf *Raft) appendLog(command interface{}) {
	rf.mu.Lock()
	entry := Log{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	isLeader := rf.leaderId == rf.me
	rf.mu.Unlock()
	if !isLeader {
		return
	}
	//schedule
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			if rf.isAppending[i] {
				rf.mu.Unlock()
				return
			} else {
				rf.isAppending[i] = true
				rf.mu.Unlock()
			}

			//TODO 需要总结这里的并发编程经过
			for {
				rf.mu.Lock()
				begin := rf.nextIndex[i]
				end := len(rf.log)
				rf.mu.Unlock()
				_, _ = DPrintf("end: %d, begin: %d", begin, end)
				if end-begin == 1 {
					rf.mu.Lock()
					prevLogIndex := begin - 1
					prevLogTerm := rf.log[prevLogIndex].Term
					_, _ = DPrintf("%d leader send log to %d, match: %d, begin: %d, end: %d, PrevLogIndex %d", rf.me, i, rf.matchIndex[i], begin, end, prevLogIndex)
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogTerm:  prevLogTerm,
						PrevLogIndex: prevLogIndex,
						Entries:      rf.log[begin:end],
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := new(AppendEntriesReply)
					ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
					_, _ = DPrintf("%d leader recv reply from %d success: %t, term: %d", rf.me, i, reply.Success, reply.Term)
					if ok {
						if reply.Success {
							rf.checkCommit(i, end-1)
						} else {
							rf.mu.Lock()
							if reply.Term > 0 && reply.Term > rf.currentTerm {
								//old leader
								rf.currentTerm = reply.Term
								return
							} else {
								//adjust next index
								rf.nextIndex[i]--
							}
							rf.mu.Unlock()
						}
					} else {
						//失败
					}
				} else {
					rf.mu.Lock()
					rf.isAppending[i] = false
					rf.mu.Unlock()
					break
				}
			}
		}(i)

	}
}

func (rf *Raft) checkCommit(server, repEnd int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nextIndex[server] > 1 && rf.matchIndex[server] == 0 {
		rf.matchIndex[server] = rf.nextIndex[server]
	}
	rf.nextIndex[server] = repEnd + 1
	rf.matchIndex[server] = repEnd

	//serverMatchIndex := rf.matchIndex[server]
	//if serverMatchIndex < rf.commitIndex {
	//	min := int(math.Min(float64(repEnd), float64(rf.commitIndex)))
	//	rf.matchIndex[server] += min - serverMatchIndex
	//	rf.nextIndex[server] += min - serverMatchIndex
	//}
	//原出错原因：已经修改过 matchIndex，但是由于下面排序算法会修改原数组，导致数据出错，v可能没有发生变化
	moreThanHalfIndex := MoreThanHalf(len(rf.matchIndex))
	v := GetSortedLocalValue(rf.matchIndex, moreThanHalfIndex)
	//TODO 第二个条件？
	_, _ = DPrintf("%d next is %d, match is %d, v: %d, commit: %d, repend is %d", server, rf.nextIndex[server], rf.matchIndex[server], v, rf.commitIndex, repEnd)
	fmt.Println(rf.matchIndex)
	if v > rf.commitIndex && rf.log[v].Term == rf.currentTerm {
		for i := rf.commitIndex + 1; i <= v; i++ {
			rf.apply(i)
		}
		rf.commitIndex = v
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//_, _ = DPrintf("%d Term is %d, commit is %d, %d leader Term is %d, commit is %d", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderId, args.Term, args.LeaderCommit)
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
		lenlog := len(rf.log)
		rf.mu.Unlock()
		if lenlog-1 > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		} else {
			_, _ = DPrintf("%d args.Entries len is %d", rf.me, len(args.Entries))
			index := args.PrevLogIndex + 1
			srcMax := lenlog - 1
			for _, e := range args.Entries {
				//need check term and index
				if index <= srcMax {
					rf.setLog(e, index)
				} else {
					rf.appendLog(e.Command)
					_, _ = DPrintf("%d append log exist log len is %d, command is %d", rf.me, len(rf.log), e.Command.(int))
				}
			}
			reply.Success = true
		}

	}

	go func() {
		rf.mu.Lock()
		if args.LeaderCommit > rf.commitIndex {
			_, _ = DPrintf("%d max index: %d, leader commit %d, gid %d", rf.me, len(rf.log)-1, args.LeaderCommit, GetGID())
			//_, _ = DPrintf("%d commit: %d,leader commit %d", rf.me, rf.commitIndex, args.LeaderCommit)
			//oldCommitIndex := rf.commitIndex
			max := int(math.Min(float64(len(rf.log)-1), float64(args.LeaderCommit)))
			for i := rf.lastApplied + 1; i <= max; i++ {
				rf.apply(i)
				rf.commitIndex++
			}
		}
		rf.mu.Unlock()
	}()

}

func (rf *Raft) setLog(entry Log, i int) {
	srcEntry := rf.log[i]
	if srcEntry.Term == entry.Term {
		return
	} else {
		rf.log[i] = entry
	}
}
