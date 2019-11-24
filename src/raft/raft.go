package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

type Log struct {
	Command string
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leaderId    int
	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	aeCh chan int //AppendEntries channel
	hbCh chan int //heartbeat channel
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.leaderId == rf.me
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil {
		return
	}
	r := bytes.NewReader(data)
	d := gob.NewDecoder(r)
	_ = d.Decode(&rf.currentTerm)
	_ = d.Decode(&rf.votedFor)
	_ = d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	//TODO  要大写，否则无法 decode
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//已经投过
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.me == rf.leaderId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		_, _ = DPrintf("p1 %d vote result for %d is %t", rf.me, args.CandidateId, false)
		return
	}
	if rf.votedFor != -1 {
		reply.VoteGranted = false
		_, _ = DPrintf("p2 %d vote result for %d is %t", rf.me, args.CandidateId, false)
		return
	}
	// candidate 的 log 至少 要与 跟随者一样新
	if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else if args.Term == rf.currentTerm {
		if rf.commitIndex != 0 {
			if args.LastLogTerm > rf.log[rf.commitIndex].Term {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			} else if args.LastLogIndex >= rf.commitIndex {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			} else {
				reply.VoteGranted = false
			}
		} else {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	_, _ = DPrintf("p3 %d vote result for %d is %t", rf.me, args.CandidateId, reply.VoteGranted)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = *new([]Log)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.leaderId = -1
	rf.aeCh = make(chan int)

	rf.matchIndex = make([]int, 0, len(rf.peers))
	rf.nextIndex = make([]int, 0, len(rf.peers))
	//do election
	go func() {
		//first wait AppendEntries request without empty Entries for a timeout
		for {
			timeout := randInt(150, 300)
			timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
			select {
			case f := <-rf.aeCh:
				//reset random election timeout
				_, _ = DPrintf("%d recv msg from %d, at %d", rf.me, f, time.Now().Nanosecond())
				break
			case <-timer.C:
				//election
				_, _ = DPrintf("%d start election, timeout is %d, now is %d", rf.me, timeout, time.Now().Nanosecond())
				isLeader := rf.election()
				if isLeader {
					t := time.NewTicker(time.Duration(50) * time.Millisecond)
					//如何自己知道自己已经断开链接，停止继续发送心跳包？
				loop1:
					for {
						//使用无阻塞管道进行轮询
						select {
						case <-rf.hbCh:
							break loop1
						default:
							<-t.C
							rf.sendHeartbeat()
						}
					}
				} else {
					rf.votedFor = -1

				}
			}
		}

	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) election() bool {
	rf.leaderId = -1
	rf.currentTerm++
	rf.votedFor = rf.me
	voteCount := 1
	vch := make(chan int)
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				var lastLogTerm int
				if rf.commitIndex == 0 {
					lastLogTerm = 0
				} else {
					lastLogTerm = rf.log[rf.commitIndex].Term
				}
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.commitIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := new(RequestVoteReply)
				ok := rf.sendRequestVote(i, args, reply)
				if ok {
					rf.mu.Lock()
					if reply.VoteGranted {
						voteCount++
					} else if reply.Term != 0 {
						rf.currentTerm = reply.Term
					}
					rf.mu.Unlock()
				}
				vch <- i
			}(i)
		}

	}
	recvCount := 0
loop:
	for {
		select {
		case <-vch:
			recvCount++
			if recvCount == len(rf.peers)-1 {
				break loop
			}
		case <-rf.aeCh:
			return false
		}
	}
	_, _ = DPrintf("%d election result %d", rf.me, voteCount)
	if voteCount > len(rf.peers)/2 {
		rf.leaderId = rf.me
		rf.sendHeartbeat()
		return true
	} else {
		return false
	}
	//可能没有选举出领导，在选择适当的timeout 之后重新开始
}
