package raft

type QueueApplyMsg struct {
	applyMsg ApplyMsg
	to       int
}

func (rf *Raft) apply(index int) {
	_, _ = DPrintf("%d apply index %d", rf.me, index)
	applyMsg := ApplyMsg{
		Command: rf.log[index].Command,
		Index:   index,
	}
	rf.applyCh <- applyMsg
	rf.lastApplied++
}

func (rf *Raft) beginApply() {
	applyMsgCh := make(chan ApplyMsg)
	for am := range applyMsgCh {
		rf.applyCh <- am
	}

}
