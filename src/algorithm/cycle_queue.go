package algorithm

import "sync"

type CycleQueue struct {
	sync.Mutex
	container []interface{}
	begin     int
	len       int
	cap       int
}

func (cq *CycleQueue) Enqueue(ele interface{}) {
	cq.Lock()
	defer cq.Unlock()
	if cq.container == nil {
		cq.len = 0
		if cq.cap == 0 {
			cq.cap = 5
		}
		cq.container = make([]interface{}, cq.cap)
	}
	if cq.len == cq.cap {
		dst := make([]interface{}, cq.cap*2)
		copy(dst, cq.container[cq.begin:cq.cap])
		copy(dst[cq.cap-cq.begin:cq.begin+1], cq.container[:cq.begin+1])
		cq.container = dst
		cq.cap *= 2
	}
	cq.container[cq.len] = ele
	cq.len++
}

func (cq *CycleQueue) Dequeue() (ele interface{}) {
	cq.Lock()
	defer cq.Unlock()
	ele = cq.front()
	cq.begin = (cq.begin + 1) % cq.cap
	cq.len--
	return ele
}

func (cq *CycleQueue) Empty() bool {
	cq.Lock()
	defer cq.Unlock()
	return cq.empty()
}

func (cq *CycleQueue) empty() bool {
	return cq.len == 0
}

func (cq *CycleQueue) Front() (ele interface{}) {
	cq.Lock()
	defer cq.Unlock()
	return cq.front()
}

func (cq *CycleQueue) front() (ele interface{}) {
	if cq.empty() {
		panic("queue is empty")
	}
	ele = cq.container[cq.begin]
	return ele
}

func (cq *CycleQueue) Len() int {
	cq.Lock()
	defer cq.Unlock()
	return cq.len
}
