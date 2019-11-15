package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type TaskPool struct {
	sync.Mutex
	waitPool *list.List
	schePool *list.List
}

func (tp *TaskPool) Add(task *DoTaskArgs) {
	tp.waitPool.PushBack(task)
}

func (tp *TaskPool) ScheOne() (task *DoTaskArgs) {
	tp.Lock()
	defer tp.Unlock()
	if tp.waitPool.Len() == 0 {
		return nil
	}
	ele := tp.waitPool.Back()
	task = ele.Value.(*DoTaskArgs)
	tp.waitPool.Remove(ele)
	tp.schePool.PushBack(task)
	return task
}

func (tp *TaskPool) Complete(task *DoTaskArgs) {
	tp.Lock()
	defer tp.Unlock()
	ele := tp.findInSche(task)
	tp.schePool.Remove(ele)
}

func (tp *TaskPool) findInSche(task *DoTaskArgs) *list.Element {
	if tp.schePool.Len() == 0 {
		return nil
	}
	for ele := tp.schePool.Front(); ele != nil; ele = ele.Next() {
		target := ele.Value.(*DoTaskArgs)
		if target.TaskNumber == task.TaskNumber {
			return ele
		}
	}
	return nil
}

func (tp *TaskPool) WhenError(task *DoTaskArgs) {
	tp.Lock()
	defer tp.Unlock()
	ele := tp.findInSche(task)
	tp.waitPool.PushBack(task)
	tp.schePool.Remove(ele)
}

func (tp *TaskPool) IsEmpty() bool {
	tp.Lock()
	defer tp.Unlock()
	return tp.schePool.Len() == 0 && tp.waitPool.Len() == 0
}

type WorkerPool struct {
	sync.Mutex
	mr *Master
	//working		*list.List
	idle *list.List
	all  *list.List
}

func CreateWp(mr *Master) (wp *WorkerPool) {
	wp = new(WorkerPool)
	wp.Mutex = sync.Mutex{}
	wp.mr = mr
	wp.idle = list.New()
	wp.all = list.New()
	mr.Lock()
	defer mr.Unlock()
	for _, w := range mr.workers {
		wp.idle.PushBack(w)
		wp.all.PushBack(w)
	}
	return wp
}

func (wp *WorkerPool) getIdle() string {
	wp.Lock()
	defer wp.Unlock()
	if wp.idle.Len() > 0 {
		ele := wp.idle.Front()
		worker := ele.Value.(string)
		wp.idle.Remove(ele)
		return worker
	} else {
		return ""
	}
}

func (wp *WorkerPool) WaitIdle() {
	//1. 完成的 2.中途加入的
	worker := <-wp.mr.registerChannel
	ele := wp.findInAll(worker)
	if ele == nil {
		wp.all.PushBack(worker)
	}
	wp.idle.PushBack(worker)
}

func (wp *WorkerPool) Complete(worker string) {
	wp.mr.registerChannel <- worker
}

func (wp *WorkerPool) WhenError(worker string) {
	wp.Lock()
	defer wp.Unlock()
	ele := wp.findInAll(worker)
	//TODO 还需要清除mr中的workerd
	wp.all.Remove(ele)
}

func (wp *WorkerPool) findInAll(worker string) *list.Element {
	if wp.all.Len() == 0 {
		return nil
	}
	for ele := wp.all.Front(); ele != nil; ele = ele.Next() {
		target := ele.Value.(string)
		if target == worker {
			return ele
		}
	}
	return nil
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	//TODO 使用 *list.New()时，当第一次PushBack时，第一个元素会是 root.next.next ...
	workerPool := CreateWp(mr)
	taskPool := TaskPool{sync.Mutex{}, list.New(), list.New()}
	for i, file := range mr.files {
		taskArgs := new(DoTaskArgs)
		taskArgs.TaskNumber = i
		taskArgs.JobName = mr.jobName
		taskArgs.Phase = phase
		taskArgs.NumOtherPhase = nios
		taskArgs.File = file
		taskPool.Add(taskArgs)
	}

	for !taskPool.IsEmpty() {
		//任务池不空，则可暂时判定有任务
		taskArgs := taskPool.ScheOne()
		if taskArgs == nil {
			//没有取到任务，则试着循环继续取
			continue
		}
		//取到一个任务时，再尝试分配worker。确保，有任务需要做才分配worker
		worker := workerPool.getIdle()
		for len(worker) == 0 {
			workerPool.WaitIdle()
			worker = workerPool.getIdle()
		}

		//TODO 获取worker放在工作线程中，似乎会循环创建线程直到任务池用尽，当任务池用尽，但没有全部完成时，也会不断轮询。将获取worker和任务都放在主线程
		//TODO 当进行reduce时，worker都已经notify过，没有新注册的则会卡住
		go func() {
			//TODO
			//先分配任务，如果任务没有分到，则直接退出或等待都可以。
			//如果先分配worker,但却没有任务，则必须释放worker，因为直接return，worker会不再被访问
			var err error
			ok := call(worker, "Worker.DoTask", taskArgs, err)
			if !ok {
				taskPool.WhenError(taskArgs)
				workerPool.WhenError(worker)
			} else {
				taskPool.Complete(taskArgs)
				//自己线程调用会被堵塞（来不及接收）
				//TODO 当没有线程在等待空闲worker时，该线程会阻塞，也许需要线程专门去接受idle
				workerPool.Complete(worker)
			}
		}()
	}
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
