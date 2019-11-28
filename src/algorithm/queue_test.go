package algorithm

import (
	"fmt"
	"testing"
)

func tQ(q Queue) {
	q.Enqueue(1)
	q.Enqueue(2)
	fmt.Println(q.Len())
	q.Dequeue()
	fmt.Println(q.Len())
}

func TestCycleQueue_Dequeue(t *testing.T) {
	q := CycleQueue{
		cap: 5,
	}
	tQ(&q)
}
