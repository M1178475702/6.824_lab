package algorithm

type Queue interface {
	Enqueue(ele interface{})
	Dequeue() interface{}
	Front() interface{}
	Empty() bool
	Len() int
}
