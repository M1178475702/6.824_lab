package raft

import (
	"bytes"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var myRand = rand.New(rand.NewSource(int64(time.Now().Nanosecond())))

func randInt(b, s int) (r int) {
	r = myRand.Intn(s - b)
	r += b
	return r
}

func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func MoreThanHalf(len int) (index int) {
	index = len / 2
	return index
}

func GetSortedLocalValue(srcArr []int, index int) int {
	//TODO arr 需是一个copy
	arr := make([]int, len(srcArr))

	copy(arr, srcArr)
	size := len(arr)
	max := index + 1
	for i := 0; i < max; i++ {
		heapAdjust(arr[0 : size-i])
		swapInt(&arr[0], &arr[size-i-1])
	}
	return arr[size-max]
}

func heapAdjust(arr []int) {
	max := len(arr) - 1
	for i := max; i >= 0; i-- {
		l := i*2 + 1
		r := i*2 + 2
		if l == max {
			if arr[l] < arr[i] {
				swapInt(&arr[l], &arr[i])

			}
		} else if r <= max {
			if arr[l] < arr[r] {
				swapInt(&arr[l], &arr[r])

			}
			if arr[r] < arr[i] {
				swapInt(&arr[r], &arr[i])

			}
		}
	}
}

func swapInt(l, r *int) {
	temp := *l
	*l = *r
	*r = temp
}
