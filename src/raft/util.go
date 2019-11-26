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
