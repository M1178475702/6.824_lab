package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randInt(b, s int) (r int) {
	r = rand.Intn(s - b)
	r += b
	return r
}
