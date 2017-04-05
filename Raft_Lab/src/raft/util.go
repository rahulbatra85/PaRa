//Name: raft.go
//Description: This file defines API to get/set raft persistent state
//Author: Rahul Batra

package raft

import "fmt"

//To turn on Debugging set this to 1
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		fmt.Printf(format, a...)
	}
	return
}

func GetMajority(N int) int {
	return (N + 1) / 2
}
