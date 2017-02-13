package raft

//import "log"
import "fmt"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func GetMajority(N int) int {
	return (N + 1) / 2
}
