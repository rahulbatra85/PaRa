//Name: util.go
//Description: Utility to turn on/off debugging
//Author: Rahul Batra

package paxos

import (
	"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}
