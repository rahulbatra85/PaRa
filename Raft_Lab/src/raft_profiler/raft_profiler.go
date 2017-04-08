package main

import (
	"flag"
	"fmt"
	//"raft"
)

func main() {
	var numClients int
	var numServers int
	var profileType string
	//Input - number of clients, cluster size, profile type
	flag.IntVar(&numClients, "nClients", 3, "Number of Clients. Default is 3.")
	flag.IntVar(&numServers, "nServers", 3, "Number of Servers. Default is 3.")
	flag.StringVar(&profileType, "p", "all", "Profile Type: r=Reliable,u=Unreliable,p=Partition,m=Memory Profile,c=Cpu Profile")

	flag.Parse()

	fmt.Println("Starting Raft Profiler")
	switch profileType {
	case "r":
		reliable(numClients, numServers)
	case "u":
		unreliable(numClients, numServers)
	case "p":
		partition(numClients, numServers)
	case "m":
		memoryProfile(numClients, numServers)
	case "c":
		cpuProfile(numClients, numServers)
	case "all":
		all(numClients, numServers)
	default:
		fmt.Println("ERROR: Invalid Profile Type")
	}

	fmt.Println("Raft Profiler Exit")
}

func reliable(numClients int, numServers int) {
	fmt.Println("Profile with reliable network Enter")
	fmt.Println("Profile with reliable network Exit")
}

func unreliable(numClients int, numServers int) {
	fmt.Println("Profile with unreliable network Enter")
	fmt.Println("Profile with unreliable network Exit")
}

func partition(numClients int, numServers int) {
	fmt.Println("Profile with partition network Enter")
	fmt.Println("Profile with partition network Exit")
}

func memoryProfile(numClients int, numServers int) {
	fmt.Println("Memory Profile Enter")
	fmt.Println("Memory Profile Exit")
}

func cpuProfile(numClients int, numServers int) {
	fmt.Println("CPU Profile Enter")
	fmt.Println("CPU Profile Exit")
}

func all(numClients int, numServers int) {
	reliable(numClients, numServers)
	unreliable(numClients, numServers)
	partition(numClients, numServers)
	memoryProfile(numClients, numServers)
	cpuProfile(numClients, numServers)
}
