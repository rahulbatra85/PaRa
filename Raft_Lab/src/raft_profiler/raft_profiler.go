package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"raft_kv"
	"runtime/pprof"
	"strconv"
	"time"
)

const (
	UNRELIABLE = "UNRELIABLE"
	RELIABLE   = "RELIABLE"
	CPUPROFILE = "CPUPROFILE"
)

type clientStats struct {
	clientId     int
	numGETs      int
	totalGETTime float64 //seconds
	maxGETTime   float64 //seconds
	minGETTime   float64 //seconds
	numPUTs      int
	totalPUTTime float64 //seconds
	maxPUTTime   float64 //seconds
	minPUTTime   float64 //seconds
}

var clientID int

func NextClientId() int {
	clientID++
	return clientID
}

func main() {
	var numClients int
	var numServers int
	var numRequests int
	var profileType string
	//Input - number of clients, cluster size, profile type, requests per client
	flag.IntVar(&numClients, "nc", 2, "Max Number of Clients. Default is 2.")
	flag.IntVar(&numServers, "ns", 3, "Max Number of Servers.  Default is 3.")
	flag.IntVar(&numRequests, "nr", 100, "Number of Request per client. Default is 200.")
	flag.StringVar(&profileType, "p", "all", "Profile Type: r=Reliable,u=Unreliable,p=Partition,m=Memory Profile,c=Cpu Profile")

	flag.Parse()

	fmt.Println("Starting Raft Profiler")
	err := os.Remove("raft_profile.out")
	if err != nil {
		log.Fatal(err)
	}

	switch profileType {
	case "r":
		reliable(numClients, numServers, numRequests)
	case "u":
		unreliable(numClients, numServers, numRequests)
	case "c":
		cpuProfile(numClients, numServers, numRequests)
	case "all":
		all(numClients, numServers, numRequests)
	default:
		fmt.Println("ERROR: Invalid Profile Type")
	}

	fmt.Println("Paxos Profiler Exit")
}

func reliable(numClients int, numServers int, numRequests int) {
	fmt.Println("Profile with reliable network Enter")
	startTest(numClients, numServers, numRequests, RELIABLE, "")
	fmt.Println("Profile with reliable network Exit")
}

func unreliable(numClients int, numServers int, numRequests int) {
	fmt.Println("Profile with unreliable network Enter")
	startTest(numClients, numServers, numRequests, UNRELIABLE, "")
	fmt.Println("Profile with unreliable network Exit")
}

func cpuProfile(numClients int, numServers int, numRequests int) {
	fmt.Println("CPU Profile Enter")
	f, err := os.Create("CPU_Profile_Reliable.out")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	startTest(numClients, numServers, numRequests, RELIABLE, CPUPROFILE)
	pprof.StopCPUProfile()
	f.Close()

	f, err = os.Create("CPU_Profile_Unreliable.out")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	startTest(numClients, numServers, numRequests, UNRELIABLE, CPUPROFILE)
	fmt.Println("CPU Profile Exit")
	pprof.StopCPUProfile()
	f.Close()
}

func all(numClients int, numServers int, numRequests int) {
	reliable(numClients, numServers, numRequests)
	unreliable(numClients, numServers, numRequests)
	cpuProfile(numClients, numServers, numRequests)
}

func startTest(numClients int, numServers int, numRequests int, networkType string, outputType string) {
	// connections
	var rKVConns []string = make([]string, numServers)
	for c := 0; c < numServers; c++ {
		rKVConns[c] = port(c)
	}

	//Create servers
	var rKVServers []*raft_kv.RaftKVServer = make([]*raft_kv.RaftKVServer, numServers)
	for s := 0; s < numServers; s++ {
		rKVServers[s] = raft_kv.StartKVServer(rKVConns, s, nil, 0)
		if networkType == UNRELIABLE {
			rKVServers[s].Setunreliable(true)
		} else if networkType == RELIABLE {
			rKVServers[s].Setunreliable(false)
		}
	}

	//Create clients
	var rClients []*raft_kv.RaftClient = make([]*raft_kv.RaftClient, numClients)
	for c := 0; c < numClients; c++ {
		rClients[c] = raft_kv.MakeRaftClient(rKVConns, NextClientId())
	}

	statsCh := make(chan clientStats, numClients)

	//Run each clients in it's goroutine
	for c := 0; c < numClients; c++ {
		go func(cid int) {
			var data clientStats
			data.clientId = cid
			data.maxGETTime = 0
			data.maxPUTTime = 0
			data.minGETTime = 200000 //Some big number
			data.minPUTTime = 200000 //Some big number
			fmt.Printf("Client ID=%d Enter\n", cid)
			for r := 0; r < numRequests; r++ {
				fmt.Printf("Client ID=%d Req=%d\n", cid, r)
				//Generate request
				requestType := rand.Int() % 2
				key := strconv.Itoa(rand.Int() % 1000)
				value := strconv.Itoa(rand.Int())
				//Get time
				start := time.Now()
				//Send request
				if requestType == 0 {
					data.numPUTs++
					rClients[cid].Put(key, value)
					latency := time.Since(start).Seconds()
					if latency > data.maxPUTTime {
						data.maxPUTTime = latency
					}
					if latency < data.minPUTTime {
						data.minPUTTime = latency
					}
					data.totalPUTTime += latency
				} else {
					data.numGETs++
					rClients[cid].Get(key)
					latency := time.Since(start).Seconds()
					if latency > data.maxGETTime {
						data.maxGETTime = latency
					}
					if latency < data.minGETTime {
						data.minGETTime = latency
					}
					data.totalGETTime += latency
				}
			}
			fmt.Printf("Client ID=%d Done with requests\n", cid)
			//send data back to main routine
			statsCh <- data
			//main routine will dump data to a file
		}(c)
	}

	if outputType != CPUPROFILE {
		dataf, err := os.OpenFile("raft_profile.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(dataf, "\n\n")
		fmt.Fprintf(dataf, "Type: %s\n", networkType)
		fmt.Fprintf(dataf, "NumClients:%d NumServers: %d NumRequests:%d\n", numClients, numServers, numRequests)
		headerRow := "Num(GET),TotalTime(GET),Average Latency(GET),Max Latency(GET),Min Latency(GET),Num(PUT),TotalTime(PUT),Average Latency(PUT),Max Latency(PUT),Min Latency(PUT)\n"
		fmt.Fprintf(dataf, headerRow)
		for c := 0; c < numClients; c++ {
			data := <-statsCh
			fmt.Fprintf(dataf, "%d,%f,%f,%f,%f,%d,%f,%f,%f,%f\n",
				data.numGETs,
				data.totalGETTime,
				data.totalGETTime/float64(data.numGETs),
				data.maxGETTime,
				data.minGETTime,
				data.numPUTs,
				data.totalPUTTime,
				data.totalPUTTime/float64(data.numPUTs),
				data.maxPUTTime,
				data.minPUTTime)
		}
		dataf.Close()
	} else if outputType == CPUPROFILE {
		for c := 0; c < numClients; c++ {
			<-statsCh
		}
	}
	StopKVServers(rKVServers)
}

func StopKVServers(rKVServers []*raft_kv.RaftKVServer) {
	for i := 0; i < len(rKVServers); i++ {
		if rKVServers[i] != nil {
			rKVServers[i].Kill()
		}
	}
}

func port(host int) string {
	s := "/var/tmp/paxos_profile-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += strconv.Itoa(os.Getpid()) + "-"
	s += strconv.Itoa(host)
	return s
}
