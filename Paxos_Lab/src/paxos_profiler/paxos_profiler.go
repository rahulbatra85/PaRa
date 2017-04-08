package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"paxos_kv"
	"runtime/pprof"
	"strconv"
	"time"
)

const (
	UNRELIABLE = "UNRELIABLE"
	RELIABLE   = "RELIABLE"
	MEMPROFILE = "MEMPROFILE"
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
	var maxClients int
	var maxServers int
	var numRequests int
	var profileType string
	//Input - number of clients, cluster size, profile type, requests per client
	flag.IntVar(&maxClients, "nc", 4, "Max Number of Clients. Will increment by power of 2. Default is 32.")
	flag.IntVar(&maxServers, "ns", 3, "Max Number of Servers. Will increment by 2, and start with an odd value. Default is 11.")
	flag.IntVar(&numRequests, "nr", 1000, "Number of Request per client. Default is 1000.")
	flag.StringVar(&profileType, "p", "all", "Profile Type: r=Reliable,u=Unreliable,p=Partition,m=Memory Profile,c=Cpu Profile")

	flag.Parse()

	fmt.Println("Starting Paxos Profiler")
	err := os.Remove("paxos_profile.out")
	if err != nil {
		log.Fatal(err)
	}

	switch profileType {
	case "r":
		reliable(maxClients, maxServers, numRequests)
	case "u":
		unreliable(maxClients, maxServers, numRequests)
	case "p":
		partition(maxClients, maxServers, numRequests)
	case "m":
		memoryProfile(maxClients, maxServers, numRequests)
	case "c":
		cpuProfile(maxClients, maxServers, numRequests)
	case "all":
		all(maxClients, maxServers, numRequests)
	default:
		fmt.Println("ERROR: Invalid Profile Type")
	}

	fmt.Println("Paxos Profiler Exit")
}

func reliable(maxClients int, maxServers int, numRequests int) {
	fmt.Println("Profile with reliable network Enter")
	startTest(maxClients, maxServers, numRequests, RELIABLE, RELIABLE)
	fmt.Println("Profile with reliable network Exit")
}

func unreliable(maxClients int, maxServers int, numRequests int) {
	fmt.Println("Profile with unreliable network Enter")
	startTest(maxClients, maxServers, numRequests, UNRELIABLE, UNRELIABLE)
	fmt.Println("Profile with unreliable network Exit")
}

func partition(maxClients int, maxServers int, numRequests int) {
	fmt.Println("Profile with partition network Enter")
	fmt.Println("Profile with partition network Exit")
}

func memoryProfile(maxClients int, maxServers int, numRequests int) {
	fmt.Println("Memory Profile Enter")
	startTest(maxClients, maxServers, numRequests, RELIABLE, MEMPROFILE)

	startTest(maxClients, maxServers, numRequests, UNRELIABLE, MEMPROFILE)

	fmt.Println("Memory Profile Exit")
}

func cpuProfile(maxClients int, maxServers int, numRequests int) {
	fmt.Println("CPU Profile Enter")
	f, err := os.Create("CPU_Profile_Reliable.out")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	startTest(maxClients, maxServers, numRequests, RELIABLE, CPUPROFILE)
	pprof.StopCPUProfile()
	f.Close()

	f, err = os.Create("CPU_Profile_Unreliable.out")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	startTest(maxClients, maxServers, numRequests, UNRELIABLE, CPUPROFILE)
	fmt.Println("CPU Profile Exit")
	pprof.StopCPUProfile()
	f.Close()
}

func all(maxClients int, maxServers int, numRequests int) {
	reliable(maxClients, maxServers, numRequests)
	unreliable(maxClients, maxServers, numRequests)
	partition(maxClients, maxServers, numRequests)
	memoryProfile(maxClients, maxServers, numRequests)
	cpuProfile(maxClients, maxServers, numRequests)
}

func startTest(maxClients int, maxServers int, numRequests int, networkType string, outputType string) {
	if outputType != MEMPROFILE && outputType != CPUPROFILE {

	}
	for numServers := 3; numServers <= maxServers; numServers += 2 {
		for numClients := 2; numClients <= maxClients; numClients *= 2 {
			// connections
			var pKVConns []string = make([]string, numServers)
			for c := 0; c < numServers; c++ {
				pKVConns[c] = port(c)
			}

			//Create servers
			var pKVServers []*paxos_kv.PaxosKVServer = make([]*paxos_kv.PaxosKVServer, numServers)
			for s := 0; s < numServers; s++ {
				pKVServers[s] = paxos_kv.StartServer(pKVConns, s)
				if networkType == UNRELIABLE {
					pKVServers[s].Setunreliable(true)
				} else if networkType == RELIABLE {
					pKVServers[s].Setunreliable(false)
				}
			}

			//Create clients
			var pClients []*paxos_kv.PaxosClient = make([]*paxos_kv.PaxosClient, maxClients)
			for c := 0; c < numClients; c++ {
				pClients[c] = paxos_kv.MakePaxosClient(pKVConns, NextClientId())
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
						//Generate request
						requestType := rand.Int() % 2
						key := strconv.Itoa(rand.Int() % 1000)
						value := strconv.Itoa(rand.Int())
						//Get time
						start := time.Now()
						//Send request
						if requestType == 0 {
							data.numPUTs++
							pClients[cid].Put(key, value)
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
							pClients[cid].Get(key)
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

			if outputType != MEMPROFILE && outputType != CPUPROFILE {
				dataf, err := os.OpenFile("paxos_profile.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
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
			} else if outputType == MEMPROFILE {
				fname := fmt.Sprintf("%s_%s.out", "MEM_Profile", networkType)
				f, err := os.Create(fname)
				if err != nil {
					log.Fatal(err)
				}

				for c := 0; c < numClients; c++ {
					<-statsCh
					if c == numClients/2 {
						pprof.WriteHeapProfile(f)
					}
				}
				f.Close()
			}
			StopKVServers(pKVServers)
		}
	}
}

func StopKVServers(pKVServers []*paxos_kv.PaxosKVServer) {
	for i := 0; i < len(pKVServers); i++ {
		if pKVServers[i] != nil {
			pKVServers[i].Kill()
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
