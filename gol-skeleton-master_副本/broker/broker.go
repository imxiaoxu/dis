package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type Broker struct {
	workers []string // save Worker address
}

type WorldParams struct {
	ImageWidth  int
	ImageHeight int
	World       [][]uint8
}

type Task struct {
	StartY, EndY int
	WorldPart    [][]uint8
}
type WorkerClient struct {
	client *rpc.Client
}

var workerList []WorkerClient
var workerMutex sync.Mutex

// Register Worker address
// Process Client requires and split task to Worker
func (b *Broker) ProcessTurn(params WorldParams, reply *[][]uint8) error {
	// save new world state
	newWorld := make([][]uint8, params.ImageHeight)
	for i := range newWorld {
		newWorld[i] = make([]uint8, params.ImageWidth)
	}

	numWorkers := len(workerList)
	if numWorkers == 0 {
		return fmt.Errorf("no workers available")
	}
	rowsPerWorker := params.ImageHeight / numWorkers
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, worker := range workerList {
		startY := i * rowsPerWorker
		endY := startY + rowsPerWorker
		if i == numWorkers-1 {
			endY = params.ImageHeight
		}

		//  contain part world data
		world_part := make([][]uint8, endY-startY+2)
		copy(world_part, params.World[startY:endY])
		world_part[len(world_part)-2] = params.World[(endY)%params.ImageHeight]
		world_part[len(world_part)-1] = params.World[(startY-1+params.ImageHeight)%params.ImageHeight]
		task := Task{
			StartY:    startY,
			EndY:      endY,
			WorldPart: world_part,
		}

		wg.Add(1)
		go func(worker WorkerClient, task Task) {
			defer wg.Done()
			var workerResult [][]uint8
			err := worker.client.Call("Worker.ProcessPart", task, &workerResult)
			if err != nil {
				fmt.Println("Error calling ProcessPart on worker:", err)
				return
			}

			// combine Worker's result to newWorld
			mu.Lock()
			for y := 0; y < len(workerResult); y++ {
				newWorld[task.StartY+y] = workerResult[y]
			}
			mu.Unlock()
		}(worker, task)
	}

	// wait for Worker complete task
	wg.Wait()
	*reply = newWorld
	return nil
}


func registerWorker(address string) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error connecting to worker at %s: %v\n", address, err)
		return err
	}
	workerMutex.Lock()
	defer workerMutex.Unlock()
	workerList = append(workerList, WorkerClient{client: client})
	fmt.Printf("Worker registered: %s\n", address)
	return nil
}

func main() {

	workerAddresses := []string{

		"localhost:8031",
		"localhost:8050",
	}

	for _, addr := range workerAddresses {
		if err := registerWorker(addr); err != nil {
			fmt.Printf("Failed to register worker at %s\n", addr)
		}
	}
	broker := new(Broker)
	err := rpc.Register(broker)
	if err != nil {
		fmt.Println("Error registering broker:", err)
		return
	}

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error starting broker:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Broker is listening on port 8080...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
