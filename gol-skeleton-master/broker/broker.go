package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// Broker 负责调度 worker，并维护当前世界（用于 AliveCellsCount）
type Broker struct {
	currentWorld [][]uint8
	mu           sync.Mutex // 保护 currentWorld
}

// WorldParams 必须和 distributor / worker 那边保持一致
type WorldParams struct {
	ImageWidth  int
	ImageHeight int
	World       [][]uint8
}

// 每个 worker 客户端连接
type WorkerClient struct {
	addr   string
	client *rpc.Client
}

// 发送给 worker 的任务：，对应的 worldPart 带上下边界
type Task struct {
	StartY, EndY int
	WorldPart    [][]uint8
}

var (
	workerList  []WorkerClient
	workerMutex sync.Mutex
)

// ProcessTurn：接收 Distributor 的请求，分发任务给 Worker，合并结果
func (b *Broker) ProcessTurn(params WorldParams, reply *[][]uint8) error {
	// 1. 先更新当前世界（如果 AliveCellsCount 在下一时刻被问到）
	b.mu.Lock()
	b.currentWorld = params.World
	b.mu.Unlock()

	// 2. 初始化新世界
	newWorld := make([][]uint8, params.ImageHeight)
	for i := range newWorld {
		newWorld[i] = make([]uint8, params.ImageWidth)
	}

	// 3. 拷贝一份当前的 worker 列表，避免并发问题
	workerMutex.Lock()
	numWorkers := len(workerList) //获取当前已注册的工作节点数量 。初始化
	workers := make([]WorkerClient, numWorkers)
	copy(workers, workerList) //获取当前时刻 避免变化影响逻辑
	workerMutex.Unlock()

	if numWorkers == 0 {
		return fmt.Errorf("no workers available")
	}

	rowsPerWorker := params.ImageHeight / numWorkers

	var wg sync.WaitGroup
	var resultMu sync.Mutex

	// 4. 分给每个 worker 一段 y 区间
	for i, worker := range workers { //// i 是当前工作节点的索引，worker 是对应的工作节点客户端（用于后续分配任务）
		startY := i * rowsPerWorker
		endY := startY + rowsPerWorker
		if i == numWorkers-1 {
			endY = params.ImageHeight // 最后一个 worker 把剩下的都算完 将结束行设为世界总高度
		}

		// 构造 worldPart：核心行 + 上下边界（循环边界）
		worldPartLen := endY - startY
		worldPart := make([][]uint8, worldPartLen+2)

		// 核心行复制
		copy(worldPart[1:worldPartLen+1], params.World[startY:endY])

		// 上边界：startY 的上一行（循环）
		worldPart[0] = params.World[(startY-1+params.ImageHeight)%params.ImageHeight]

		// 下边界：endY 的下一行（循环）
		worldPart[worldPartLen+1] = params.World[endY%params.ImageHeight]

		task := Task{
			StartY:    startY,
			EndY:      endY,
			WorldPart: worldPart,
		}

		wg.Add(1)
		go func(w WorkerClient, t Task) {
			defer wg.Done()

			var workerResult [][]uint8
			// 调用 Worker.ProcessPart —— 下面 worker.go 会实现这个
			err := w.client.Call("Worker.ProcessPart", t, &workerResult)
			if err != nil {
				fmt.Printf("Worker %s process task failed: %v\n", w.addr, err)
				return
			}

			// 合并结果到 newWorld
			resultMu.Lock()
			for y := 0; y < len(workerResult); y++ {
				newWorld[t.StartY+y] = workerResult[y]
			}
			resultMu.Unlock()
		}(worker, task)
	}

	// 5. 等所有 worker 完成
	wg.Wait()

	// 6. 更新 Broker 保存的世界为新状态
	b.mu.Lock()
	b.currentWorld = newWorld
	b.mu.Unlock()

	*reply = newWorld
	return nil
}

// GetAliveCellsCount： Distributor 通过 RPC 查询当前世界的存活细胞数量
// 参数类型用 struct{}，和 distributor 中的 struct{}{} 一致。
func (b *Broker) GetAliveCellsCount(_ struct{}, reply *int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	aliveCount := 0
	for _, row := range b.currentWorld {
		for _, cell := range row {
			//
			if cell == 255 {
				aliveCount++
			}
		}
	}

	*reply = aliveCount
	return nil
}

// 注册一个 worker 建立RPC连接
func registerWorker(address string) error {
	client, err := rpc.Dial("tcp", address) //TCP连接并初始化RPC客户端
	if err != nil {
		fmt.Printf("Connect worker %s failed: %v\n", address, err)
		return err
	}

	workerMutex.Lock()
	workerList = append(workerList, WorkerClient{
		addr:   address,
		client: client,
	})
	workerMutex.Unlock()

	fmt.Printf("Worker %s registered successfully\n", address)
	return nil
}

func main() {
	workerAddresses := []string{
		// EC2-A
		"172.31.90.169:8031",
		"172.31.90.169:8032",
		"172.31.90.169:8033",
		// EC2-B
		"172.31.17.148:8031",
		"172.31.17.148:8032",
		"172.31.17.148:8033",

		// EC2-C
		"172.31.16.85:8031",
		"172.31.16.85:8032",
		"172.31.16.85:8033",
		"172.31.16.85:8034",
	}

	// 注册所有 worker
	for _, addr := range workerAddresses { // 注册每个 worker
		if err := registerWorker(addr); err != nil {
			fmt.Printf("Register worker %s failed\n", addr)
		}
	}

	// regist  Broker RPC service
	broker := new(Broker)
	if err := rpc.Register(broker); err != nil {
		fmt.Printf("Register broker RPC service failed: %v\n", err)
		return
	}

	// listen 8080
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Broker listen on port 8080 failed: %v\n", err)
		return
	}
	defer listener.Close()

	fmt.Println("Broker started successfully, listening on :8080...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Accept connection failed: %v\n", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
