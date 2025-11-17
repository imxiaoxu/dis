package gol

import (
	"fmt"
	"net"
	"net/rpc"
)

// LocalBroker 实现 RPC 接口，模拟远程服务器
type LocalBroker struct{}

// ProcessTurn 本地计算下一代（与分布式版本一致）
func (b *LocalBroker) ProcessTurn(params WorldParams, reply *[][]uint8) error {
	*reply = ProcessTurnLocal(params)
	return nil
}

// GetAliveCellsCount 返回世界中活细胞数量（非必须，但测试用例中可能调用）
func (b *LocalBroker) GetAliveCellsCount(_ any, reply *int) error {
	world := sampleWorld // 从全局变量读取当前世界（仅示例用）
	if world == nil {
		*reply = 0
	} else {
		*reply = countAlive(world)
	}
	return nil
}

// 启动一个本地 RPC 服务（单例）
func StartLocalRPCServer() (net.Listener, error) {
	server := rpc.NewServer()
	err := server.RegisterName("Broker", new(LocalBroker))
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		return nil, err
	}
	fmt.Println("[LocalRPC] Started on 127.0.0.1:8080")
	go server.Accept(ln)
	return ln, nil
}

// 停止服务
func StopLocalRPCServer(ln net.Listener) {
	if ln != nil {
		_ = ln.Close()
		fmt.Println("[LocalRPC] Server stopped")
	}
}

// 用于临时存储当前世界（便于 GetAliveCellsCount 使用）
var sampleWorld [][]uint8

// ProcessTurnLocal: 本地实现单步演化（直接从 distributor 里复制即可）
func ProcessTurnLocal(params WorldParams) [][]uint8 {
	w := params.World
	h := params.ImageHeight
	wd := params.ImageWidth
	newWorld := make([][]uint8, h)
	for y := 0; y < h; y++ {
		newWorld[y] = make([]uint8, wd)
		for x := 0; x < wd; x++ {
			n := countLiveNeighbors(w, x, y, wd, h)
			if w[y][x] == 255 {
				if n == 2 || n == 3 {
					newWorld[y][x] = 255
				} else {
					newWorld[y][x] = 0
				}
			} else {
				if n == 3 {
					newWorld[y][x] = 255
				} else {
					newWorld[y][x] = 0
				}
			}
		}
	}
	sampleWorld = newWorld // 更新全局状态（供 countAlive 使用）
	return newWorld
}
