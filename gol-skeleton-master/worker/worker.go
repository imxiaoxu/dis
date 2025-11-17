package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
)

// 和 broker 中的 Task 保持字段、名字一致（导出）
type Task struct {
	StartY, EndY int
	WorldPart    [][]uint8
}

// Worker 类型
type Worker struct{}

// ProcessPart：对 Task.WorldPart 的“中间那几行”应用 GOL 规则，返回结果行
func (w *Worker) ProcessPart(t Task, reply *[][]uint8) error {
	height := t.EndY - t.StartY
	if height <= 0 {
		return fmt.Errorf("invalid task: height <= 0")
	}
	if len(t.WorldPart) < height+2 {
		return fmt.Errorf("invalid task: worldPart too small")
	}

	width := len(t.WorldPart[0])
	res := make([][]uint8, height)

	// 对应的核心行在 WorldPart 中是 [1 .. height]
	for y := 0; y < height; y++ {
		row := make([]uint8, width)
		srcY := y + 1 // 对应 worldPart 中的行号

		for x := 0; x < width; x++ {
			neighbors := 0

			// 8 邻居（注意：垂直方向靠 halo 行，水平方向用环绕）
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					if dx == 0 && dy == 0 {
						continue
					}
					ny := srcY + dy
					if ny < 0 || ny >= len(t.WorldPart) {
						continue
					}
					nx := (x + dx + width) % width // 左右环绕
					if t.WorldPart[ny][nx] == 255 {
						neighbors++
					}
				}
			}

			cell := t.WorldPart[srcY][x]
			if cell == 255 {
				// 存活细胞
				if neighbors == 2 || neighbors == 3 {
					row[x] = 255
				} else {
					row[x] = 0
				}
			} else {
				// 死细胞
				if neighbors == 3 {
					row[x] = 255
				} else {
					row[x] = 0
				}
			}
		}
		res[y] = row
	}

	*reply = res
	return nil
}

// main：启动 RPC 服务，监听指定端口
func main() {
	port := flag.Int("port", 8031, "port to listen on")
	flag.Parse()

	srv := rpc.NewServer()
	if err := srv.RegisterName("Worker", new(Worker)); err != nil {
		fmt.Println("RegisterName error:", err)
		os.Exit(1)
	}

	addr := fmt.Sprintf(":%d", *port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Listen error:", err)
		os.Exit(1)
	}
	fmt.Printf("Worker listening on %s\n", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		go srv.ServeConn(conn)
	}
}
