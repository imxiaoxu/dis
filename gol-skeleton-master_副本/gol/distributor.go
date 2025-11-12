package gol

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

type WorldParams struct {
	ImageWidth  int
	ImageHeight int
	World       [][]uint8
}

// distributor: 基于实现 A 的修改版本（并发安全、去重计数、易读按键处理）
func distributor(p Params, c distributorChannels, keyPresses <-chan rune) {
	// mutex 保护共享 world
	var mu sync.Mutex

	// 1. 初始化世界
	world := make([][]uint8, p.ImageHeight)
	for y := range world {
		world[y] = make([]uint8, p.ImageWidth)
	}

	// 2. 读取初始图像
	c.ioCommand <- ioInput
	c.ioFilename <- fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput //从IO模块读像素，将亮度值填充到当前单元格
		}
	}

	// 3. 发送初始执行状态
	turn := 0
	c.events <- StateChange{turn, Executing} //是专门向外部模块发送状态事件的通道

	// 4. 发送初始存活细胞事件（批量）
	var initialAlive []util.Cell //存储初始代所有存活细胞的坐标

	mu.Lock()
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
				initialAlive = append(initialAlive, util.Cell{X: x, Y: y})
			}
		}
	}
	mu.Unlock()
	if len(initialAlive) > 0 { //// 若有存活细胞，才发送事件（避免空事件）
		c.events <- CellsFlipped{CompletedTurns: turn, //状态发生变化的细胞坐标 。回合数
			Cells: initialAlive} // 状态变化的细胞坐标集合
	}
	c.events <- TurnComplete{CompletedTurns: turn}
	//读取初始图像 → 发送 Executing 状态 → 发送存活细胞（CellsFlipped） → 发送 TurnComplete
	// 5. 连接 RPC 服务器（用于 ProcessTurn 与 GetAliveCellsCount）
	client, err := rpc.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer client.Close() //函数退出时执行 避免网络连接泄漏

	isPaused := false

	// ticker 每 2 秒通过 RPC 请求远端的存活细胞数量（仅保留远端计数）
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	done := make(chan struct{}) //创建一个无缓冲的空结构体通道 。作为后台定时协程的 “退出开关”

	go func() {
		for {
			select {
			case <-ticker.C:
				var aliveCount int
				// 直接询问 Broker，避免读取本地 world 导致并发冲突
				err := client.Call("Broker.GetAliveCellsCount", nil, &aliveCount)
				if err == nil {
					c.events <- AliveCellsCount{CompletedTurns: turn, CellsCount: aliveCount}
				} else {
					// 打印错误，但不强制退出；根据需要可修改为退出
					fmt.Println("AliveCellsCount RPC error:", err)
				}
			case <-done:
				return
			}
		}
	}()

	// 简单明了的按键处理：使用布尔标志确保只 close 一次（比 sync.Once 更易读）
	doneClosed := false
	eventsClosed := false

	// handleKey 返回 true 表示需要退出 distributor（主循环应 return）
	handleKey := func(key rune) bool {
		switch key {
		case 'p':
			isPaused = !isPaused
			if isPaused {
				c.events <- StateChange{turn, Paused}
			} else {
				c.events <- StateChange{turn, Executing}
			}
		case 's':
			// 保存当前世界（为避免 race，传入深拷贝）
			mu.Lock()
			worldCopy := deepCopyWorldUint8(world) //创建 world 的完全副本（而非引用）避免两个变量指向同一个内存
			mu.Unlock()
			saveWorld(p, c, worldCopy, turn)
		case 'q':
			// 停止 ticker 并 finalize，然后退出
			if !doneClosed {
				close(done)
				doneClosed = true
			}
			// 为 finalize 传入深拷贝，避免 race
			mu.Lock()
			worldCopy := deepCopyWorldUint8(world)
			mu.Unlock()
			finalizeGame(p, c, worldCopy, turn)
			return true
		case 'k':
			// 优雅关机：保存，关闭 rpc，等待 IO，发送 Quitting，关闭 events，退出
			mu.Lock()
			worldCopy := deepCopyWorldUint8(world)
			mu.Unlock()
			saveWorld(p, c, worldCopy, turn)

			fmt.Println("Shutting down gracefully...")
			_ = client.Close() // 忽略关闭错误

			// 等待 IO 空闲
			c.ioCommand <- ioCheckIdle
			<-c.ioIdle

			c.events <- StateChange{turn, Quitting}

			if !eventsClosed {
				close(c.events)
				eventsClosed = true
			}
			if !doneClosed {
				close(done)
				doneClosed = true
			}
			return true
		default:
			// 未识别按键，忽略
		}
		return false
	}

	// 主循环：处理按键与回合推进
	for turn < p.Turns {
		select {
		case key := <-keyPresses: // 监听键盘输入
			if handleKey(key) { // 需要退出直接返回
				return
			}
		default: // 无按键时，推进回合
			if !isPaused { // 未暂停则执行下一代
				// 为 RPC 调用构造参数：对 world 做深拷贝以免在 RPC 期间持锁
				mu.Lock()
				params := WorldParams{
					ImageWidth:  p.ImageWidth,
					ImageHeight: p.ImageHeight,
					World:       deepCopyWorldUint8(world),
				}
				mu.Unlock()

				var newWorld [][]uint8
				err := client.Call("Broker.ProcessTurn", params, &newWorld)
				if err != nil {
					fmt.Println("Error calling server:", err)
					// 停止 ticker 并退出
					if !doneClosed {
						close(done)
						doneClosed = true
					}
					return
				}

				// 收集变化并批量发送 CellsFlipped（在持锁期间读取旧 world）
				var flipped []util.Cell
				mu.Lock()
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						if world[y][x] != newWorld[y][x] {
							flipped = append(flipped, util.Cell{X: x, Y: y})
						}
					}
				}
				// 原子更新 world
				world = newWorld
				mu.Unlock()

				// 回合已完成
				turn++

				if len(flipped) > 0 {
					c.events <- CellsFlipped{CompletedTurns: turn, Cells: flipped}
				}
				c.events <- TurnComplete{CompletedTurns: turn}
			}
		}
	}

	// 完成所有回合：停止 ticker 并 finalize（传入深拷贝）
	if !doneClosed {
		close(done)
	}
	mu.Lock()
	finalWorldCopy := deepCopyWorldUint8(world)
	mu.Unlock()
	finalizeGame(p, c, finalWorldCopy, turn)
}

// deepCopyWorldUint8 对 [][]uint8 做深拷贝
func deepCopyWorldUint8(src [][]uint8) [][]uint8 {
	if src == nil {
		return nil
	}
	h := len(src)
	dst := make([][]uint8, h)
	for i := 0; i < h; i++ {
		row := make([]uint8, len(src[i]))
		copy(row, src[i])
		dst[i] = row
	}
	return dst
}

// 计算邻居存活数：统一使用 [][]uint8
func countLiveNeighbors(world [][]uint8, x, y, width, height int) int {
	dirs := []struct{ dx, dy int }{ //遍历 dirs 切片的每一个元素，循环次数 = 切片长度（这里是 8 次）
		{-1, -1}, {-1, 0}, {-1, 1}, // 上左、上中、上右
		{0, -1}, {0, 1}, // 左、右（跳过当前细胞 (x,y)）
		{1, -1}, {1, 0}, {1, 1}, // 下左、下中、下右
	}
	count := 0
	// 计算当前方向邻居的坐标
	for _, d := range dirs {
		// 边界判断
		nx, ny := x+d.dx, y+d.dy
		if nx >= 0 && nx < width && ny >= 0 && ny < height {
			if world[ny][nx] == 255 { //判断该邻居是否存活
				count++
			}
		}
	}
	return count
}

// 统计存活细胞总数
func countAlive(world [][]uint8) int {
	count := 0
	for _, row := range world {
		for _, cell := range row {
			if cell == 255 {
				count++ //循环次数 = 网格的宽度（len(row)，即每一行的细胞数）。
			}
		}
	}
	return count
}

// 获取所有存活细胞的坐标
func getAliveCells(world [][]uint8) []util.Cell {
	var alive []util.Cell
	for y, row := range world { //Iterate over each row
		for x, cell := range row { //  Iterate over each cell  in the current row
			if cell == 255 {
				alive = append(alive, util.Cell{X: x, Y: y}) // 将存活细胞的坐标添加到切片中
			}
		}
	}
	return alive
}

// saveWorld 接受一个 world 副本（[][]uint8）并写出到 IO，随后发送 ImageOutputComplete。
// 注意：为避免 race，调用者应传入 deep copy（本文件中已如此处理）。
func saveWorld(p Params, c distributorChannels, world [][]uint8, turn int) {
	filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioCommand <- ioOutput //通知 IO 模块 “开始输出文件
	c.ioFilename <- filename
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- world[y][x]
		}
	}
	// 发出完成事件（保持原有行为）
	c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: filename}
}

// finalizeGame 接受一个 world 副本并发送最终事件、保存图像并等待 IO 空闲，然后发送 Quitting。
// 注意：调用者应传入 deep copy 以避免 race。
func finalizeGame(p Params, c distributorChannels, world [][]uint8, turn int) {
	// 发送最终活细胞列表
	finalAlive := getAliveCells(world)
	c.events <- FinalTurnComplete{CompletedTurns: turn, Alive: finalAlive}

	// 保存最终世界
	saveWorld(p, c, world, turn)

	// 等待 IO 完成
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// 通知退出并关闭 events
	c.events <- StateChange{turn, Quitting}
	close(c.events)
}
