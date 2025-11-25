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

func distributor(p Params, c distributorChannels, keyPresses <-chan rune) {
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
			world[y][x] = <-c.ioInput
		}
	}

	// 3. 初始状态事件
	turn := 0
	c.events <- StateChange{turn, Executing}

	// 4. 发送初始存活细胞（CellsFlipped），方便 SDL / 测试拿到初始状态
	var initialAlive []util.Cell
	mu.Lock()
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
				initialAlive = append(initialAlive, util.Cell{X: x, Y: y})
			}
		}
	}
	mu.Unlock()
	if len(initialAlive) > 0 {
		c.events <- CellsFlipped{CompletedTurns: turn, Cells: initialAlive}
	}
	c.events <- TurnComplete{CompletedTurns: turn} // 用于同步系统状态，告知 SDL

	// 5. 连接 Broker（AWS 端）
	client, err := rpc.Dial("tcp", "54.87.214.152:8080")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	// 延迟关闭 RPC 连接：无论是否正常都关 防止长期占用 Broker 连接资源，避免tcp资源泄漏
	defer client.Close()

	isPaused := false

	// 6. 每 2 秒统计一次活细胞数量
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				aliveCount := countAlive(world)
				currentTurn := turn
				mu.Unlock()

				c.events <- AliveCellsCount{
					CompletedTurns: currentTurn,
					CellsCount:     aliveCount,
				}
			case <-done:
				return
			}
		}
	}()

	// 7. 输入- 单独 goroutine 专门处理 'p'，确保 Paused 事件在 2 秒内发出（满足 TestKeyboard）
	//    - 其它按键通过 controlKeys 交给主循环处理
	controlKeys := make(chan rune, 16)

	go func() {
		for key := range keyPresses { // 循环读取 keyPresses 通道中的键盘输入
			if key == 'p' {
				mu.Lock()
				isPaused = !isPaused
				currentTurn := turn
				state := Executing // 定义当前执行状态
				if isPaused {
					state = Paused
				}
				mu.Unlock()

				// 立即通知暂停 / 继续
				c.events <- StateChange{currentTurn, state}
			} else {
				controlKeys <- key
			}
		}
	}()

	// 确保通道只被关闭一次
	doneClosed := false
	eventsClosed := false

	// 处理除 'p' 之外的按键：s / q / k
	handleKey := func(key rune) bool {
		switch key {
		case 's':
			// 保存当前世界
			mu.Lock()
			worldCopy := deepCopyWorldUint8(world) //保存的是“按下保存键瞬间”的世界状态，后续主协程修改 world 不会干扰保存结果
			currentTurn := turn
			mu.Unlock()
			saveWorld(p, c, worldCopy, currentTurn)

		case 'q':
			// 退出控制器：保存最终世界并发送 FinalTurnComplete + Quitting
			if !doneClosed {
				close(done)
				doneClosed = true
			}
			mu.Lock()
			worldCopy := deepCopyWorldUint8(world)
			currentTurn := turn
			mu.Unlock()
			finalizeGame(p, c, worldCopy, currentTurn)
			return true

		case 'k':
			// 关闭整个分布式系统：保存一次当前世界 + 等待 IO 空闲 + Quitting
			mu.Lock()
			worldCopy := deepCopyWorldUint8(world)
			currentTurn := turn
			mu.Unlock()
			saveWorld(p, c, worldCopy, currentTurn)

			fmt.Println("Shutting down gracefully...")
			_ = client.Close()

			// 等待 IO 空闲，确保文件写完
			c.ioCommand <- ioCheckIdle
			<-c.ioIdle

			c.events <- StateChange{currentTurn, Quitting}

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
			// 其他按键忽略
		}
		return false
	}

	// 8. 主回合循环：推进 Game of Life，并处理 s/q/k
	for turn < p.Turns {
		select {
		case key := <-controlKeys:
			if handleKey(key) {
				return
			}

		default:
			mu.Lock()
			paused := isPaused
			mu.Unlock()

			if paused {
				// 暂停时什么都不算，稍微 sleep 防止空转
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// 构造 RPC 参数（直接传 world 引用，在本回合结束前我们不会再改它）
			mu.Lock()
			params := WorldParams{
				ImageWidth:  p.ImageWidth,
				ImageHeight: p.ImageHeight,
				World:       world,
			}
			mu.Unlock()

			var newWorld [][]uint8
			err := client.Call("Broker.ProcessTurn", params, &newWorld)
			if err != nil {
				fmt.Println("Error calling server:", err)
				if !doneClosed {
					close(done)
					doneClosed = true
				}
				return
			}

			// 对比 old vs new，找出翻转的细胞，并更新 world
			var flipped []util.Cell
			mu.Lock()
			for y := 0; y < p.ImageHeight; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if world[y][x] != newWorld[y][x] {
						flipped = append(flipped, util.Cell{X: x, Y: y})
					}
				}
			}
			world = newWorld
			turn++
			currentTurn := turn
			mu.Unlock()

			if len(flipped) > 0 {
				c.events <- CellsFlipped{CompletedTurns: currentTurn, Cells: flipped}
			}
			c.events <- TurnComplete{CompletedTurns: currentTurn}
		}
	}

	// 9. 所有回合完成：发送最终事件并退出
	if !doneClosed {
		close(done)
	}
	mu.Lock()
	finalWorldCopy := deepCopyWorldUint8(world)
	finalTurn := turn
	mu.Unlock()
	finalizeGame(p, c, finalWorldCopy, finalTurn)
}

// deepCopyWorldUint8 对 [][]uint8 做深拷贝
func deepCopyWorldUint8(src [][]uint8) [][]uint8 {
	if src == nil {
		return nil
	} //// 边界处理：，避免后续索引 panic
	h := len(src) //// 获取原始切片的高度（行数）：h 等于外层切片的长度
	// 创建目标切片的外层结构
	dst := make([][]uint8, h)
	for i := 0; i < h; i++ {
		row := make([]uint8, len(src[i]))
		copy(row, src[i]) //// 拷贝原始行数据到目标行：copy 是值拷贝，将 src[i] 的所有元素复制到 row
		dst[i] = row
	}
	return dst //// 返回深拷贝后的完整二维切片：dst 与 src 内存完全独立，数据完全一致
}

// 统计存活细胞总数
func countAlive(world [][]uint8) int {
	count := 0
	for _, row := range world {
		for _, cell := range row {
			if cell == 255 {
				count++
			}
		}
	}
	return count
}

// 获取所有存活细胞的坐标
func getAliveCells(world [][]uint8) []util.Cell {
	var alive []util.Cell
	for y, row := range world {
		for x, cell := range row {
			if cell == 255 {
				alive = append(alive, util.Cell{X: x, Y: y})
			}
		}
	}
	return alive
}

// saveWorld：写出 world，并确保 IO 完成后才发 ImageOutputComplete
func saveWorld(p Params, c distributorChannels, world [][]uint8, turn int) {
	filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)

	// 1. 通知 IO 开始输出
	c.ioCommand <- ioOutput
	c.ioFilename <- filename

	// 2. 逐像素写出
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- world[y][x]
		}
	}

	// 3. 等待 IO 空闲（确保文件已经写完）
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// 4. 再发 ImageOutputComplete（TestKeyboard 会读这个文件）
	c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: filename}
}

// finalizeGame：发送 FinalTurnComplete + 保存最终世界 + Quitting
func finalizeGame(p Params, c distributorChannels, world [][]uint8, turn int) {
	finalAlive := getAliveCells(world)
	c.events <- FinalTurnComplete{CompletedTurns: turn, Alive: finalAlive}

	saveWorld(p, c, world, turn)

	c.events <- StateChange{turn, Quitting}
	close(c.events)
}
