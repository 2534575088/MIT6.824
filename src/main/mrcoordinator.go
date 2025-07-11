package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	// 检查命令行参数，确保至少提供一个输入文件
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// 创建协调器实例，传入输入文件列表和reduce任务数量(10个)
	m := mr.MakeCoordinator(os.Args[1:], 10)
	// 循环检查协调器是否完成所有任务
    // 未完成时休眠1秒后继续检查
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	// 任务完成后额外等待1秒，确保所有资源释放
	time.Sleep(time.Second)
}
