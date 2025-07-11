package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义为全局，worker之间访问coordinator时加锁
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	ReduceTaskChannel chan *Task     // 使用chan保证并发安全
	MapTaskChannel    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state State // 任务的状态
	StartTime time.Time // 任务的开始时间 为crash做准备
	TaskAdr *Task // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct{
	MetaMap map[int]*TaskMetaInfo // 通过下拉hash快速定位
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// MakeCoordinator 创建 Coordinator 实例
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		ReducerNum: nReduce,
		DistPhase: MapPhase,
		MapTaskChannel: make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			// 任务的总数应该是files + Reducer的数量
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.makeMapTasks(files) // 初始化 Map 任务
	c.server() // 启动 RPC 服务      
	go c.CrashDetector() // 启动任务崩溃检测器
	return &c
}

// CrashDetector 负责检测 Worker 超时未完成的任务，并重新调度
func (c *Coordinator) CrashDetector(){
	for{
		time.Sleep(2 * time.Second)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap{
			if v.state == Working && time.Since(v.StartTime) > 9*time.Second{
				fmt.Printf("the task [%d] is crash, take [%0.2f] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime).Seconds())

				switch v.TaskAdr.TaskType{
				case MapTask:
					c.MapTaskChannel <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAdr
					v.state = Waiting
				}
			}
		}
		mu.Unlock()
	}
}

// makeMapTasks 创建 Map 阶段任务并加入通道
func (c *Coordinator) makeMapTasks(files []string){
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType: MapTask,
			TaskId: id,
			ReducerNum: c.ReducerNum,
			FileSlice: []string{v},
		}
		taskMetaInfo := TaskMetaInfo{
			state: Waiting,  // 任务等待被执行
			TaskAdr: &task,  // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		c.MapTaskChannel <- &task
	}
}

// makeReduceTasks 创建 Reduce 阶段任务并加入通道
func (c *Coordinator) makeReduceTasks(){
	for i := 0; i < c.ReducerNum; i++{
		id := c.generateTaskId()
		task := Task{
			TaskId: id,
			TaskType: ReduceTask,
			FileSlice: selectReduceName(i),
		}
		taskMetaInfo := TaskMetaInfo{
			state: Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		c.ReduceTaskChannel <- &task
	}
}

// selectReduceName 查找中间文件中属于指定 Reduce 分区的文件
func selectReduceName(reduceNum int) []string{
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files{
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(),strconv.Itoa(reduceNum)){
			s = append(s, fi.Name())
		}
	}
	return s
}

// acceptMeta 将任务元信息注册到任务管理器中
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool{
	taskId := TaskInfo.TaskAdr.TaskId
	if _, exists := t.MetaMap[taskId]; exists{
		return false
	}
	t.MetaMap[taskId] = TaskInfo
	return true
}

// 分发任务给 Worker（被 Worker 的 RPC 调用）
/*
将map任务管道中的任务取出，如果取不出来，说明任务已经取尽，
那么此时任务要么就已经完成，要么就是正在进行。
判断任务map任务是否先完成，如果完成那么应该进入下一个任务处理阶段（ReducePhase），
因为此时我们先验证map则直接跳过reduce直接allDone全部完成
*/
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 分发任务应该上锁，防止多个worker竞争，并用defer回退解锁
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase{
	case MapPhase:
		if len(c.MapTaskChannel) > 0 {
			*reply = *<-c.MapTaskChannel
			if !c.taskMetaHolder.judgeState(reply.TaskId){
				fmt.Printf("Map-taskid[ %d ] is running\n",reply.TaskId)
			}
		}else{
			reply.TaskType = WaittingTask
			if c.taskMetaHolder.checkTaskDone(){
				c.toNextPhase()
			}
		}
	case ReducePhase:
		if len(c.ReduceTaskChannel) > 0{
			*reply = *<-c.ReduceTaskChannel
			if !c.taskMetaHolder.judgeState(reply.TaskId){
				fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
			}
		}else {
			reply.TaskType = WaittingTask
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
		}
	case AllDone:
		reply.TaskType = ExitTask
	default:
		panic("The phase undefined!")
	}
	return nil
}

// toNextPhase 切换任务阶段（Map → Reduce → AllDone）
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	}else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// 检查当前阶段的任务是否全部完成
func (t *TaskMetaHolder) checkTaskDone() bool {
	mapDoneNum, mapUnDoneNum := 0, 0
	reduceDoneNum, reduceUnDoneNum := 0, 0

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		switch v.TaskAdr.TaskType {
		case MapTask:
			if v.state == Done {
				mapDoneNum++
			}else{
				mapUnDoneNum++
			}
		case ReduceTask:
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	// 若 Map 阶段完成，且未进入 Reduce，或者 Reduce 阶段已完成
	if(mapDoneNum > 0 && mapUnDoneNum == 0 && reduceDoneNum == 0 && reduceUnDoneNum == 0) ||
		(reduceDoneNum > 0 && reduceUnDoneNum == 0) {
			return true
	}
	return false
}

// 若任务处于 Waiting 状态，则将其标记为 Working，并记录开始时间
func (t *TaskMetaHolder) judgeState(taskId int) bool{
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int{
	id := c.TaskId
	c.TaskId++
	return id
}

// MarkFinished 被 Worker 调用，标记任务已完成
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error{
	mu.Lock()
	defer mu.Unlock()
	meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

	if ok && meta.state == Working {
		meta.state = Done
		fmt.Printf("%v task Id[%d] marked as Done.\n", args.TaskType, args.TaskId)
	} else {
		fmt.Printf("Task Id[%d] already finished or invalid (type: %v).\n", args.TaskId, args.TaskType)
	}
	return nil
}

// Done 被主线程调用，用于判断是否可以退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Println("All tasks are finished, the coordinator will exit!")
		return true
	}
	return false
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
