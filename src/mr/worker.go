package mr

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
// KeyValue 定义了 Map 阶段输出的 key/value 对。
type KeyValue struct {
	Key   string
	Value string
}

// SortedKey 实现 sort.Interface 接口，使 []KeyValue 支持按 key 排序
type SortedKey []KeyValue

// Len 重写len,swap,less才能排序
func (k SortedKey) Len() int { return len(k) }
func (k SortedKey) Swap(i, j int) { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// ihash 用于对 key 进行哈希，将 key 分配给对应的 reduce 分区（bucket）
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) // 取正数
}


//
// main/mrworker.go calls this function.
// Worker 主函数，启动后会不断轮询获取任务、执行任务，并将完成结果回传给协调器
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	keepFlag := true
	for keepFlag {
		task := GetTask() // 从 coordinator 拉取任务

		switch task.TaskType {
		case MapTask:
			DoMapTask(mapf, &task)
			callDone(&task) // 通知任务完成

		case ReduceTask:
			DoReduceTask(reducef, &task)
			callDone(&task)

		case WaittingTask:
			time.Sleep(time.Second * 5) // 等待任务重新分配

		case ExitTask:
			// time.Sleep(1s) 有可能使得其他 RPC 尝试在 socket 被清理后继续发生，产生 connection refused
			// time.Sleep(time.Second)
			fmt.Println("All tasks are Done, will be exiting...")
			keepFlag = false // 终止循环
			return // 直接退出，避免再发 RPC
		}
	}
	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// 从 coordinator 获取一个任务（可能是 map、reduce 或等待/退出）
func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// 执行 Map 任务逻辑：读取输入文件，调用 mapf，按 hash(key) 分区写入中间 JSON 文件
func DoMapTask(mapf func(string, string) []KeyValue, response *Task){
	var intermediate []KeyValue
	filename := response.FileSlice[0] // 每个 map task 只有一个文件

	// 读取文件内容
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 通过io工具包获取content,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 执行用户提供的 map 函数，返回 kv 列表
	intermediate = mapf(filename, string(content))

	// 根据 hash(key) % nReduce 将 kv 分组
	rn := response.ReducerNum
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		reduceIdx := ihash(kv.Key) % rn
		HashedKV[reduceIdx] = append(HashedKV[reduceIdx], kv)
	}

	// 将每个 reduce 分区的 kv 写入 JSON 文件：mr-tmp-mapid-reduceid
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			if err := enc.Encode(kv); err != nil {
				return 
			}
		}
		ofile.Close()
	}
}

// 执行 Reduce 任务逻辑：读取所有 map 产生的中间文件，排序、分组后应用 reducef 输出结果
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId // 用于最终输出文件的编号
	intermediate := shuffle(response.FileSlice) // 聚合所有中间文件

	// 创建临时输出文件（防止中途失败写入）
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	// 遍历排序后的 kv 数组，按 key 分组，调用 reducef 处理
	i := 0
	for i < len(intermediate){
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key{
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()

	// 最后写入文件重命名为最终输出文件名
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

// 读取中间 JSON 文件，解码为 KeyValue 并排序
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kva))
	return kva
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// callDone 通知 coordinator 该任务已完成
func callDone(f *Task) Task {
	args := f
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return reply
}