package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample();

	//发送RPC请求以其获取任务
	for{
		args := CallArgs{}
		reply := CallReply{}
		call("Coordinator.GetTask", &args, &reply)
		switch reply.TaskType{
		case int(MapTask):
			filename := reply.FileName
			file, err := os.Open(filename)
                	if err != nil {
                        	log.Fatalf("cannot open %v", filename)
                	}
                	content, err := ioutil.ReadAll(file)
                	if err != nil {
                        	log.Fatalf("cannot read %v", filename)
                	}
                	file.Close()

			// 将 Map 结果进行分组
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))
			intermediates := make([][]KeyValue, reply.NReduce)
                        for _, kv := range kva {
                                reduceNum := ihash(kv.Key) % reply.NReduce
                                intermediates[reduceNum] = append(intermediates[reduceNum], kv)
                        }

			//将结果写入中间文件
			intermediateFileNames := make([]string, reply.NReduce)
			for reduceNum, intermediate :=range intermediates{
				intermediateFileName :=  fmt.Sprintf("mr-%d-%d", reply.TaskIndex, reduceNum)
				intermediateFileNames[reduceNum] = intermediateFileName
				intermediateFile, _ := os.Create(intermediateFileName)
				enc := json.NewEncoder(intermediateFile)
                                for _, kv := range intermediate {
                                        enc.Encode(&kv)
                                }
                                intermediateFile.Close()
				
			}
			
			args := CallArgs{
				TaskIndex:             reply.TaskIndex,
				TaskType:              reply.TaskType,
				IntermediateFileNames: intermediateFileNames,
			}
			call("Coordinator.FinishTask", &args, &reply)

		case int(ReduceTask):
			intermediateFileNames := reply.IntermediateFileNames
			kva := []KeyValue{}
			//从中间文件中读取键值对出来
			for _, intermediateFileName := range intermediateFileNames {
				intermediateFile, _ := os.Open(intermediateFileName)
				dec := json.NewDecoder(intermediateFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				intermediateFile.Close()
			}
			sort.Sort(ByKey(kva))
			//创建文件
			oname := fmt.Sprintf("mr-out-%d", reply.TaskIndex)
			tempFile, err := ioutil.TempFile(".", oname)
			if err != nil {
				fmt.Println("cannot create %v", tempFile)
			}

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
				i = j
			}

			tempFile.Close()
			os.Rename(tempFile.Name(), oname)

			// 向 Coordinator 发送输出文件名
			args := CallArgs{
				TaskIndex:  reply.TaskIndex,
				TaskType:   reply.TaskType,
				//OutputFile: oname,
			}
			
			//这里使用了FinishTask方法，但仍待定义
			call("Coordinator.FinishTask", &args, &reply)

		//所有任务已经完成，直接返回
		case int(Done):
			return

		//估计是所有任务分配完但还未完成
		default:

		}//end of switch
			
	}

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
