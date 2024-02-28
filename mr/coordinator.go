package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"



type Coordinator struct {
	mu         sync.Mutex   // 用于保护共享状态的互斥锁
	files      []string     // 输入文件列表
	nReduce    int          // Reduce 任务的数量
	mapTasks   []Task       // Map 任务列表
	reduceTasks []Task       // Reduce 任务列表
	mapDone bool
	done       bool         // 任务是否完成标志
	intermediateFileNames map[int][]string
}


type Task struct {
	taskType  TaskType    // 任务类型：Map 或 Reduce
	taskIndex int         // 任务索引
	status    TaskStatus  // 任务状态：Idle、In progress 或 Completed
	startTime time.Time   // 任务开始时间
}
type TaskType int
const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	Done	   TaskType = 2 
	Goose 	   TaskType = 3
)
type TaskStatus int
const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)



func (c *Coordinator) assignMapTaskL(args *CallArgs, reply *CallReply) error{
	for i, task := range c.mapTasks {
	        if task.status == Idle {
                       c.mapTasks[i].status = InProgress
                       c.mapTasks[i].taskType = MapTask
                       reply.TaskType = int(MapTask)
                       reply.TaskIndex = i
                       reply.FileName = c.files[i]
                       reply.NReduce = c.nReduce
                       				//在这里可以启动一个监听进程
                       go c.waitTaskTimeout(&c.mapTasks[i])
                       return nil
                }
        }
						//所有map任务都分配出去了，但并不是都完成了，只能够傻等
	reply.TaskType = int(Goose)
	return nil
}
func (c *Coordinator) assignReduceTaskL(args *CallArgs, reply *CallReply) error{
	for i, task :=range c.reduceTasks{
                				//如果reduce任务中还有未分配的
                if task.status == Idle {
                        c.reduceTasks[i].status = InProgress
                        c.reduceTasks[i].taskType = ReduceTask
                        reply.TaskType = int(ReduceTask)
                        reply.TaskIndex = i
                        reply.IntermediateFileNames = c.intermediateFileNames[i]

                        go c.waitTaskTimeout(&c.reduceTasks[i])
                        return nil
                }
        }
        					//很好，所有的任务都分配完了，就傻等一下
        reply.TaskType = int(Goose)
        return nil

}

func (c *Coordinator) GetTask(args *CallArgs, reply *CallReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
                 				//如果map任务未完成，就只能够分配map任务
	if !c.mapDone{
		c.assignMapTaskL(args, reply)
		return nil
	}else if !c.done {                      //map任务都完成了，此时如果没有完成所有reduce任务
		c.assignReduceTaskL(args, reply)
		return nil
	}
						//所有的任务都完成了，就结束;
	reply.TaskType = int(Done)
	return nil
}


func (c *Coordinator) waitTaskTimeout(task *Task) {
    select {
    case <-time.After(10 * time.Second):
		if task.status== Completed{
			return
		}
        	c.handleTaskTimeout(task)
    }
}

func (c *Coordinator) handleTaskTimeout(task *Task) {
	c.mu.Lock()
    	defer c.mu.Unlock()
        task.status = Idle
    	//fmt.Printf("task %v-%v timeout on worker %v\n", task.taskType, task.taskIndex)
}

func (c *Coordinator) FinishTask(args *CallArgs, reply *CallReply) error {
        c.mu.Lock()
        defer c.mu.Unlock()
        taskType := args.TaskType
        // 更新任务状态为 Completed
        if taskType == int(MapTask) {
                c.handleMapFinishL(args, reply)
        } else if taskType == int(ReduceTask) {
                c.handleReduceFinishL(args, reply)

        }
        return nil
}

func (c *Coordinator) handleMapFinishL(args *CallArgs, reply *CallReply){
	taskIndex := args.TaskIndex
	c.mapTasks[taskIndex].status = Completed
        //fmt.Printf("Map Task %d completed\n", taskIndex)
        for r :=0; r<c.nReduce; r++{
                c.intermediateFileNames[r] = append(c.intermediateFileNames[r], args.IntermediateFileNames[r])
        }
        temp := true
        for _, task := range c.mapTasks {
                if task.status != Completed {
                        temp  = false
                        break
                }
        }
        c.mapDone = temp
}

func (c *Coordinator) handleReduceFinishL(args *CallArgs, reply *CallReply){
	taskIndex := args.TaskIndex
	c.reduceTasks[taskIndex].status = Completed
        //fmt.Printf("Reduce Task %d completed\n", taskIndex)
        temp := true
         for _, task := range c.reduceTasks {
                if task.status != Completed {
                temp = false
                break
                }
        }
        c.done = temp
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	ret=c.done
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	
	//初始化Map任务队列
	for i := 0; i < len(c.files); i++ {
		mapTask := Task{
			taskType:  MapTask,
			status:    Idle,
		}
		c.mapTasks = append(c.mapTasks, mapTask)
	}

	//初始化Reduce任务队列
	for i := 0; i < c.nReduce; i++ {
		reduceTask := Task{
			taskType:  ReduceTask,
			status:    Idle,
		}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}
	c.mapDone = false
	c.done = false
	c.intermediateFileNames = make(map[int][]string)
	c.server()
	return &c
}
