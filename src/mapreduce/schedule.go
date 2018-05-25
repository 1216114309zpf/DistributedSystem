package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
        var failedTask [] int
        finishChan:=make(chan string)
        callReplyChan:=make(chan bool)
        failedTaskChan:=make(chan int)
        workerCount:=0

        for i:=0;i<ntasks;i++ {
             failedTask=append(failedTask,i)
        }

        for len(failedTask)!=0 {
             var worker string
             select {
                 case worker=<-registerChan:
                        workerCount++
                 case worker=<-finishChan:
                        reply:=<-callReplyChan
                        if reply==false {
                            taskNo:=<-failedTaskChan
                            failedTask = append(failedTask, taskNo)
                            continue
                        }
             }

             taskNo:=failedTask[0]
             failedTask=failedTask[1:]

             go func(taskNo int,worker string){
                 args:=new(DoTaskArgs)
                 args.JobName=jobName
                 if phase==mapPhase {
                      args.File=mapFiles[taskNo]
                 }
                 args.Phase=phase
                 args.TaskNumber=taskNo
                 args.NumOtherPhase=n_other
                 reply:=call(worker,"Worker.DoTask",args,nil)
                 finishChan<-worker
                 callReplyChan<-reply
                 if reply==false {
                     workerCount-- //here is the key point
                     failedTaskChan<-taskNo
                 }
             }(taskNo,worker)
        }

        for count:=0;count<workerCount;count++{
            <-finishChan
        }
	fmt.Printf("Schedule: %v done\n", phase)
}
