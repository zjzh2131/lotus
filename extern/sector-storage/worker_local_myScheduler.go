package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/storage-sealing/lib/nullreader"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/xerrors"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var sc *SchedulerControl

var (
	pending = "pending"
	running = "running"
	done    = "done"
	finish  = "finish"
	failed  = "failed"
)

type SchedulerControl struct {
	lk        sync.Locker
	AP        chan taskReq
	P1        chan taskReq
	P2        chan taskReq
	C1        chan taskReq
	C2        chan taskReq
	FZ        chan taskReq
	ApP1      chan struct{}
	P2C2      chan struct{}
	SealingM  map[uint64]struct{}
	SealingCh chan struct{}
	closing   chan struct{}
}

type taskReq struct {
	ID       primitive.ObjectID
	TaskType string
	SectorId uint64
}

func (sc *SchedulerControl) myScheduler() {
	for {
		select {
		case <-sc.closing:
			return
		default:
		}

		// 分配完任务进入一个短暂的休眠等待任务执行
		//restInterval := time.NewTicker(10 * time.Second)
		//select {
		//case <-restInterval.C:
		//}

		sc.onceScheduler()
	}
}

func (sc *SchedulerControl) myCallChildProcess() {
	// step 4. child process
	for {
		select {
		case apReq := <-sc.AP:
			sc.apCp(apReq)
		case p1Req := <-sc.P1:
			sc.p1Cp(p1Req)
		case p2Req := <-sc.P2:
			sc.p2Cp(p2Req)
		case c1Req := <-sc.C1:
			sc.c1Cp(c1Req)
		case c2Req := <-sc.C2:
			sc.c2Cp(c2Req)
		case fzReq := <-sc.FZ:
			sc.finalizeSectorCp(fzReq)
		}
	}
}

func (sc *SchedulerControl) getTask() ([]*myModel.SealingTask, error) {
	sc.lk.Lock()
	defer sc.lk.Unlock()
	needSealing := cap(sc.SealingCh) - len(sc.SealingCh)
	//needApP1 := cap(sc.ApP1) - len(sc.ApP1)
	//needP2C2 := cap(sc.P2C2) - len(sc.P2C2)
	var all []*myModel.SealingTask
	sids := []uint64{}
	var err error

	// old task
	for key := range sc.SealingM {
		tasks, err := myMongo.FindBySIdStatus(key, pending)
		if err != nil {
			return []*myModel.SealingTask{}, err
		}
		all = append(all, tasks...)
		sids = append(sids, key)
	}
	// new task
	if needSealing != 0 {
		sids, err = myMongo.FindSmallerSectorId(sids, uint64(needSealing), "pending")
		if err != nil {
			return []*myModel.SealingTask{}, err
		}
		// step 2 control task count
		for _, v := range sids {
			tasks, err := myMongo.FindBySIdStatus(v, pending)
			if err != nil {
				return []*myModel.SealingTask{}, err
			}
			all = append(all, tasks...)
		}
	}
	return all, nil
}

func (sc *SchedulerControl) onceGetTask() ([]*myModel.SealingTask, error) {
	sc.lk.Lock()
	defer sc.lk.Unlock()
	needSealing := cap(sc.SealingCh) - len(sc.SealingCh)
	needApP1 := cap(sc.ApP1) - len(sc.ApP1)
	//needP2C2 := cap(sc.P2C2) - len(sc.P2C2)
	needP2 := cap(sc.P2) - len(sc.P2)
	needC1 := cap(sc.C1) - len(sc.C1)
	needC2 := cap(sc.C2) - len(sc.C2)
	needFZ := cap(sc.FZ) - len(sc.FZ)

	var all []*myModel.SealingTask
	var tasks []*myModel.SealingTask
	sids := []uint64{}
	var err error

	// old task
	for key := range sc.SealingM {
		sids = append(sids, key)
	}
	// new task
	if needSealing != 0 {
		s, err := myMongo.GetSmallerSectorId("", "", int64(needSealing))
		if err != nil {
			return []*myModel.SealingTask{}, err
		}
		myMongo.UpdateSectorsWorkerIp(s, myUtils.GetLocalIPv4s())
		sids = append(sids, s...)
	}

	tasks, err = myMongo.GetSuitableTask(sids, []string{string(sealtasks.TTAddPiece), string(sealtasks.TTPreCommit1)}, "pending", int64(needApP1))
	if err != nil {
		return nil, err
	}
	all = append(all, tasks...)

	tasks, err = myMongo.GetSuitableTask(sids, []string{string(sealtasks.TTPreCommit2)}, "pending", int64(needP2))
	if err != nil {
		return nil, err
	}
	all = append(all, tasks...)

	tasks, err = myMongo.GetSuitableTask(sids, []string{string(sealtasks.TTCommit1)}, "pending", int64(needC1))
	if err != nil {
		return nil, err
	}
	all = append(all, tasks...)

	tasks, err = myMongo.GetSuitableTask(sids, []string{string(sealtasks.TTCommit2)}, "pending", int64(needC2))
	if err != nil {
		return nil, err
	}
	all = append(all, tasks...)

	tasks, err = myMongo.GetSuitableTask(sids, []string{string(sealtasks.TTFinalize)}, "pending", int64(needFZ))
	if err != nil {
		return nil, err
	}
	all = append(all, tasks...)

	return all, nil
}

func (sc *SchedulerControl) onceScheduler() error {
	//ctx := context.TODO()
	//err := myMongo.MongoLock.XLock(ctx, "global_lock", myMongo.LockId, lock.LockDetails{})
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer func() {
	//	_, err = myMongo.MongoLock.Unlock(ctx, myMongo.LockId)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//}()

	tasks, err := sc.onceGetTask()
	if err != nil {
		return err
	}
	for _, task := range tasks {
		// step 3. resource placeholder
		if _, ok := sc.SealingM[uint64(task.SectorRef.ID.Number)]; !ok {
			outTick := time.NewTicker(10 * time.Second)
			select {
			case sc.SealingCh <- struct{}{}:
				sc.lk.Lock()
				sc.SealingM[uint64(task.SectorRef.ID.Number)] = struct{}{}
				sc.lk.Unlock()
			case <-outTick.C:
				fmt.Println("===========================================:超出sealing限制")
				continue
			}
		}
		// step 3.1 findAndModify
		// step 3.2 chan <- taskReq
		switch task.TaskType {
		case "seal/v0/addpiece":
			sc.ap(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number))
		case "seal/v0/precommit/1":
			sc.p1(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number))
		case "seal/v0/precommit/2":
			sc.p2(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number))
		case "seal/v0/commit/1":
			sc.c1(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number))
		case "seal/v0/commit/2":
			sc.c2(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number))
		case "seal/v0/finalize":
			sc.finalizeSector(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number))
		}
	}
	return nil
}

func (sc *SchedulerControl) ap(taskId primitive.ObjectID, taskType string, sid uint64) {
	outTick := time.NewTicker(10 * time.Second)
	select {
	case sc.ApP1 <- struct{}{}:
	case <-outTick.C:
		return
	}

	sc.lk.Lock()
	defer sc.lk.Unlock()

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.AP <- taskReq{
			ID:       taskId,
			TaskType: taskType,
			SectorId: sid,
		}
		return
	}
	if err != nil || !ok {
		<-sc.ApP1
		return
	}
}

func (sc *SchedulerControl) p1(taskId primitive.ObjectID, taskType string, sid uint64) {
	outTick := time.NewTicker(10 * time.Second)
	select {
	case sc.ApP1 <- struct{}{}:
	case <-outTick.C:
		return
	}

	sc.lk.Lock()
	defer sc.lk.Unlock()

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.P1 <- taskReq{
			ID:       taskId,
			TaskType: taskType,
			SectorId: sid,
		}
		return
	}
	if err != nil || !ok {
		<-sc.ApP1
		return
	}
}

func (sc *SchedulerControl) p2(taskId primitive.ObjectID, taskType string, sid uint64) {
	outTick := time.NewTicker(10 * time.Second)
	select {
	case sc.P2C2 <- struct{}{}:
	case <-outTick.C:
		return
	}

	sc.lk.Lock()
	defer sc.lk.Unlock()

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.P2 <- taskReq{
			ID:       taskId,
			TaskType: taskType,
			SectorId: sid,
		}
		return
	}
	if err != nil || !ok {
		<-sc.P2C2
		return
	}
}

func (sc *SchedulerControl) c1(taskId primitive.ObjectID, taskType string, sid uint64) {
	sc.lk.Lock()
	defer sc.lk.Unlock()
	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if err != nil {
		return
	}
	if ok {
		sc.C1 <- taskReq{
			ID:       taskId,
			TaskType: taskType,
			SectorId: sid,
		}
	}
}

func (sc *SchedulerControl) c2(taskId primitive.ObjectID, taskType string, sid uint64) {
	outTick := time.NewTicker(10 * time.Second)
	select {
	case sc.P2C2 <- struct{}{}:
	case <-outTick.C:
		return
	}

	sc.lk.Lock()
	defer sc.lk.Unlock()

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.C2 <- taskReq{
			ID:       taskId,
			TaskType: taskType,
			SectorId: sid,
		}
		return
	}
	if err != nil || !ok {
		<-sc.P2C2
		return
	}
}

func (sc *SchedulerControl) finalizeSector(taskId primitive.ObjectID, taskType string, sid uint64) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if err != nil {
		return
	}
	if ok {
		sc.FZ <- taskReq{
			ID:       taskId,
			TaskType: taskType,
			SectorId: sid,
		}
	}
}

func (sc *SchedulerControl) apCp(apReq taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", apReq.ID)
	id = strings.Split(id, `"`)[1]
	go func() {
		err := callChildProcess([]string{apReq.TaskType, id})
		if err != nil {
			myMongo.UpdateStatus(apReq.ID, "failed")
		}
		<-sc.ApP1
	}()
}

func (sc *SchedulerControl) p1Cp(p1Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", p1Req.ID)
	id = strings.Split(id, `"`)[1]
	go func() {
		err := callChildProcess([]string{p1Req.TaskType, id})
		if err != nil {
			myMongo.UpdateStatus(p1Req.ID, "failed")
		}
		<-sc.ApP1
	}()
}

func (sc *SchedulerControl) p2Cp(p2Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", p2Req.ID)
	id = strings.Split(id, `"`)[1]
	go func() {
		err := callChildProcess([]string{p2Req.TaskType, id})
		if err != nil {
			myMongo.UpdateStatus(p2Req.ID, "failed")
		}
		<-sc.P2C2
	}()
}

func (sc *SchedulerControl) c1Cp(c1Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", c1Req.ID)
	id = strings.Split(id, `"`)[1]
	go func() {
		err := callChildProcess([]string{c1Req.TaskType, id})
		if err != nil {
			myMongo.UpdateStatus(c1Req.ID, "failed")
		}
	}()
}

func (sc *SchedulerControl) c2Cp(c2Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", c2Req.ID)
	id = strings.Split(id, `"`)[1]
	go func() {
		err := callChildProcess([]string{c2Req.TaskType, id})
		if err != nil {
			myMongo.UpdateStatus(c2Req.ID, "failed")
		}
		<-sc.P2C2
	}()
}

func (sc *SchedulerControl) finalizeSectorCp(fzReq taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", fzReq.ID)
	id = strings.Split(id, `"`)[1]
	go func() {
		err := callChildProcess([]string{fzReq.TaskType, id})
		if err != nil {
			myMongo.UpdateStatus(fzReq.ID, "failed")
		}
		// TODO 1. ftp migrate
		// 2. update sector storage path
		myMongo.UpdateSectorStoragePath(fzReq.ID, "")
		myMongo.UpdateSectorStatus(fzReq.SectorId, "living")
		<-sc.SealingCh
		delete(sc.SealingM, uint64(fzReq.SectorId))
	}()
}

// 1.0
/*-------------------------------------------------------------------------------------*/

func (l *LocalWorker) myScheduler() {
	time.Sleep(30 * time.Second)
	for {
		select {
		case <-l.closing:
			return
		default:
		}

		task, err := l.myGetSuitableTask()
		if err != nil {
			continue
		}

		// TODO priority

		// TODO resource

		err = l.myAssignmentTask(task)
		if err != nil {
			return
		}

		time.Sleep(10 * time.Second)
	}
}

func (l *LocalWorker) myResourceEnough() bool {
	return true
}

func (l *LocalWorker) myGetSuitableTask() (*myModel.SealingTask, error) {
	// TODO option suitable task
	// 优先级
	// 1. 任务时间太长的
	// 2. 绑定任务
	// 3. 新任务
	//sids, err := myMongo.FindUnfinishedSectorTasks()
	//if err != nil {
	//	return nil, err
	//}
	taskType := ""
	// TODO resource placeholder
	switch taskType {
	case "":

	}

	var task *myModel.SealingTask
	tasks, err := myMongo.FindByStatus("pending")
	if err != nil {
		return nil, err
	}
	if len(tasks) != 0 {
		task = tasks[0]
		return task, nil
	}
	return nil, xerrors.New("no new task")
}

func (l *LocalWorker) myAssignmentTask(task *myModel.SealingTask) error {
	switch task.TaskType {
	case "seal/v0/addpiece":
		//err := l.myAPTask(task)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	id := fmt.Sprintf("%v", task.ID)
		//	id = strings.Split(id, `"`)[1]
		//	cmd("myApTask", id)
		//}()
		id := fmt.Sprintf("%v", task.ID)
		id = strings.Split(id, `"`)[1]
		go func() {
			callChildProcess([]string{task.TaskType, id})
		}()
	case "seal/v0/precommit/1":
		//err := l.myP1Task(task)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	id := fmt.Sprintf("%v", task.ID)
		//	id = strings.Split(id, `"`)[1]
		//	cmd("myP1Task", id)
		//}()
		id := fmt.Sprintf("%v", task.ID)
		id = strings.Split(id, `"`)[1]
		go func() {
			callChildProcess([]string{task.TaskType, id})
		}()
	case "seal/v0/precommit/2":
		//err := l.myP2Task(task)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	id := fmt.Sprintf("%v", task.ID)
		//	id = strings.Split(id, `"`)[1]
		//	cmd("myP2Task", id)
		//}()
		id := fmt.Sprintf("%v", task.ID)
		id = strings.Split(id, `"`)[1]
		go func() {
			callChildProcess([]string{task.TaskType, id})
		}()
	case "seal/v0/commit/1":
		//err := l.myC1Task(task)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	id := fmt.Sprintf("%v", task.ID)
		//	id = strings.Split(id, `"`)[1]
		//	cmd("myC1Task", id)
		//}()
		id := fmt.Sprintf("%v", task.ID)
		id = strings.Split(id, `"`)[1]
		go func() {
			callChildProcess([]string{task.TaskType, id})
		}()
	case "seal/v0/commit/2":
		//err := l.myC2Task(task)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	id := fmt.Sprintf("%v", task.ID)
		//	id = strings.Split(id, `"`)[1]
		//	cmd("myC2Task", id)
		//}()
		id := fmt.Sprintf("%v", task.ID)
		id = strings.Split(id, `"`)[1]
		go func() {
			callChildProcess([]string{task.TaskType, id})
		}()
	case "seal/v0/finalize":
		//err := l.myFinalizeSectorTask(task)
		//if err != nil {
		//	return err
		//}
		//go func() {
		//	id := fmt.Sprintf("%v", task.ID)
		//	id = strings.Split(id, `"`)[1]
		//	cmd("myFinalizeSectorTask", id)
		//}()
		id := fmt.Sprintf("%v", task.ID)
		id = strings.Split(id, `"`)[1]
		go func() {
			callChildProcess([]string{task.TaskType, id})
		}()
	default:
		return xerrors.New("myWorkerScheduler: not an executable task type")
	}
	return nil
}

func (l *LocalWorker) myAPTask(task *myModel.SealingTask) error {
	sb, err := l.executor()
	if err != nil {
		fmt.Println("l.executor() err:", err)
		return err
	}
	var param0 myModel.APParam0
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	size, _ := strconv.Atoi(task.TaskParameters[1])

	myMongo.UpdateStatus(task.ID, "running")

	piece, err := sb.AddPiece(context.TODO(), task.SectorRef, param0, abi.UnpaddedPieceSize(size), nullreader.NewNullReader(abi.UnpaddedPieceSize(size)))
	if err != nil {
		return err
	}

	strPiece, _ := json.Marshal(piece)
	myMongo.UpdateStatus(task.ID, "done")
	myMongo.UpdateResult(task.ID, string(strPiece))
	return nil
}

func (l *LocalWorker) myP1Task(task *myModel.SealingTask) error {
	sb, err := l.executor()
	if err != nil {
		fmt.Println("l.executor() err:", err)
		return err
	}

	var param0 abi.SealRandomness
	var param1 []abi.PieceInfo
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	_ = json.Unmarshal([]byte(task.TaskParameters[1]), &param1)

	p1Out, err := sb.SealPreCommit1(context.TODO(), task.SectorRef, param0, param1)
	if err != nil {
		return err
	}
	p1OutByte, _ := json.Marshal(p1Out)
	myMongo.UpdateStatus(task.ID, "done")
	myMongo.UpdateResult(task.ID, string(p1OutByte))
	return nil
}

func (l *LocalWorker) myP2Task(task *myModel.SealingTask) error {
	sb, err := l.executor()
	if err != nil {
		fmt.Println("l.executor() err:", err)
		return err
	}

	var param0 storage.PreCommit1Out
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)

	p2Out, err := sb.SealPreCommit2(context.TODO(), task.SectorRef, param0)
	if err != nil {
		return err
	}
	p2OutByte, _ := json.Marshal(p2Out)
	myMongo.UpdateStatus(task.ID, "done")
	myMongo.UpdateResult(task.ID, string(p2OutByte))
	return nil
}

func (l *LocalWorker) myC1Task(task *myModel.SealingTask) error {
	sb, err := l.executor()
	if err != nil {
		fmt.Println("l.executor() err:", err)
		return err
	}

	var param0 abi.SealRandomness
	var param1 abi.InteractiveSealRandomness
	var param2 []abi.PieceInfo
	var param3 storage.SectorCids
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	_ = json.Unmarshal([]byte(task.TaskParameters[1]), &param1)
	_ = json.Unmarshal([]byte(task.TaskParameters[2]), &param2)
	_ = json.Unmarshal([]byte(task.TaskParameters[3]), &param3)

	c1Out, err := sb.SealCommit1(context.TODO(), task.SectorRef, param0, param1, param2, param3)
	if err != nil {
		return err
	}
	c1OutByte, _ := json.Marshal(c1Out)
	myMongo.UpdateStatus(task.ID, "done")
	myMongo.UpdateResult(task.ID, string(c1OutByte))
	return nil
}

func (l *LocalWorker) myC2Task(task *myModel.SealingTask) error {
	sb, err := l.executor()
	if err != nil {
		fmt.Println("l.executor() err:", err)
		return err
	}

	var param0 storage.Commit1Out
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)

	c1Out, err := sb.SealCommit2(context.TODO(), task.SectorRef, param0)
	if err != nil {
		return err
	}
	c1OutByte, _ := json.Marshal(c1Out)
	myMongo.UpdateStatus(task.ID, "done")
	myMongo.UpdateResult(task.ID, string(c1OutByte))
	return nil
}

func (l *LocalWorker) myFinalizeSectorTask(task *myModel.SealingTask) error {
	sb, err := l.executor()
	if err != nil {
		fmt.Println("l.executor() err:", err)
		return err
	}

	var param0 []storage.Range
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)

	err = sb.FinalizeSector(context.TODO(), task.SectorRef, param0)
	if err != nil {
		myMongo.UpdateError(task.ID, err.Error())
		myMongo.UpdateStatus(task.ID, "done")
		return nil
	}
	myMongo.UpdateStatus(task.ID, "done")
	return nil
}

/*-------------------------------------------------------------------------------------*/

func cmd(taskType string, taskId string) {
	cmd := exec.Command("/home/lotus/projects/myfilecoin/lotus/lotus-worker", "tasks", taskType, taskId)
	err := cmd.Run()
	if err != nil {
		log.Fatalf("failed to call cmd.Run(): %v", err)
	}
}
