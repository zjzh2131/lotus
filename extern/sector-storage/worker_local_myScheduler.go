package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/storage-sealing/lib/nullreader"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	migration "github.com/filecoin-project/lotus/my/migrate"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	lock "github.com/square/mongo-lock"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/xerrors"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var sc *SchedulerControl
var myRs *MyResourceScheduler

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
	ID        primitive.ObjectID
	TaskType  string
	SectorId  uint64
	RSProof   abi.RegisteredSealProof
	SectorRef storage.SectorRef
}

func (sc *SchedulerControl) myScheduler() {
	for {
		select {
		case <-sc.closing:
			return
		default:
		}

		// 分配完任务进入一个短暂的休眠等待任务执行
		//restInterval := time.NewTicker(1 * time.Second)
		//select {
		//case <-restInterval.C:
		//}

		sc.onceScheduler()
	}
}

func (sc *SchedulerControl) myCallChildProcess() {
	// step 4. child process
	for {
		time.Sleep(10 * time.Second)
		sc.lk.Lock()
		tasks := []taskReq{}
		lap := len(sc.AP)
		lp1 := len(sc.P1)
		lp2 := len(sc.P2)
		lc1 := len(sc.C1)
		lc2 := len(sc.C2)
		lfz := len(sc.FZ)
		if lap|lp1|lp2|lc1|lc2|lfz == 0 {
			sc.lk.Unlock()
			continue
		}
		for i := 0; i < lap; i++ {
			select {
			case task := <-sc.AP:
				tasks = append(tasks, task)
			default:
			}
		}
		for i := 0; i < lp1; i++ {
			select {
			case task := <-sc.P1:
				tasks = append(tasks, task)
			default:
			}
		}
		for i := 0; i < lp2; i++ {
			select {
			case task := <-sc.P2:
				tasks = append(tasks, task)
			default:
			}
		}
		for i := 0; i < lc1; i++ {
			select {
			case task := <-sc.C1:
				tasks = append(tasks, task)
			default:
			}
		}
		for i := 0; i < lc2; i++ {
			select {
			case task := <-sc.C2:
				tasks = append(tasks, task)
			default:
			}
		}
		for i := 0; i < lfz; i++ {
			select {
			case task := <-sc.FZ:
				tasks = append(tasks, task)
			default:
			}
		}
		fmt.Println("-=================================================tmpcp")
		sc.tmpCp(tasks)
		sc.lk.Unlock()
		//select {
		//case apReq := <-sc.AP:
		//	sc.Cp(apReq)
		//case p1Req := <-sc.P1:
		//	sc.Cp(p1Req)
		//case p2Req := <-sc.P2:
		//	sc.Cp(p2Req)
		//case c1Req := <-sc.C1:
		//	sc.Cp(c1Req)
		//case c2Req := <-sc.C2:
		//	sc.Cp(c2Req)
		//case fzReq := <-sc.FZ:
		//	sc.Cp(fzReq)
		//}
	}
}

func (sc *SchedulerControl) tmpCp(tasks []taskReq) {
	go func() {
		if len(tasks) == 0 {
			return
		}
		taskTypes := []string{}
		ids := []string{}
		for _, task := range tasks {
			id := fmt.Sprintf("%v", task.ID)
			id = strings.Split(id, `"`)[1]
			ids = append(ids, id)
			taskTypes = append(taskTypes, task.TaskType)
		}
		fmt.Println("================================================qwjoidnqwudoqwnuoqwo")
		err := callCp(strings.Join(taskTypes, ","), "58,59", "6", strings.Join(ids, ","), "")
		if err != nil {
			fmt.Println(err)
			//myMongo.UpdateStatus(task.ID, "failed")
		}
	}()
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
		s, err := myMongo.GetSmallerSectorId("", []string{myUtils.GetLocalIPv4s(), ""}, int64(needSealing))
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
	ctx := context.TODO()
	err := myMongo.MongoLock.XLock(ctx, myMongo.LockResourceName, myMongo.LockId, lock.LockDetails{})
	if err != nil {
		return err
	}
	defer func() {
		_, err = myMongo.MongoLock.Unlock(ctx, myMongo.LockId)
		if err != nil {
			log.Warn(err)
		}
	}()

	sc.lk.Lock()
	defer sc.lk.Unlock()

	tasks, err := sc.onceGetTask()
	if err != nil {
		return err
	}
	tmpP1Tasks := []*myModel.SealingTask{}
	for k, v := range tasks {
		if v.TaskType == "seal/v0/precommit/1" {
			tmpP1Tasks = append(tmpP1Tasks, tasks[k])
			if k != len(tasks)-1 {
				tasks = append(tasks[:k], tasks[k+1:]...)
			} else {
				tasks = tasks[:k]
			}
		}
	}
	for {
		if len(tmpP1Tasks) >= 5 {
			tasks = append(tasks, tmpP1Tasks...)
		}
		break
	}
	for _, task := range tasks {
		log.Infof("get task, objId: [%v], sector_id: [%v], task_type: [%v]\n", task.ID.String(), task.SectorRef.ID.Number, task.TaskType)
		// step 3. resource placeholder
		if _, ok := sc.SealingM[uint64(task.SectorRef.ID.Number)]; !ok {
			outTick := time.NewTicker(1 * time.Second)
			select {
			case sc.SealingCh <- struct{}{}:
				sc.SealingM[uint64(task.SectorRef.ID.Number)] = struct{}{}
			case <-outTick.C:
				fmt.Println("===========================================:超出sealing限制")
				continue
			}
		}
		// step 3.1 findAndModify
		// step 3.2 chan <- taskReq
		switch task.TaskType {
		case "seal/v0/addpiece":
			sc.ap(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number), task.SectorRef)
		case "seal/v0/precommit/1":
			sc.p1(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number), task.SectorRef)
		case "seal/v0/precommit/2":
			sc.p2(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number), task.SectorRef)
		case "seal/v0/commit/1":
			sc.c1(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number), task.SectorRef.ProofType, task.SectorRef)
		case "seal/v0/commit/2":
			sc.c2(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number), task.SectorRef)
		case "seal/v0/finalize":
			sc.finalizeSector(task.ID, task.TaskType, uint64(task.SectorRef.ID.Number), task.SectorRef.ProofType, task.SectorRef)
		}
		fmt.Println(sc.String())
	}
	return nil
}

func (sc *SchedulerControl) ap(taskId primitive.ObjectID, taskType string, sid uint64, sectorRef storage.SectorRef) {
	outTick := time.NewTicker(1 * time.Second)
	select {
	case sc.ApP1 <- struct{}{}:
	case <-outTick.C:
		return
	}

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.AP <- taskReq{
			ID:        taskId,
			TaskType:  taskType,
			SectorId:  sid,
			SectorRef: sectorRef,
		}
		return
	}
	if err != nil || !ok {
		<-sc.ApP1
		return
	}
}

func (sc *SchedulerControl) p1(taskId primitive.ObjectID, taskType string, sid uint64, sectorRef storage.SectorRef) {
	outTick := time.NewTicker(1 * time.Second)
	select {
	case sc.ApP1 <- struct{}{}:
	case <-outTick.C:
		return
	}

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.P1 <- taskReq{
			ID:        taskId,
			TaskType:  taskType,
			SectorId:  sid,
			SectorRef: sectorRef,
		}
		return
	}
	if err != nil || !ok {
		<-sc.ApP1
		return
	}
}

func (sc *SchedulerControl) p2(taskId primitive.ObjectID, taskType string, sid uint64, sectorRef storage.SectorRef) {
	outTick := time.NewTicker(1 * time.Second)
	select {
	case sc.P2C2 <- struct{}{}:
	case <-outTick.C:
		return
	}

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.P2 <- taskReq{
			ID:        taskId,
			TaskType:  taskType,
			SectorId:  sid,
			SectorRef: sectorRef,
		}
		return
	}
	if err != nil || !ok {
		<-sc.P2C2
		return
	}
}

func (sc *SchedulerControl) c1(taskId primitive.ObjectID, taskType string, sid uint64, rsProof abi.RegisteredSealProof, sectorRef storage.SectorRef) {
	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if err != nil {
		return
	}
	if ok {
		sc.C1 <- taskReq{
			ID:        taskId,
			TaskType:  taskType,
			SectorId:  sid,
			SectorRef: sectorRef,
		}
	}
}

func (sc *SchedulerControl) c2(taskId primitive.ObjectID, taskType string, sid uint64, sectorRef storage.SectorRef) {
	outTick := time.NewTicker(1 * time.Second)
	select {
	case sc.P2C2 <- struct{}{}:
	case <-outTick.C:
		return
	}

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if ok && err == nil {
		sc.C2 <- taskReq{
			ID:        taskId,
			TaskType:  taskType,
			SectorId:  sid,
			SectorRef: sectorRef,
		}
		return
	}
	if err != nil || !ok {
		<-sc.P2C2
		return
	}
}

func (sc *SchedulerControl) finalizeSector(taskId primitive.ObjectID, taskType string, sid uint64, rsProof abi.RegisteredSealProof, sectorRef storage.SectorRef) {

	ok, err := myMongo.FindAndModifyForStatus(taskId, "pending", "running")
	if err != nil {
		return
	}
	if ok {
		sc.FZ <- taskReq{
			ID:        taskId,
			TaskType:  taskType,
			SectorId:  sid,
			RSProof:   rsProof,
			SectorRef: sectorRef,
		}
	}
}

func (sc *SchedulerControl) Cp(task taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", task.ID)
	id = strings.Split(id, `"`)[1]

	sector, _ := json.Marshal(task)
	go func() {
		switch task.TaskType {
		case "seal/v0/addpiece":
			defer func() { <-sc.ApP1 }()
		case "seal/v0/precommit/1":
			defer func() { <-sc.ApP1 }()
		case "seal/v0/precommit/2":
			defer func() { <-sc.P2C2 }()
		case "seal/v0/commit/1":
		case "seal/v0/commit/2":
			defer func() { <-sc.P2C2 }()
		case "seal/v0/finalize":
			defer func() {
				<-sc.SealingCh
				delete(sc.SealingM, uint64(task.SectorId))
			}()
		default:
		}
		var cpusStr []string
		bound, freed, ok := myRs.GetNuma(task, tasksNeedNumaResource[task.TaskType].cpuCount)
		if !ok {
			myMongo.UpdateStatus(task.ID, "failed")
		}
		defer freed()
		for _, v := range bound.cpus {
			cpusStr = append(cpusStr, strconv.Itoa(v))
		}

		log.Infof("child process start: SectorId(%v), TaskType(%v)\n", task.SectorId, task.TaskType)
		//err := callChildProcess([]string{task.TaskType, id, string(sector), strings.Join(cpusStr, ","), strconv.Itoa(bound.nodeId)})
		err := callCp(task.TaskType, strings.Join(cpusStr, ","), strconv.Itoa(bound.nodeId), id, string(sector))
		if err != nil {
			myMongo.UpdateStatus(task.ID, "failed")
		}
		log.Infof("child process end: SectorId(%v), TaskType(%v)\n", task.SectorId, task.TaskType)
	}()
}

func (sc *SchedulerControl) apCp(apReq taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", apReq.ID)
	id = strings.Split(id, `"`)[1]

	sector, _ := json.Marshal(apReq)
	go func() {
		defer func() {
			<-sc.ApP1
		}()

		log.Infof("apCp start: SectorId(%v)\n", apReq.SectorId)
		err := callChildProcess([]string{apReq.TaskType, id, string(sector)})
		if err != nil {
			myMongo.UpdateStatus(apReq.ID, "failed")
		}
		log.Infof("apCp end: SectorId(%v)\n", apReq.SectorId)
	}()
}

func (sc *SchedulerControl) p1Cp(p1Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", p1Req.ID)
	id = strings.Split(id, `"`)[1]

	sector, _ := json.Marshal(p1Req)
	go func() {
		defer func() {
			<-sc.ApP1
		}()

		log.Infof("p1Cp start: SectorId(%v)\n", p1Req.SectorId)
		err := callChildProcess([]string{p1Req.TaskType, id, string(sector)})
		if err != nil {
			myMongo.UpdateStatus(p1Req.ID, "failed")
		}
		log.Infof("p1Cp end: SectorId(%v)\n", p1Req.SectorId)
	}()
}

func (sc *SchedulerControl) p2Cp(p2Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", p2Req.ID)
	id = strings.Split(id, `"`)[1]

	sector, _ := json.Marshal(p2Req)
	go func() {
		defer func() {
			<-sc.P2C2
		}()

		log.Infof("p2Cp start: SectorId(%v)\n", p2Req.SectorId)
		err := callChildProcess([]string{p2Req.TaskType, id, string(sector)})
		if err != nil {
			myMongo.UpdateStatus(p2Req.ID, "failed")
		}
		log.Infof("p2Cp end: SectorId(%v)\n", p2Req.SectorId)
	}()
}

func (sc *SchedulerControl) c1Cp(c1Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", c1Req.ID)
	id = strings.Split(id, `"`)[1]

	sector, _ := json.Marshal(c1Req)
	go func() {
		log.Infof("c1Cp start: SectorId(%v)\n", c1Req.SectorId)
		err := callChildProcess([]string{c1Req.TaskType, id, string(sector)})
		if err != nil {
			myMongo.UpdateStatus(c1Req.ID, "failed")
		}
		log.Infof("c1Cp end: SectorId(%v)\n", c1Req.SectorId)
	}()
}

func (sc *SchedulerControl) c2Cp(c2Req taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", c2Req.ID)
	id = strings.Split(id, `"`)[1]

	sector, _ := json.Marshal(c2Req)
	go func() {
		defer func() {
			<-sc.P2C2
		}()

		log.Infof("c2Cp start: SectorId(%v)\n", c2Req.SectorId)
		err := callChildProcess([]string{c2Req.TaskType, id, string(sector)})
		if err != nil {
			myMongo.UpdateStatus(c2Req.ID, "failed")
		}
		log.Infof("c2Cp end: SectorId(%v)\n", c2Req.SectorId)
	}()
}

func (sc *SchedulerControl) finalizeSectorCp(fzReq taskReq) {
	sc.lk.Lock()
	defer sc.lk.Unlock()

	id := fmt.Sprintf("%v", fzReq.ID)
	id = strings.Split(id, `"`)[1]

	sector, _ := json.Marshal(fzReq)
	go func() {
		defer func() {
			<-sc.SealingCh
			delete(sc.SealingM, uint64(fzReq.SectorId))
		}()

		log.Infof("finalizeSectorCp start: SectorId(%v)\n", fzReq.SectorId)
		err := callChildProcess([]string{fzReq.TaskType, id, string(sector)})
		if err != nil {
			myMongo.UpdateStatus(fzReq.ID, "failed")
			return
		}
		//myMongo.UpdateSectorStatus(fzReq.SectorId, "living")
		log.Infof("finalizeSectorCp end: SectorId(%v)\n", fzReq.SectorId)

		//log.Infof("Migrate start: SectorId(%v)\n", fzReq.SectorId)
		//err = Migrate(fzReq)
		//if err != nil {
		//	fmt.Println("Migrate err:", err)
		//	myMongo.UpdateStatus(fzReq.ID, "failed")
		//	return
		//}
		//log.Infof("Migrate end: SectorId(%v)\n", fzReq.SectorId)
	}()
}

func (sc *SchedulerControl) String() string {
	var sealingMArr []string
	for key := range sc.SealingM {
		e := fmt.Sprintf("SectorId(%d)", key)
		sealingMArr = append(sealingMArr, e)
	}
	sealingMStr := "sealing sector:" + "[" + strings.Join(sealingMArr, ",") + "]\n"
	sealingStr := fmt.Sprintf("sealing len: %d, cap: %d\n", len(sc.SealingCh), cap(sc.SealingCh))

	apP1Str := fmt.Sprintf("apP1 len: %d, cap: %d\n", len(sc.ApP1), cap(sc.ApP1))
	p2c2Str := fmt.Sprintf("p2C2 len: %d, cap: %d\n", len(sc.P2C2), cap(sc.P2C2))

	aPStr := fmt.Sprintf("ap len: %d, cap: %d\n", len(sc.AP), cap(sc.AP))
	p1Str := fmt.Sprintf("p1 len: %d, cap: %d\n", len(sc.P1), cap(sc.P1))
	p2Str := fmt.Sprintf("p2 len: %d, cap: %d\n", len(sc.P2), cap(sc.P2))
	c1Str := fmt.Sprintf("c1 len: %d, cap: %d\n", len(sc.C1), cap(sc.C1))
	c2Str := fmt.Sprintf("c2 len: %d, cap: %d\n", len(sc.C2), cap(sc.C2))
	fzStr := fmt.Sprintf("fz len: %d, cap: %d\n", len(sc.FZ), cap(sc.FZ))
	return sealingMStr + sealingStr + apP1Str + p2c2Str + aPStr + p1Str + p2Str + c1Str + c2Str + fzStr
}

func Migrate(sector storage.SectorRef) error {
	// TODO ftp Migrate
	sid, err := myMongo.FindSectorsBySid(uint64(sector.ID.Number))
	if err != nil {
		return err
	}
	folder := fmt.Sprintf("s-t0%v-%v", sector.ID.Miner, sector.ID.Number)
	ip := myUtils.GetLocalIPv4s()
	workerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   ip,
		"role": "worker",
	})
	if err != nil {
		return err
	}
	if workerMachine == nil {
		return xerrors.New("不存在该worker")
	}
	storageMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   sid.StorageIp,
		"role": "storage",
	})
	if sid == nil || workerMachine == nil || storageMachine == nil {
		return xerrors.New("sid/workerMachine/storageMachine is empty")
	}
	p := migration.MigrateParam{
		SectorID:  folder,
		FromIP:    ip,
		FromPath:  workerMachine.WorkerLocalPath,
		StoreIP:   sid.StorageIp,
		StorePath: sid.StoragePath,
		FtpEnv: migration.FtpEnvStruct{
			FtpPort:     storageMachine.FtpEnv.FtpPort,
			FtpUser:     storageMachine.FtpEnv.FtpUser,
			FtpPassword: storageMachine.FtpEnv.FtpPassword,
		},
	}
	err = migration.MigrateWithFtp(p, sector.ProofType)
	if err != nil {
		fmt.Println("===========================================ftp err:", err)
		return err
	}
	// 2. update sector storage path
	//myMongo.UpdateSectorStatus(fzReq.SectorId, "migrated")
	return nil
}

func MigrateC1out(sectorRef storage.SectorRef) error {
	folder := fmt.Sprintf("s-t0%v-%v", sectorRef.ID.Miner, sectorRef.ID.Number)

	ip := myUtils.GetLocalIPv4s()
	workerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   ip,
		"role": "worker",
	})
	if err != nil {
		return err
	}
	if workerMachine == nil {
		return xerrors.New("不存在该worker")
	}
	storageMachine, err := myMongo.FindOneMachine(bson.M{
		"role": "tmp_storage",
	})
	if workerMachine == nil || storageMachine == nil {
		return xerrors.New("workerMachine/storageMachine is empty")
	}
	p := migration.MigrateParam{
		SectorID:  folder,
		FromIP:    ip,
		FromPath:  workerMachine.WorkerLocalPath,
		StoreIP:   storageMachine.Ip,
		StorePath: storageMachine.StoragePath,
		FtpEnv: migration.FtpEnvStruct{
			FtpPort:     storageMachine.FtpEnv.FtpPort,
			FtpUser:     storageMachine.FtpEnv.FtpUser,
			FtpPassword: storageMachine.FtpEnv.FtpPassword,
		},
	}
	err = migration.MigrateC1Out(&p)
	if err != nil {
		fmt.Println("===========================================ftp err:", err)
		return err
	}
	return nil
}

func migrateC2out(sectorRef storage.SectorRef) error {
	folder := fmt.Sprintf("s-t0%v-%v", sectorRef.ID.Miner, sectorRef.ID.Number)

	ip := myUtils.GetLocalIPv4s()
	workerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   ip,
		"role": "worker",
	})
	if err != nil {
		return err
	}
	if workerMachine == nil {
		return xerrors.New("不存在该worker")
	}
	storageMachine, err := myMongo.FindOneMachine(bson.M{
		"role": "tmp_storage",
	})
	if workerMachine == nil || storageMachine == nil {
		return xerrors.New("workerMachine/storageMachine is empty")
	}
	p := migration.MigrateParam{
		SectorID:  folder,
		FromIP:    ip,
		FromPath:  workerMachine.WorkerLocalPath,
		StoreIP:   storageMachine.Ip,
		StorePath: storageMachine.StoragePath,
		FtpEnv: migration.FtpEnvStruct{
			FtpPort:     storageMachine.FtpEnv.FtpPort,
			FtpUser:     storageMachine.FtpEnv.FtpUser,
			FtpPassword: storageMachine.FtpEnv.FtpPassword,
		},
	}
	err = migration.MigrateC2Out(&p)
	if err != nil {
		fmt.Println("===========================================ftp err:", err)
		return err
	}
	return nil
}

func migrateAddPiece(sectorRef storage.SectorRef, fileName string) error {
	folder := fmt.Sprintf("s-t0%v-%v", sectorRef.ID.Miner, sectorRef.ID.Number)
	ip := myUtils.GetLocalIPv4s()
	minerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   ip,
		"role": "miner",
	})
	if err != nil {
		return err
	}
	if minerMachine == nil {
		return xerrors.New("不存在该miner")
	}
	storageMachine, err := myMongo.FindOneMachine(bson.M{
		"role": "tmp_storage",
	})
	if minerMachine == nil || storageMachine == nil {
		return xerrors.New("workerMachine/storageMachine is empty")
	}

	err = migration.MigrateAddPiece(folder, ip, minerMachine.MinerLocalPath, storageMachine.Ip, storageMachine.StoragePath,
		storageMachine.FtpEnv.FtpPort, storageMachine.FtpEnv.FtpUser, storageMachine.FtpEnv.FtpPassword, fileName)
	if err != nil {
		fmt.Println("===========================================ftp err:", err)
		return err
	}
	return nil
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

		time.Sleep(1 * time.Second)
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
