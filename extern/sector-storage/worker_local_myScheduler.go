package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/storage-sealing/lib/nullreader"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var (
	runningAp = 0
	runningP1 = 0
	runningP2 = 0
	runningC1 = 0
	runningC2 = 0
)

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

// 1.0
/*-------------------------------------------------------------------------------------*/

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
