package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/storage-sealing/lib/nullreader"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myUtils"
	"golang.org/x/xerrors"
	"strconv"
	"time"
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
		err := l.myAPTask(task)
		if err != nil {
			return err
		}
	case "seal/v0/precommit/1":
		err := l.myP1Task(task)
		if err != nil {
			return err
		}
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
	fmt.Println("--------------------------------------p0----------------------------------", param0)
	fmt.Println("--------------------------------------p1----------------------------------", size)

	myMongo.UpdateStatus(task.ID, "running")

	piece, err := sb.AddPiece(context.TODO(), task.SectorRef, param0, abi.UnpaddedPieceSize(size), nullreader.NewNullReader(abi.UnpaddedPieceSize(size)))
	if err != nil {
		return err
	}

	strPiece, _ := json.Marshal(piece)
	myMongo.UpdateStatus(task.ID, "done")
	myMongo.UpdateResult(task.ID, string(strPiece))
	fmt.Println("======================================out======================================", piece)
	myUtils.WriteFileString("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
	myUtils.WriteFileString(string(strPiece))
	myUtils.WriteFileString("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
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
	fmt.Println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-p1Out:", p1Out)
	p1OutByte, _ := json.Marshal(p1Out)
	myMongo.UpdateStatus(task.ID, "done")
	myMongo.UpdateResult(task.ID, string(p1OutByte))
	return nil
}
