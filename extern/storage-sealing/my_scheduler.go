package sealing

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	"github.com/filecoin-project/lotus/my/myModel"
	"golang.org/x/xerrors"
	"time"
)

func (m *Sealing) myScheduler() {
	for {
		time.Sleep(10 * time.Second)

		fmt.Println("============================================go in myGetDoneTask")
		task, err := m.myGetDoneTask()
		if err != nil {
			continue
		}

		fmt.Println("============================================go in myAssignmentTask")
		err = m.myAssignmentTask(task)
		if err != nil {
			continue
		}
	}
}

func (m *Sealing) myGetDoneTask() (*myModel.SealingTask, error) {
	var task *myModel.SealingTask
	tasks, err := myMongo.FindTasks("done")
	if err != nil {
		return nil, err
	}
	if len(tasks) != 0 {
		task = tasks[0]
		return task, nil
	}
	return nil, xerrors.New("no new done task")
}

func (m *Sealing) myAssignmentTask(task *myModel.SealingTask) error {
	switch task.TaskType {
	case "seal/v0/addpiece":
		err := m.myAPDone(task)
		if err != nil {
			return err
		}
	case "":
	default:
		return xerrors.New("mySealingScheduler: not an executable task type")
	}
	return nil
}

func (m *Sealing) myAPDone(task *myModel.SealingTask) error {
	var msg []abi.PieceInfo
	_ = json.Unmarshal([]byte(task.TaskResult), &msg)
	err := m.sectors.Send(task.SectorId, SectorPacked{FillerPieces: msg})
	if err != nil {
		fmt.Println("-------------------------------err!!!:::", err)
		return err
	}

	myMongo.UpdateStatus(task.ID, "send")

	fmt.Println("============================================sended msg")
	return nil
}
