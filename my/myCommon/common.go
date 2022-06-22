package myCommon

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"sync"
	"time"
)

func WaitResult(wg *sync.WaitGroup, sectorId uint64, taskType, taskStatus string, out interface{}) error {
	defer wg.Done()
	heartbeat := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-heartbeat.C:
			// step1: get task
			var task *myModel.SealingTask
			tasks, err := myMongo.FindBySIdTypeStatus(sectorId, taskType, taskStatus)
			if err != nil {
				return err
			}
			if len(tasks) == 0 {
				continue
			}

			// step2: write out
			task = tasks[0]
			ok, err := assignmentOut(task, out)
			if ok {
				_ = myMongo.UpdateStatus(task.ID, "finish")
				if err != nil {
					return err
				}
				return nil
			}
		}
	}
}

func assignmentOut(task *myModel.SealingTask, out interface{}) (bool, error) {
	switch out.(type) {
	// AddPiece
	case *abi.PieceInfo:
		err := json.Unmarshal([]byte(task.TaskResult), out.(*abi.PieceInfo))
		if err != nil {
			return false, err
		}
	// SealPreCommit1
	case *storage.PreCommit1Out:
		err := json.Unmarshal([]byte(task.TaskResult), out.(*storage.PreCommit1Out))
		if err != nil {
			return false, err
		}
	// SealPreCommit2
	case *storage.SectorCids:
		err := json.Unmarshal([]byte(task.TaskResult), out.(*storage.SectorCids))
		if err != nil {
			return false, err
		}
	// SealCommit1
	case *storage.Commit1Out:
		err := json.Unmarshal([]byte(task.TaskResult), out.(*storage.Commit1Out))
		if err != nil {
			return false, err
		}
	// SealCommit2
	case *storage.Proof:
		err := json.Unmarshal([]byte(task.TaskResult), out.(*storage.Proof))
		if err != nil {
			return false, err
		}
	// FinalizeSector
	case *myModel.MyFinalizeSectorOut:
		if task.TaskError != "" {
			return true, xerrors.New(task.TaskError)
		}
	default:
		return false, xerrors.New("myScheduler: This type is not triggered")
	}
	return true, nil
}

func MountAllStorage() {
	machines, err := myMongo.FindMachineByRole("storage")
	if err != nil {
		return
	}
	for _, v := range machines {
		// 	cmd := exec.Command("sudo", "mount", "192.168.0.128:/data/nfs", "/data/mount_nfs1")
		nfsServer := v.Ip + ":" + v.StoragePath
		nfsPath := "/data/mount/" + v.Ip
		exists, err := myUtils.PathExists(nfsPath)
		if err != nil {
			e := fmt.Sprintf("mount folder creation error, path: %v\n", nfsPath)
			panic(e)
			return
		}
		if exists {
			err := myUtils.MountNfs(nfsPath, nfsServer)
			if err != nil {
				e := fmt.Sprintf("mount error, nfsPath: %v, nfsServer: %v\n", nfsPath, nfsServer)
				panic(e)
			}
		}
	}
}
