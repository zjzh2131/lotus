package myCommon

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	migration "github.com/filecoin-project/lotus/my/migrate"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/xerrors"
	"path/filepath"
	"sync"
	"time"
)

func WaitResult(wg *sync.WaitGroup, sectorId uint64, taskType string, taskStatus []string, out interface{}) error {
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
			if task.TaskStatus == "failed" {
				// TODO
				if task.TaskType == "seal/v0/finalize" {
					sid, err := myMongo.FindSectorsBySid(sectorId)
					if err != nil {
						return err
					}
					if err := migration.CancelStoreMachine(context.TODO(), &migration.MigrateTasks{StorePath: sid.StoragePath, StoreIP: sid.StorageIp}); err != nil {
						fmt.Println("CancelStoreMachine err ", err)
					}
					fmt.Println("[=====================================CancelStoreMachine Success=====================================]")
				}
				e := fmt.Sprintf("child process run task failed, ObjId: [%v], taskType: [%v], taskError: [%v]", task.ID, task.TaskType, task.TaskError)
				return xerrors.New(e)
			}
			if task.TaskType == "seal/v0/finalize" {
				sid, err := myMongo.FindSectorsBySid(sectorId)
				if err != nil {
					return err
				}
				if err := migration.DoneStoreMachine(context.TODO(), &migration.MigrateTasks{StorePath: sid.StoragePath, StoreIP: sid.StorageIp}); err != nil {
					fmt.Println("DoneStoreMachine err ", err)
				}
				fmt.Println("[=====================================DoneStoreMachine Success=====================================]")
			}
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

func assignmentOut(task *myModel.SealingTask, out interface{}) (ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			wrappedError := fmt.Errorf("recover for error: %w", r)
			err = wrappedError
		}
	}()
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
		minerMachine, err := myMongo.FindOneMachine(bson.M{
			"ip":   myUtils.GetLocalIPv4s(),
			"role": "miner",
		})
		tmpStorageMachine, err := myMongo.FindOneMachine(bson.M{
			"role": "tmp_storage",
		})
		folder := fmt.Sprintf("s-t0%v-%v", task.SectorRef.ID.Miner, task.SectorRef.ID.Number)
		filePath := filepath.Join(minerMachine.MinerMountPath, tmpStorageMachine.Ip, "c1Out", folder, "c1Out")
		c1OutByte, err := migration.ReadDataFromFile(filePath)
		if err != nil {
			return false, err
		}

		err = json.Unmarshal(c1OutByte, out.(*storage.Commit1Out))
		if err != nil {
			return false, err
		}
	// SealCommit2
	case *storage.Proof:
		err = json.Unmarshal([]byte(task.TaskResult), out.(*storage.Proof))
		if err != nil {
			return false, err
		}
	// FinalizeSector
	case *myModel.MyFinalizeSectorOut:
		if task.TaskResult != "" {
			return true, xerrors.New(task.TaskError)
		}
	case *storiface.WindowPoStResult:
		err = json.Unmarshal([]byte(task.TaskResult), out.(*storiface.WindowPoStResult))
		if err != nil {
			return false, err
		}
	default:
		return false, xerrors.New("myScheduler: This type is not triggered")
	}
	return true, nil
}

func PreHandleTask(sector storage.SectorRef, taskType sealtasks.TaskType, taskStatus string) error {
	// step 1 count document
	count, err := myMongo.CountTaskBySidStatus(uint64(sector.ID.Number), string(taskType), taskStatus)
	if err != nil {
		return err
	}
	// step 2 del
	if count > 0 {
		delTaskType := myModel.DelTaskInfo[taskType]
		if len(delTaskType) != 0 {
			// del
			err := myMongo.DelTasks(sector, delTaskType, "pending")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func MountAllStorage() {
	machines, err := myMongo.FindMachine(bson.M{
		"role": bson.M{"$in": []string{"storage", "tmp_storage"}},
	})
	if err != nil {
		return
	}
	filter := bson.M{
		"ip":   myUtils.GetLocalIPv4s(),
		"role": "miner",
	}
	miner, err := myMongo.FindOneMachine(filter)
	if err != nil || miner == nil || miner.MinerMountPath == "" {
		panic("miner machine info err")
	}
	for _, v := range machines {
		nfsServer := v.Ip + ":" + v.StoragePath
		nfsPath := filepath.Join(miner.MinerMountPath, v.Ip)
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
