package sectorstorage

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	migration "github.com/filecoin-project/lotus/my/migrate"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
	"path/filepath"
	"time"
)

import (
	"encoding/json"
	"github.com/docker/docker/pkg/reexec"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/storage-sealing/lib/nullreader"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myReexec"
	"os"
	"strconv"
)

func init() {
	myReexec.Register("seal/v0/addpiece", func() error {
		log.Infof("ap child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
		var err error
		taskId := os.Args[1]
		err = ap(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/precommit/1", func() error {
		log.Infof("p1 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
		var err error
		taskId := os.Args[1]
		err = p1(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/precommit/2", func() error {
		log.Infof("p2 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
		var err error
		taskId := os.Args[1]
		err = p2(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/commit/1", func() error {
		log.Infof("c1 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
		var err error
		taskId := os.Args[1]
		err = c1(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/commit/2", func() error {
		log.Infof("c2 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
		var err error
		taskId := os.Args[1]
		err = c2(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/finalize", func() error {
		log.Infof("fz child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
		var err error
		taskId := os.Args[1]
		err = fs(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	if myReexec.Init() {
		os.Exit(0)
	}
}

func callChildProcess(args []string) error {
	cmd := reexec.Command(args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func ap(taskId string) (err error) {
	var resultError error
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			log.Infof("Recovered in ap: %v", r)
			wrappedError := fmt.Errorf("recover for error: %w", r)
			resultError = multierror.Append(resultError, wrappedError)
			err = wrappedError
		}

		if resultError != nil {
			update := bson.M{}
			update["$set"] = bson.D{
				bson.E{Key: "task_error", Value: resultError.Error()},
			}
			myMongo.UpdateTask(bson.M{"_id": task.ID}, update)
		}
	}()

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	var param0 myModel.APParam0
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	size, err := strconv.Atoi(task.TaskParameters[1])
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	piece, err := sb.AddPiece(context.TODO(), task.SectorRef, param0, abi.UnpaddedPieceSize(size), nullreader.NewNullReader(abi.UnpaddedPieceSize(size)))
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	strPiece, err := json.Marshal(piece)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	resultError = multierror.Append(resultError, err)

	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(strPiece))
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	return nil
}

func p1(taskId string) (err error) {
	var resultError error
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			log.Infof("Recovered in p1: %v", r)
			wrappedError := fmt.Errorf("recover for error: %w", r)
			resultError = multierror.Append(resultError, wrappedError)
			err = wrappedError
		}

		if resultError != nil {
			update := bson.M{}
			update["$set"] = bson.D{
				bson.E{Key: "task_error", Value: resultError.Error()},
			}
			myMongo.UpdateTask(bson.M{"_id": task.ID}, update)
		}
	}()

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	var param0 abi.SealRandomness
	var param1 []abi.PieceInfo
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	resultError = multierror.Append(resultError, err)
	err = json.Unmarshal([]byte(task.TaskParameters[1]), &param1)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	p1Out, err := sb.SealPreCommit1(context.TODO(), task.SectorRef, param0, param1)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	p1OutByte, err := json.Marshal(p1Out)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	resultError = multierror.Append(resultError, err)
	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(p1OutByte))
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	return nil
}

func p2(taskId string) (err error) {
	var resultError error
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			log.Infof("Recovered in p2: %v", r)
			wrappedError := fmt.Errorf("recover for error: %w", r)
			resultError = multierror.Append(resultError, wrappedError)
			err = wrappedError
		}

		if resultError != nil {
			update := bson.M{}
			update["$set"] = bson.D{
				bson.E{Key: "task_error", Value: resultError.Error()},
			}
			myMongo.UpdateTask(bson.M{"_id": task.ID}, update)
		}
	}()

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	var param0 storage.PreCommit1Out
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	p2Out, err := sb.SealPreCommit2(context.TODO(), task.SectorRef, param0)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	p2OutByte, err := json.Marshal(p2Out)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	resultError = multierror.Append(resultError, err)

	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(p2OutByte))
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	return nil
}

func c1(taskId string) (err error) {
	var resultError error
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			log.Infof("Recovered in c1: %v", r)
			wrappedError := fmt.Errorf("recover for error: %w", r)
			resultError = multierror.Append(resultError, wrappedError)
			err = wrappedError
		}

		if resultError != nil {
			update := bson.M{}
			update["$set"] = bson.D{
				bson.E{Key: "task_error", Value: resultError.Error()},
			}
			myMongo.UpdateTask(bson.M{"_id": task.ID}, update)
		}
	}()

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	var param0 abi.SealRandomness
	var param1 abi.InteractiveSealRandomness
	var param2 []abi.PieceInfo
	var param3 storage.SectorCids
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	resultError = multierror.Append(resultError, err)
	err = json.Unmarshal([]byte(task.TaskParameters[1]), &param1)
	resultError = multierror.Append(resultError, err)
	err = json.Unmarshal([]byte(task.TaskParameters[2]), &param2)
	resultError = multierror.Append(resultError, err)
	err = json.Unmarshal([]byte(task.TaskParameters[3]), &param3)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	c1Out, err := sb.SealCommit1(context.TODO(), task.SectorRef, param0, param1, param2, param3)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	// TODO
	//c1OutByte, err := json.Marshal(c1Out)
	//if err != nil {
	//	resultError = multierror.Append(resultError, err)
	//	return err
	//}
	// step 1 write c1out
	workerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   myUtils.GetLocalIPv4s(),
		"role": "worker",
	})
	folder := fmt.Sprintf("s-t0%v-%v", task.SectorRef.ID.Miner, task.SectorRef.ID.Number)
	filePath := filepath.Join(workerMachine.WorkerLocalPath, "c1Out", folder, "c1Out")
	err = migration.WriteDataToFile(filePath, c1Out)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	// step ftp migrate
	err = migrateC1out(task.SectorRef)
	if err != nil {
		return err
	}

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	resultError = multierror.Append(resultError, err)
	// TODO

	err = myMongo.UpdateTaskResStatus(task.ID, "done", "")
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	return nil
}

func c2(taskId string) (err error) {
	var resultError error
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			log.Infof("Recovered in c2: %v", r)
			wrappedError := fmt.Errorf("recover for error: %w", r)
			resultError = multierror.Append(resultError, wrappedError)
			err = wrappedError
		}

		if resultError != nil {
			update := bson.M{}
			update["$set"] = bson.D{
				bson.E{Key: "task_error", Value: resultError.Error()},
			}
			myMongo.UpdateTask(bson.M{"_id": task.ID}, update)
		}
	}()

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		return err
	}

	var param0 storage.Commit1Out
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	c2Out, err := sb.SealCommit2(context.TODO(), task.SectorRef, param0)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	c2OutByte, err := json.Marshal(c2Out)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	resultError = multierror.Append(resultError, err)

	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(c2OutByte))
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	return nil
}

func fs(taskId string) (err error) {
	var resultError error
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			log.Infof("Recovered in fs: %v", r)
			wrappedError := fmt.Errorf("recover for error: %w", r)
			resultError = multierror.Append(resultError, wrappedError)
			err = wrappedError
		}

		if resultError != nil {
			update := bson.M{}
			update["$set"] = bson.D{
				bson.E{Key: "task_error", Value: resultError.Error()},
			}
			myMongo.UpdateTask(bson.M{"_id": task.ID}, update)
		}
	}()

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	var param0 []storage.Range
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	err = sb.FinalizeSector(context.TODO(), task.SectorRef, param0)
	if err != nil {
		err = myMongo.UpdateTaskResStatus(task.ID, "done", err.Error())
		if err != nil {
			resultError = multierror.Append(resultError, err)
			return err
		}
		return nil
	}

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	resultError = multierror.Append(resultError, err)

	err = myMongo.UpdateStatus(task.ID, "done")
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	return nil
}

type MyTmpLocalWorkerPathProvider struct{}

func (l *MyTmpLocalWorkerPathProvider) AcquireSector(ctx context.Context, sector storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, sealing storiface.PathType) (storiface.SectorPaths, func(), error) {
	filter := bson.M{
		"ip":   myUtils.GetLocalIPv4s(),
		"role": "worker",
	}
	machine, err := myMongo.FindMachine(filter)
	if err != nil || len(machine) == 0 {
		return storiface.SectorPaths{}, nil, err
	}
	machinePath := machine[0].WorkerLocalPath
	// TODO t0 f0
	folder := fmt.Sprintf("s-t0%v-%v", sector.ID.Miner, sector.ID.Number)
	return storiface.SectorPaths{
		ID:          sector.ID,
		Unsealed:    filepath.Join(machinePath, "unsealed", folder),
		Sealed:      filepath.Join(machinePath, "sealed", folder),
		Cache:       filepath.Join(machinePath, "cache", folder),
		Update:      "",
		UpdateCache: "",
	}, func() {}, nil
}
