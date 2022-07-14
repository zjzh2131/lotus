package sectorstorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	migration "github.com/filecoin-project/lotus/my/migrate"
	"github.com/filecoin-project/lotus/my/myNuma"
	"github.com/filecoin-project/lotus/my/myReexec"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
	"io"
	"os/exec"
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
	"os"
	"strconv"
)

var tasksCaller = map[string]func(string, string, string) error{
	"seal/v0/addpiece":    ap,
	"seal/v0/precommit/1": p1,
	"seal/v0/precommit/2": p2,
	"seal/v0/commit/1":    c1,
	"seal/v0/commit/2":    c2,
	"seal/v0/finalize":    fs,
}

type applicationResource struct {
	cpuCount int
}

var tasksNeedNumaResource = map[string]applicationResource{
	"seal/v0/addpiece": {
		cpuCount: 1,
	},
	"seal/v0/precommit/1": {
		cpuCount: 2,
	},
	"seal/v0/precommit/2": {
		cpuCount: 1,
	},
	"seal/v0/commit/1": {
		cpuCount: 1,
	},
	"seal/v0/commit/2": {
		cpuCount: 1,
	},
	"seal/v0/finalize": {
		cpuCount: 1,
	},
}

func init() {
	myReexec.Register("seal/v0/addpiece", register)
	myReexec.Register("seal/v0/precommit/1", register)
	myReexec.Register("seal/v0/precommit/2", register)
	myReexec.Register("seal/v0/commit/1", register)
	myReexec.Register("seal/v0/commit/2", register)
	myReexec.Register("seal/v0/finalize", register)
	if myReexec.Init() {
		os.Exit(0)
	}
}

func register() error {
	log.Infof("child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)

	// set numa
	if os.Args[3] != "" {
		err := boundCpu(os.Args[3], strconv.Itoa(os.Getpid()))
		if err != nil {
			return errors.New("bound cpu error")
		}
		fmt.Printf("bound cpus: [%v]\n", os.Args[3])
	}

	nodeId, _ := strconv.Atoi(os.Args[4])
	if nodeId == -1 {
		myNuma.SetPreferred(nodeId)
		fmt.Printf("bound node memory: [%v]\n", nodeId)
	}

	// call
	if call, ok := tasksCaller[os.Args[0]]; ok {
		err := call(os.Args[1], os.Args[3], os.Args[4])
		if err != nil {
			return err
		}
	}
	return nil
}

func boundCpu(cpus, pid string) error {
	//taskset -pc 0,2 11498
	cmd := exec.Command("taskset", "-pc", cpus, pid)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

//func init() {
//	myReexec.Register("seal/v0/addpiece", func() error {
//		log.Infof("ap child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
//		var err error
//		taskId := os.Args[1]
//		err = ap(taskId)
//		if err != nil {
//			return err
//		}
//		return nil
//	})
//	myReexec.Register("seal/v0/precommit/1", func() error {
//		log.Infof("p1 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
//		var err error
//		taskId := os.Args[1]
//		err = p1(taskId)
//		if err != nil {
//			return err
//		}
//		return nil
//	})
//	myReexec.Register("seal/v0/precommit/2", func() error {
//		log.Infof("p2 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
//		var err error
//		taskId := os.Args[1]
//		err = p2(taskId)
//		if err != nil {
//			return err
//		}
//		return nil
//	})
//	myReexec.Register("seal/v0/commit/1", func() error {
//		log.Infof("c1 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
//		var err error
//		taskId := os.Args[1]
//		err = c1(taskId)
//		if err != nil {
//			return err
//		}
//		return nil
//	})
//	myReexec.Register("seal/v0/commit/2", func() error {
//		log.Infof("c2 child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
//		var err error
//		taskId := os.Args[1]
//		err = c2(taskId)
//		if err != nil {
//			return err
//		}
//		return nil
//	})
//	myReexec.Register("seal/v0/finalize", func() error {
//		log.Infof("fz child process pid: %v, ppid: %v, args: %v\n", os.Getpid(), os.Getppid(), os.Args)
//		var err error
//		taskId := os.Args[1]
//		err = fs(taskId)
//		if err != nil {
//			return err
//		}
//		return nil
//	})
//	if myReexec.Init() {
//		os.Exit(0)
//	}
//}

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

func ap(taskId, cpus, node string) (err error) {
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node)
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	var r io.Reader
	if task.TaskPath != "" {
		workerMachine, err := myMongo.FindOneMachine(bson.M{
			"ip":   myUtils.GetLocalIPv4s(),
			"role": "worker",
		})
		tmpStorageMachine, err := myMongo.FindOneMachine(bson.M{
			"role": "tmp_storage",
		})
		folder := fmt.Sprintf("s-t0%v-%v", task.SectorRef.ID.Miner, task.SectorRef.ID.Number)
		filePath := filepath.Join(workerMachine.WorkerMountPath, tmpStorageMachine.Ip, "AddPiece", folder, task.TaskPath)
		r, err = os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open file failed,err: %w", err)
		}
	} else {
		r = nullreader.NewNullReader(abi.UnpaddedPieceSize(size))
	}

	piece, err := sb.AddPiece(context.TODO(), task.SectorRef, param0, abi.UnpaddedPieceSize(size), r)
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

func p1(taskId, cpus, node string) (err error) {
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node)
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

func p2(taskId, cpus, node string) (err error) {
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node)
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

func c1(taskId, cpus, node string) (err error) {
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node)
	if err != nil {
		resultError = multierror.Append(resultError, err)
	}

	c1Out, err := sb.SealCommit1(context.TODO(), task.SectorRef, param0, param1, param2, param3)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	// TODO
	c1OutByte, err := json.Marshal(c1Out)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}
	// step 1 write c1out
	workerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   myUtils.GetLocalIPv4s(),
		"role": "worker",
	})
	folder := fmt.Sprintf("s-t0%v-%v", task.SectorRef.ID.Miner, task.SectorRef.ID.Number)
	filePath := filepath.Join(workerMachine.WorkerLocalPath, "c1Out", folder, "c1Out")
	err = migration.WriteDataToFile(filePath, c1OutByte)
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	// step 2 ftp migrate
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

func c2(taskId, cpus, node string) (err error) {
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
	//err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	//resultError = multierror.Append(resultError, err)

	workerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   myUtils.GetLocalIPv4s(),
		"role": "worker",
	})
	tmpStorageMachine, err := myMongo.FindOneMachine(bson.M{
		"role": "tmp_storage",
	})
	folder := fmt.Sprintf("s-t0%v-%v", task.SectorRef.ID.Miner, task.SectorRef.ID.Number)
	filePath := filepath.Join(workerMachine.WorkerMountPath, tmpStorageMachine.Ip, "c1Out", folder, "c1Out")
	c1OutByte, err := migration.ReadDataFromFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(c1OutByte, &param0)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node)
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

	//// step 1 write c1out
	//workerMachine, err := myMongo.FindOneMachine(bson.M{
	//	"ip":   myUtils.GetLocalIPv4s(),
	//	"role": "worker",
	//})
	//folder := fmt.Sprintf("s-t0%v-%v", task.SectorRef.ID.Miner, task.SectorRef.ID.Number)
	//filePath := filepath.Join(workerMachine.WorkerLocalPath, "c2Out", folder, "c2Out")
	//err = migration.WriteDataToFile(filePath, c2OutByte)
	//if err != nil {
	//	resultError = multierror.Append(resultError, err)
	//	return err
	//}
	//
	//// step 2 ftp migrate
	//err = migrateC2out(task.SectorRef)
	//if err != nil {
	//	return err
	//}

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

func fs(taskId, cpus, node string) (err error) {
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node)
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

	log.Infof("migrate start: SectorId(%v)\n", task.SectorRef.ID.Number)
	err = migrate(task.SectorRef)
	if err != nil {
		fmt.Println("migrate err:", err)
		myMongo.UpdateStatus(task.ID, "failed")
		return
	}
	log.Infof("migrate end: SectorId(%v)\n", task.SectorRef.ID.Number)

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
