package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/storage-sealing/lib/nullreader"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	migration "github.com/filecoin-project/lotus/my/migrate"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/bson"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

var tasksCmd = &cli.Command{
	Name:  "tasks",
	Usage: "Manage task processing",
	Subcommands: []*cli.Command{
		tasksEnableCmd,
		tasksDisableCmd,
		myApTaskCmd,
		myP1TaskCmd,
		myP2TaskCmd,
		myC1TaskCmd,
		myC2TaskCmd,
		myFinalizeSectorTaskCmd,
		myTask,
	},
}

var allowSetting = map[sealtasks.TaskType]struct{}{
	sealtasks.TTAddPiece:            {},
	sealtasks.TTDataCid:             {},
	sealtasks.TTPreCommit1:          {},
	sealtasks.TTPreCommit2:          {},
	sealtasks.TTCommit2:             {},
	sealtasks.TTUnseal:              {},
	sealtasks.TTReplicaUpdate:       {},
	sealtasks.TTProveReplicaUpdate2: {},
	sealtasks.TTRegenSectorKey:      {},
}

var settableStr = func() string {
	var s []string
	for _, tt := range ttList(allowSetting) {
		s = append(s, tt.Short())
	}
	return strings.Join(s, "|")
}()

var tasksEnableCmd = &cli.Command{
	Name:      "enable",
	Usage:     "Enable a task type",
	ArgsUsage: "[" + settableStr + "]",
	Action:    taskAction(api.Worker.TaskEnable),
}

var tasksDisableCmd = &cli.Command{
	Name:      "disable",
	Usage:     "Disable a task type",
	ArgsUsage: "[" + settableStr + "]",
	Action:    taskAction(api.Worker.TaskDisable),
}

func taskAction(tf func(a api.Worker, ctx context.Context, tt sealtasks.TaskType) error) func(cctx *cli.Context) error {
	return func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return xerrors.Errorf("expected 1 argument")
		}

		var tt sealtasks.TaskType
		for taskType := range allowSetting {
			if taskType.Short() == cctx.Args().First() {
				tt = taskType
				break
			}
		}

		if tt == "" {
			return xerrors.Errorf("unknown task type '%s'", cctx.Args().First())
		}

		api, closer, err := lcli.GetWorkerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return tf(api, ctx, tt)
	}
}

var myTask = &cli.Command{
	Name:      "myTask",
	Usage:     "Disable a task type",
	ArgsUsage: "<taskId>",
	Action: func(cctx *cli.Context) error {
		fmt.Println(cctx.Args())

		taskType := cctx.Args().Get(0)
		id := cctx.Args().Get(1)
		cpus := cctx.Args().Get(3)
		nodeId := cctx.Args().Get(4)
		gpuIdx := cctx.Args().Get(5)
		columnBatch := cctx.Args().Get(6)
		treeBatch := cctx.Args().Get(7)

		os.Setenv("cpus", cpus)
		os.Setenv("node_id", nodeId)
		os.Setenv("gpu_idx", gpuIdx)
		os.Setenv("column_batch", columnBatch)
		os.Setenv("tree_batch", treeBatch)

		switch taskType {
		case "seal/v0/addpiece":
			err := ap(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/precommit/1":
			err := p1(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/precommit/2":
			err := p2(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/commit/1":
			err := c1(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/commit/2":
			err := c2(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/finalize":
			err := fs(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		default:
		}
		return nil
	},
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

	sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node, os.Getpid(), os.Getppid())
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

	sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node, os.Getpid(), os.Getppid())
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

	sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	var param0 storage.PreCommit1Out
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node, os.Getpid(), os.Getppid())
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

	sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node, os.Getpid(), os.Getppid())
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
	err = sectorstorage.MigrateC1out(task.SectorRef)
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

	sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node, os.Getpid(), os.Getppid())
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

	sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
	if err != nil {
		resultError = multierror.Append(resultError, err)
		return err
	}

	var param0 []storage.Range
	err = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	resultError = multierror.Append(resultError, err)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s(), cpus, node, os.Getpid(), os.Getppid())
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
	err = sectorstorage.Migrate(task.SectorRef)
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

var myApTaskCmd = &cli.Command{
	Name:      "myApTask",
	Usage:     "Disable a task type",
	ArgsUsage: "<taskId>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must pass task id")
		}

		taskId := cctx.Args().Get(0)
		task, err := myMongo.FindByObjId(taskId)
		if err != nil {
			return xerrors.Errorf("find task id error")
		}

		sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
		if err != nil {
			return err
		}

		var param0 myModel.APParam0
		_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
		size, _ := strconv.Atoi(task.TaskParameters[1])

		myMongo.UpdateStatus(task.ID, "running")

		piece, err := sb.AddPiece(context.TODO(), task.SectorRef, param0, abi.UnpaddedPieceSize(size), nullreader.NewNullReader(abi.UnpaddedPieceSize(size)))
		if err != nil {
			log.Errorf("sb.AddPiece error: %v", err)
			return xerrors.Errorf("sb.AddPiece error")
		}

		strPiece, _ := json.Marshal(piece)
		myMongo.UpdateStatus(task.ID, "done")
		myMongo.UpdateResult(task.ID, string(strPiece))
		fmt.Println("========================================Piece:", string(strPiece))
		return nil
	},
}

var myP1TaskCmd = &cli.Command{
	Name:      "myP1Task",
	Usage:     "Disable a task type",
	ArgsUsage: "<taskId>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must pass task id")
		}

		taskId := cctx.Args().Get(0)
		task, err := myMongo.FindByObjId(taskId)
		if err != nil {
			return xerrors.Errorf("find task id error")
		}

		sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
		if err != nil {
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
	},
}

var myP2TaskCmd = &cli.Command{
	Name:      "myP2Task",
	Usage:     "Disable a task type",
	ArgsUsage: "<taskId>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must pass task id")
		}

		taskId := cctx.Args().Get(0)
		task, err := myMongo.FindByObjId(taskId)
		if err != nil {
			return xerrors.Errorf("find task id error")
		}

		sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
		if err != nil {
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
	},
}

var myC1TaskCmd = &cli.Command{
	Name:      "myC1Task",
	Usage:     "Disable a task type",
	ArgsUsage: "<taskId>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must pass task id")
		}

		taskId := cctx.Args().Get(0)
		task, err := myMongo.FindByObjId(taskId)
		if err != nil {
			return xerrors.Errorf("find task id error")
		}

		sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
		if err != nil {
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
	},
}

var myC2TaskCmd = &cli.Command{
	Name:      "myC2Task",
	Usage:     "Disable a task type",
	ArgsUsage: "<taskId>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must pass task id")
		}

		taskId := cctx.Args().Get(0)
		task, err := myMongo.FindByObjId(taskId)
		if err != nil {
			return xerrors.Errorf("find task id error")
		}

		sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
		if err != nil {
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
	},
}

var myFinalizeSectorTaskCmd = &cli.Command{
	Name:      "myFinalizeSectorTask",
	Usage:     "Disable a task type",
	ArgsUsage: "<taskId>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must pass task id")
		}

		taskId := cctx.Args().Get(0)
		task, err := myMongo.FindByObjId(taskId)
		if err != nil {
			return xerrors.Errorf("find task id error")
		}

		sb, err := ffiwrapper.New(&sectorstorage.MyTmpLocalWorkerPathProvider{})
		if err != nil {
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
	},
}
