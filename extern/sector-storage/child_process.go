package sectorstorage

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"go.mongodb.org/mongo-driver/bson"
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
		var err error
		taskId := os.Args[1]
		err = ap(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/precommit/1", func() error {
		var err error
		taskId := os.Args[1]
		err = p1(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/precommit/2", func() error {
		var err error
		taskId := os.Args[1]
		err = p2(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/commit/1", func() error {
		var err error
		taskId := os.Args[1]
		err = c1(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/commit/2", func() error {
		var err error
		taskId := os.Args[1]
		err = c2(taskId)
		if err != nil {
			return err
		}
		return nil
	})
	myReexec.Register("seal/v0/finalize", func() error {
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

func ap(taskId string) error {
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		return err
	}

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		return err
	}

	var param0 myModel.APParam0
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	size, _ := strconv.Atoi(task.TaskParameters[1])

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		return err
	}
	//err = myMongo.UpdateStatus(task.ID, "running")
	//if err != nil {
	//	return err
	//}

	piece, err := sb.AddPiece(context.TODO(), task.SectorRef, param0, abi.UnpaddedPieceSize(size), nullreader.NewNullReader(abi.UnpaddedPieceSize(size)))
	if err != nil {
		return err
	}

	strPiece, _ := json.Marshal(piece)
	//myMongo.UpdateStatus(task.ID, "done")
	//myMongo.UpdateResult(task.ID, string(strPiece))

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(strPiece))
	if err != nil {
		return err
	}
	return nil
}

func p1(taskId string) error {
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		return err
	}

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		return err
	}

	var param0 abi.SealRandomness
	var param1 []abi.PieceInfo
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)
	_ = json.Unmarshal([]byte(task.TaskParameters[1]), &param1)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		return err
	}

	//err = myMongo.UpdateStatus(task.ID, "running")
	//if err != nil {
	//	return err
	//}

	p1Out, err := sb.SealPreCommit1(context.TODO(), task.SectorRef, param0, param1)
	if err != nil {
		return err
	}
	p1OutByte, _ := json.Marshal(p1Out)
	//myMongo.UpdateStatus(c.task.ID, "done")
	//myMongo.UpdateResult(c.task.ID, string(p1OutByte))

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(p1OutByte))
	if err != nil {
		return err
	}
	return nil
}

func p2(taskId string) error {
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		return err
	}

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		return err
	}

	var param0 storage.PreCommit1Out
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		return err
	}

	//err = myMongo.UpdateStatus(task.ID, "running")
	//if err != nil {
	//	return err
	//}

	p2Out, err := sb.SealPreCommit2(context.TODO(), task.SectorRef, param0)
	if err != nil {
		return err
	}
	p2OutByte, _ := json.Marshal(p2Out)
	//myMongo.UpdateStatus(c.task.ID, "done")
	//myMongo.UpdateResult(c.task.ID, string(p2OutByte))
	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(p2OutByte))
	if err != nil {
		return err
	}
	return nil
}

func c1(taskId string) error {
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		return err
	}

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
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

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		return err
	}

	//err = myMongo.UpdateStatus(task.ID, "running")
	//if err != nil {
	//	return err
	//}

	c1Out, err := sb.SealCommit1(context.TODO(), task.SectorRef, param0, param1, param2, param3)
	if err != nil {
		return err
	}
	c1OutByte, _ := json.Marshal(c1Out)
	//myMongo.UpdateStatus(c.task.ID, "done")
	//myMongo.UpdateResult(c.task.ID, string(c1OutByte))

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(c1OutByte))
	if err != nil {
		return err
	}
	return nil
}

func c2(taskId string) error {
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		return err
	}

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		return err
	}

	var param0 storage.Commit1Out
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		return err
	}

	//err = myMongo.UpdateStatus(task.ID, "running")
	//if err != nil {
	//	return err
	//}

	c1Out, err := sb.SealCommit2(context.TODO(), task.SectorRef, param0)
	if err != nil {
		return err
	}
	c2OutByte, _ := json.Marshal(c1Out)
	//myMongo.UpdateStatus(c.task.ID, "done")
	//myMongo.UpdateResult(c.task.ID, string(c1OutByte))

	err = myMongo.UpdateTaskLog(logId, bson.M{
		"$set": bson.D{
			bson.E{Key: "end_time", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_at", Value: time.Now().UnixMilli()},
			bson.E{Key: "updated_by", Value: myUtils.GetLocalIPv4s()},
		},
	})
	err = myMongo.UpdateTaskResStatus(task.ID, "done", string(c2OutByte))
	if err != nil {
		return err
	}
	return nil
}

func fs(taskId string) error {
	task, err := myMongo.FindByObjId(taskId)
	if err != nil {
		return err
	}

	sb, err := ffiwrapper.New(&MyTmpLocalWorkerPathProvider{})
	if err != nil {
		return err
	}

	var param0 []storage.Range
	_ = json.Unmarshal([]byte(task.TaskParameters[0]), &param0)

	logId, err := myMongo.InsertTaskLog(task, myUtils.GetLocalIPv4s())
	if err != nil {
		return err
	}

	//err = myMongo.UpdateStatus(task.ID, "running")
	//if err != nil {
	//	return err
	//}

	err = sb.FinalizeSector(context.TODO(), task.SectorRef, param0)
	if err != nil {
		//myMongo.UpdateError(c.task.ID, err.Error())
		//myMongo.UpdateStatus(c.task.ID, "done")
		err = myMongo.UpdateTaskResStatus(task.ID, "done", err.Error())
		if err != nil {
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
	err = myMongo.UpdateStatus(task.ID, "done")
	if err != nil {
		return err
	}
	return nil
}

type MyTmpLocalWorkerPathProvider struct{}

func (l *MyTmpLocalWorkerPathProvider) AcquireSector(ctx context.Context, sector storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, sealing storiface.PathType) (storiface.SectorPaths, func(), error) {
	// TODO
	machinePath := "/home/lotus/.lotusminer/"
	folder := fmt.Sprintf("s-t0%v-%v", sector.ID.Miner, sector.ID.Number)
	return storiface.SectorPaths{
		ID:          sector.ID,
		Unsealed:    machinePath + "unsealed/" + folder,
		Sealed:      machinePath + "sealed/" + folder,
		Cache:       machinePath + "cache/" + folder,
		Update:      "",
		UpdateCache: "",
	}, func() {}, nil
}
