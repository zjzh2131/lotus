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
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/specs-storage/storage"
	"strconv"
	"strings"

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
