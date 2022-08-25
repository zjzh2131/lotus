package main

import (
	"context"
	"fmt"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
	"strings"

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

		os.Setenv("cpus", cpus)
		os.Setenv("node_id", nodeId)
		os.Setenv("gpu_idx", gpuIdx)

		switch taskType {
		case "seal/v0/addpiece":
			err := sectorstorage.Ap(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/precommit/1":
			err := sectorstorage.P1(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/precommit/2":
			err := sectorstorage.P2(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/commit/1":
			err := sectorstorage.C1(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/commit/2":
			err := sectorstorage.C2(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		case "seal/v0/finalize":
			err := sectorstorage.Fs(id, cpus, nodeId)
			if err != nil {
				panic(err)
			}
		default:
		}
		return nil
	},
}
