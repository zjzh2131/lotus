package sealmarket

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	apitypes "github.com/filecoin-project/lotus/api/types"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/lib/rpcenc"
	"github.com/filecoin-project/lotus/metrics/proxy"
)

func WorkerHandler(authv func(ctx context.Context, token string) ([]auth.Permission, error), a api.Worker, permissioned bool) http.Handler {
	mux := mux.NewRouter()
	readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
	rpcServer := jsonrpc.NewServer(readerServerOpt)

	wapi := proxy.MetricedWorkerAPI(a)
	if permissioned {
		wapi = api.PermissionedWorkerAPI(wapi)
	}

	rpcServer.Register("Filecoin", wapi)

	mux.Handle("/rpc/v0", rpcServer)
	mux.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
	mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

	if !permissioned {
		return mux
	}

	ah := &auth.Handler{
		Verify: authv,
		Next:   mux.ServeHTTP,
	}
	return ah
}

type Worker struct {
	*WorkerCalls
	Sess uuid.UUID

	disabled int64
}

func (w *Worker) TaskTypes(ctx context.Context) (map[sealtasks.TaskType]struct{}, error) {
	return map[sealtasks.TaskType]struct{}{
		sealtasks.TTCommit2: {},
	}, nil
}

func (w *Worker) Paths(ctx context.Context) ([]storiface.StoragePath, error) {
	return []storiface.StoragePath{}, nil
}

func (w *Worker) Info(ctx context.Context) (storiface.WorkerInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (w *Worker) TaskDisable(ctx context.Context, tt sealtasks.TaskType) error {
	return xerrors.Errorf("not supported")
}

func (w *Worker) TaskEnable(ctx context.Context, tt sealtasks.TaskType) error {
	return xerrors.Errorf("not supported")
}

func (w *Worker) Remove(ctx context.Context, sector abi.SectorID) error {
	return xerrors.Errorf("not supported")
}

func (w *Worker) Version(context.Context) (api.Version, error) {
	return api.WorkerAPIVersion0, nil
}

func (w *Worker) StorageAddLocal(ctx context.Context, path string) error {
	return xerrors.Errorf("not supported")
}

func (w *Worker) SetEnabled(ctx context.Context, enabled bool) error {
	disabled := int64(1)
	if enabled {
		disabled = 0
	}
	atomic.StoreInt64(&w.disabled, disabled)
	return nil
}

func (w *Worker) Enabled(ctx context.Context) (bool, error) {
	return atomic.LoadInt64(&w.disabled) == 0, nil
}

func (w *Worker) WaitQuiet(ctx context.Context) error {
	return xerrors.Errorf("not supported")
}

func (w *Worker) ProcessSession(ctx context.Context) (uuid.UUID, error) {
	return w.Sess, nil
}

func (w *Worker) Session(ctx context.Context) (uuid.UUID, error) {
	if atomic.LoadInt64(&w.disabled) == 1 {
		return uuid.UUID{}, xerrors.Errorf("worker disabled")
	}

	return w.Sess, nil
}

func (w *Worker) Discover(ctx context.Context) (apitypes.OpenRPCDocument, error) {
	return build.OpenRPCDiscoverJSON_Worker(), nil
}

var _ storiface.WorkerCalls = &Worker{}
