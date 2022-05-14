package sealmarket

import (
	"context"
	"sort"

	"github.com/filecoin-project/lotus/snarky"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"
)

// TODO pass in host
func NewWorkerCalls(ctx context.Context, totallyDecentralizedURL string) (*WorkerCalls, error) {

	providers, err := DiscoverProviders(ctx, totallyDecentralizedURL)
	if err != nil {
		return nil, err
	}
	return &WorkerCalls{
		providers: providers,
	}, nil
}

type WorkerCalls struct {
	host host.Host

	providers []provider
}

type provider struct {
	peer peer.AddrInfo
	jobs map[snarky.JobID]job

	success, fail int
}

func (p *provider) successRatio() float64 {
	if p.success+p.fail == 0 {
		return 1
	}

	return float64(p.success) / float64(p.success+p.fail)
}

type job struct {
	sector storage.SectorRef
	call   storiface.CallID

	commit2Params *storage.Commit1Out
}

func (w *WorkerCalls) SealCommit2(ctx context.Context, sector storage.SectorRef, c1o storage.Commit1Out) (storiface.CallID, error) {
	sort.Slice(w.providers, func(i, j int) bool {
		return w.providers[i].successRatio() > w.providers[j].successRatio()
	})

	if len(w.providers) == 0 {
		return storiface.CallID{}, xerrors.Errorf("no providers")
	}

	provider := w.providers[0]

	w.host.NewStream()
}

// Unsupported

func (w *WorkerCalls) DataCid(ctx context.Context, pieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) AddPiece(ctx context.Context, sector storage.SectorRef, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) SealPreCommit2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) FinalizeReplicaUpdate(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) ReplicaUpdate(ctx context.Context, sector storage.SectorRef, pieces []abi.PieceInfo) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) ProveReplicaUpdate1(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) ProveReplicaUpdate2(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid, vanillaProofs storage.ReplicaVanillaProofs) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) GenerateSectorKeyFromData(ctx context.Context, sector storage.SectorRef, commD cid.Cid) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) MoveStorage(ctx context.Context, sector storage.SectorRef, types storiface.SectorFileType) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) UnsealPiece(ctx context.Context, ref storage.SectorRef, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, cid cid.Cid) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) Fetch(ctx context.Context, ref storage.SectorRef, fileType storiface.SectorFileType, pathType storiface.PathType, mode storiface.AcquireMode) (storiface.CallID, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) GenerateWinningPoSt(ctx context.Context, ppt abi.RegisteredPoStProof, mid abi.ActorID, sectors []storiface.PostSectorChallenge, randomness abi.PoStRandomness) ([]proof.PoStProof, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WorkerCalls) GenerateWindowPoSt(ctx context.Context, ppt abi.RegisteredPoStProof, mid abi.ActorID, sectors []storiface.PostSectorChallenge, partitionIdx int, randomness abi.PoStRandomness) (storiface.WindowPoStResult, error) {
	//TODO implement me
	panic("implement me")
}

var _ storiface.WorkerCalls = &WorkerCalls{}
