package sealmarket

import (
	"bytes"
	"context"
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"sort"

	"github.com/filecoin-project/lotus/snarky"
	"github.com/filecoin-project/specs-actors/actors/builtin/paych"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"
)

var log = logging.Logger("sealmarket")

const PollingInterval = time.Minute

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
	chain api.FullNode
	ret   storiface.WorkerReturn

	host       host.Host
	clientAddr addr.Address

	providers []provider
}

type provider struct {
	peer peer.AddrInfo

	jobs map[storiface.CallID]*job

	success, fail int
}

type job struct {
	sector storage.SectorRef
	job    snarky.JobID

	start time.Time

	returned bool

	c2proof *storage.Proof
	err     *storiface.CallError

	// for retry todo commit2Params *storage.Commit1Out
}

func (p *provider) successRatio() float64 {
	if p.success+p.fail == 0 {
		return 1
	}

	return float64(p.success) / float64(p.success+p.fail)
}

func (p *provider) queryPrice(ctx context.Context, h host.Host, spt abi.RegisteredSealProof) (*snarky.PriceResponse, error) {
	ctx, done := context.WithTimeout(ctx, 5*time.Second)
	defer done()

	// todo addrinfo to peerstore

	stream, err := h.NewStream(ctx, p.peer.ID, snarky.ProvServPriceProtocol)
	if err != nil {
		return nil, xerrors.Errorf("opening price ask stream: %w", err)
	}

	if err := stream.SetDeadline(time.Now().Add(time.Second * 5)); err != nil {
		return nil, xerrors.Errorf("setting stream deadline: %w", err)
	}

	defer func() {
		if err := stream.Close(); err != nil {
			log.Errorw("closing stream", "err", err)
		}
	}()

	req := &snarky.PriceRequest{
		ProofType: spt,
	}
	var rb bytes.Buffer
	if err := req.MarshalCBOR(&rb); err != nil {
		return nil, xerrors.Errorf("marshal price request: %w", err)
	}

	if _, err := stream.Write(rb.Bytes()); err != nil {
		return nil, err
	}

	if err := stream.CloseWrite(); err != nil {
		return nil, xerrors.Errorf("closing stream write: %w", err)
	}

	var resp [1024]byte

	n, err := stream.Read(resp[:])
	if err != nil {
		return nil, xerrors.Errorf("read resp: %w", err)
	}

	if n == 1024 {
		return nil, xerrors.Errorf("response too long")
	}
	var res snarky.PriceResponse
	if err := res.UnmarshalCBOR(bytes.NewReader(resp[:])); err != nil {
		return nil, xerrors.Errorf("unmarshal: %w", err)
	}

	if !res.Accept {
		return &res, xerrors.Errorf("not accepted: '%s'", res.Error) // todo safe string?
	}

	return &res, nil
}

func (p *provider) requestWork(ctx context.Context, h host.Host, sector storage.SectorRef, c1o storage.Commit1Out, payment *paych.SignedVoucher) (snarky.JobID, error) {
	ctx, done := context.WithTimeout(ctx, 120*time.Second)
	defer done()

	stream, err := h.NewStream(ctx, p.peer.ID, snarky.ProvServProtocol)
	if err != nil {
		return "", xerrors.Errorf("opening price ask stream: %w", err)
	}

	if err := stream.SetDeadline(time.Now().Add(120 * time.Second)); err != nil {
		return "", xerrors.Errorf("setting stream deadline: %w", err)
	}

	defer func() {
		if err := stream.Close(); err != nil {
			log.Errorw("closing stream", "err", err)
		}
	}()

	req := &snarky.WorkRequest{
		Payment: payment,
		ProveCommitRequest: &snarky.ProveCommitRequest{
			Sector: snarky.RefFormNative(sector),
			C1o:    c1o,
		},
	}
	var rb bytes.Buffer
	if err := req.MarshalCBOR(&rb); err != nil {
		return "", xerrors.Errorf("marshal price request: %w", err)
	}

	if _, err := stream.Write(rb.Bytes()); err != nil {
		return "", xerrors.Errorf("writing request: %w", err)
	}

	if err := stream.CloseWrite(); err != nil {
		return "", xerrors.Errorf("closing stream write: %w", err)
	}

	var resp [1024]byte

	n, err := stream.Read(resp[:])
	if err != nil {
		return "", xerrors.Errorf("read resp: %w", err)
	}

	if n == 1024 {
		return "", xerrors.Errorf("response too long")
	}
	var res snarky.WorkResponse
	if err := res.UnmarshalCBOR(bytes.NewReader(resp[:])); err != nil {
		return "", xerrors.Errorf("unmarshal: %w", err)
	}

	if res.Error != "" {
		return "", xerrors.Errorf("job error: %s", res.Error) // todo safe string
	}

	// todo check jobid structure maybe

	return res.JobID, nil
}

func (p *provider) requestStatus(ctx context.Context, h host.Host, job snarky.JobID) (*snarky.StatusResponse, error) {
	ctx, done := context.WithTimeout(ctx, 5*time.Second)
	defer done()

	stream, err := h.NewStream(ctx, p.peer.ID, snarky.ProvServStatusProtocol)
	if err != nil {
		return nil, xerrors.Errorf("opening price ask stream: %w", err)
	}

	if err := stream.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, xerrors.Errorf("setting stream deadline: %w", err)
	}

	defer func() {
		if err := stream.Close(); err != nil {
			log.Errorw("closing stream", "err", err)
		}
	}()

	req := &snarky.StatusRequest{
		JobID: job,
	}
	var rb bytes.Buffer
	if err := req.MarshalCBOR(&rb); err != nil {
		return nil, xerrors.Errorf("marshal price request: %w", err)
	}

	if _, err := stream.Write(rb.Bytes()); err != nil {
		return nil, xerrors.Errorf("writing request: %w", err)
	}

	if err := stream.CloseWrite(); err != nil {
		return nil, xerrors.Errorf("closing stream write: %w", err)
	}

	var resp [102400]byte

	n, err := stream.Read(resp[:])
	if err != nil {
		return nil, xerrors.Errorf("read resp: %w", err)
	}

	if n == 102400 {
		return nil, xerrors.Errorf("response too long")
	}
	var res snarky.StatusResponse
	if err := res.UnmarshalCBOR(bytes.NewReader(resp[:])); err != nil {
		return nil, xerrors.Errorf("unmarshal: %w", err)
	}

	return &res, nil
}

func (w *WorkerCalls) SealCommit2(ctx context.Context, sector storage.SectorRef, c1o storage.Commit1Out) (storiface.CallID, error) {
	sort.Slice(w.providers, func(i, j int) bool {
		return w.providers[i].successRatio() > w.providers[j].successRatio()
	})

	if len(w.providers) == 0 {
		return storiface.CallID{}, xerrors.Errorf("no providers")
	}

	log.Info("start commit2 provider selection", "sector", sector.ID, "proofType", sector.ProofType)

	var prov *provider
	var ask *snarky.PriceResponse
	for _, p := range w.providers {
		var err error
		ask, err = p.queryPrice(ctx, w.host, sector.ProofType)
		if err == nil {
			prov = &p
			break
		}

		log.Warnw("failed to query provider", "prov", prov.peer.ID, "err", err)
	}

	log.Infow("commit2 provider selected", "provider", prov.peer.ID, "price", types.FIL(ask.Price))

	pch, err := w.chain.PaychGet(ctx, w.clientAddr, ask.Addr, ask.Price, api.PaychGetOpts{OffChain: true})
	if err != nil {
		// todo disable provider + try other providers
		return storiface.CallID{}, xerrors.Errorf("getting payment channel: %w", err)
	}

	// todo reuse lanes!!
	lane, err := w.chain.PaychAllocateLane(ctx, pch.Channel)
	if err != nil {
		return storiface.CallID{}, xerrors.Errorf("getting paych lane: %w", err)
	}

	v, err := w.chain.PaychVoucherCreate(ctx, pch.Channel, ask.Price, lane)
	if err != nil {
		return storiface.CallID{}, xerrors.Errorf("creating payment voucher: %w", err)
	}

	jobId, err := prov.requestWork(ctx, w.host, sector, c1o, v.Voucher)
	if err != nil {
		return storiface.CallID{}, xerrors.Errorf("requesting work: %w", err)
	}

	callId := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}
	log.Infow("work started", "provider", prov.peer.ID, "job", jobId, "call", callId)

	jobEntry := job{
		sector: sector,
		job:    jobId,
		start:  time.Now(),
	}

	go func() {
		ctx := context.Background()

		var res storage.Proof
		var cerr *storiface.CallError

		defer func() {
			if err := w.ret.ReturnSealCommit2(ctx, callId, res, cerr); err != nil {
				// todo retry
				log.Warnw("returning job", "error", err)
			}

			log.Infow("job returned", "call", callId, "took", time.Now().Sub(jobEntry.start))
		}()

		for {
			time.Sleep(PollingInterval)

			st, err := prov.requestStatus(ctx, w.host, jobId)
			if err != nil {
				log.Warnw("querying job status error", "provider", prov.peer.ID, "call", callId, "error", err)
				continue
			}

			if st.Status == snarky.StatusInProgress {
				// todo timeout
				continue
			}

			switch st.Status {
			case snarky.StatusDone, snarky.StatusFailed:
				if st.Result != nil {
					res = st.Result.Proof
				}
				if st.Error != "" {
					cerr = &storiface.CallError{
						Code:    storiface.ErrUnknown,
						Message: st.Error, // todo safe string
					}
				}

				return
			}
		}
	}()

	return callId, nil
}

// Unsupported

func (w *WorkerCalls) DataCid(ctx context.Context, pieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) AddPiece(ctx context.Context, sector storage.SectorRef, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) SealPreCommit2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) FinalizeReplicaUpdate(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) ReplicaUpdate(ctx context.Context, sector storage.SectorRef, pieces []abi.PieceInfo) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) ProveReplicaUpdate1(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) ProveReplicaUpdate2(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid, vanillaProofs storage.ReplicaVanillaProofs) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) GenerateSectorKeyFromData(ctx context.Context, sector storage.SectorRef, commD cid.Cid) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) MoveStorage(ctx context.Context, sector storage.SectorRef, types storiface.SectorFileType) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) UnsealPiece(ctx context.Context, ref storage.SectorRef, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, cid cid.Cid) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) Fetch(ctx context.Context, ref storage.SectorRef, fileType storiface.SectorFileType, pathType storiface.PathType, mode storiface.AcquireMode) (storiface.CallID, error) {
	panic("implement me")
}

func (w *WorkerCalls) GenerateWinningPoSt(ctx context.Context, ppt abi.RegisteredPoStProof, mid abi.ActorID, sectors []storiface.PostSectorChallenge, randomness abi.PoStRandomness) ([]proof.PoStProof, error) {
	panic("implement me")
}

func (w *WorkerCalls) GenerateWindowPoSt(ctx context.Context, ppt abi.RegisteredPoStProof, mid abi.ActorID, sectors []storiface.PostSectorChallenge, partitionIdx int, randomness abi.PoStRandomness) (storiface.WindowPoStResult, error) {
	panic("implement me")
}

var _ storiface.WorkerCalls = &WorkerCalls{}
