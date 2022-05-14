package snarky

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-address"
	addr "github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/actors/builtin/paych"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	inet "github.com/libp2p/go-libp2p-core/network"
)

var log = logging.Logger("proversvc")

const ProvServProtocol = "provingserver"
const ProvServPriceProtocol = "provingserverprice"
const ProvServStatusProtocol = "provingserverstatus"

const (
	ProofClassProveCommit = iota
)

var (
	ErrUnknownProofType      = fmt.Errorf("unknown proof type")
	ErrNotAcceptingProofType = fmt.Errorf("not accepting proof type")
)

type ProvingService struct {
	host  host.Host
	cfg   *Config
	store datastore.Datastore
	papi  ProvingApi

	addr address.Address
	vapi VoucherApi

	semaPC chan struct{}
}

type Config struct {
	ProveCommitEnable  bool
	ProveCommitParJobs int
	//WindowPoStEnable  bool
	//WinningPoStEnable bool

	Address address.Address
}

type VoucherApi interface {
	PaychVoucherCheckValid(context.Context, address.Address, *paych.SignedVoucher) error
	PaychVoucherCheckSpendable(context.Context, address.Address, *paych.SignedVoucher, []byte, []byte) (bool, error)
	PaychVoucherAdd(context.Context, address.Address, *paych.SignedVoucher, []byte, types.BigInt) (types.BigInt, error)
	PaychVoucherList(context.Context, address.Address) ([]*paych.SignedVoucher, error)
}

type ProvingApi interface {
	ComputePoRep(ctx context.Context, sector storage.SectorRef, c1o storage.Commit1Out) (storage.Proof, error)
}

func NewProvingService(h host.Host, cfg *Config, papi ProvingApi, vapi VoucherApi, ds datastore.Datastore) (*ProvingService, error) {
	ps := &ProvingService{
		host:  h,
		cfg:   cfg,
		store: ds,
		papi:  papi,
		addr:  cfg.Address,

		semaPC: make(chan struct{}, cfg.ProveCommitParJobs),
	}

	h.SetStreamHandler(ProvServPriceProtocol, ps.handlePriceRequest)
	h.SetStreamHandler(ProvServProtocol, ps.handleWorkRequest)
	h.SetStreamHandler(ProvServStatusProtocol, ps.handleStatusRequest)

	return ps, nil
}

type PriceRequest struct {
	ProofClass int64
	ProofType  int64
}

type PriceResponse struct {
	// Accept denotes whether or not the prover is even considering this type of request
	Accept bool

	Price abi.TokenAmount
	Addr  addr.Address

	Address address.Address

	Error string
}

func (ps *ProvingService) handlePriceRequest(s inet.Stream) {
	var req PriceRequest
	if err := cborutil.ReadCborRPC(s, &req); err != nil {
		log.Errorf("read price request rpc failed: %s", err)
		return
	}

	resp := ps.HandlePriceRequest(&req)

	if err := cborutil.WriteCborRPC(s, resp); err != nil {
		log.Errorf("write price request rpc failed: %s", err)
		return
	}
}

func (ps *ProvingService) HandlePriceRequest(req *PriceRequest) *PriceResponse {
	price, accept, err := ps.priceForProof(req.ProofClass, req.ProofType)
	if err != nil {
		return &PriceResponse{
			Error: err.Error(),
		}
	}

	return &PriceResponse{
		Price:   price,
		Accept:  accept,
		Address: ps.addr,
	}
}

func (ps *ProvingService) priceForProof(class int64, pt int64) (abi.TokenAmount, bool, error) {
	switch class {
	case ProofClassProveCommit:
		return abi.NewTokenAmount(10000000), true, nil
	default:
		return abi.NewTokenAmount(0), false, ErrUnknownProofType
	}
}

type SectorRef struct {
	ID        abi.SectorID
	ProofType abi.RegisteredSealProof
}

func RefFormNative(ref storage.SectorRef) SectorRef {
	return SectorRef{
		ID:        ref.ID,
		ProofType: ref.ProofType,
	}
}

func (sr SectorRef) ToNative() storage.SectorRef {
	return storage.SectorRef{
		ID:        sr.ID,
		ProofType: sr.ProofType,
	}
}

type ProveCommitRequest struct {
	Sector SectorRef
	C1o    storage.Commit1Out `maxlen:"33554432"` // 32M

	// params for prove commit
}

type PaymentData struct {
	From    address.Address
	Voucher *paych.SignedVoucher
}

type WorkRequest struct {
	Payment            *PaymentData
	ProveCommitRequest *ProveCommitRequest
	ProofClass         int64
	ProofType          int64
}

type WorkResponse struct {
	JobID string
	Error string
}

func (ps *ProvingService) handleWorkRequest(s inet.Stream) {
	var req WorkRequest
	if err := cborutil.ReadCborRPC(s, &req); err != nil {
		log.Errorf("failed to read work request rpc: %s", err)
		return
	}

	resp := ps.HandleWorkRequest(&req)

	if err := cborutil.WriteCborRPC(s, resp); err != nil {
		log.Errorf("failed to write work request rpc: %s", err)
		return
	}
}

func (ps *ProvingService) HandleWorkRequest(req *WorkRequest) *WorkResponse {
	if err := ps.ValidateRequest(req); err != nil {
		return &WorkResponse{
			Error: err.Error(),
		}
	}

	jobid, err := ps.StartWork(req)
	if err != nil {
		return &WorkResponse{
			Error: err.Error(),
		}
	}

	return &WorkResponse{
		JobID: jobid,
	}
}

type JobStatus struct {
	Status  string
	Request *WorkRequest
	Result  *ProofResult
}

func (ps *ProvingService) SetJobStatus(id string, st *JobStatus) error {
	ctx := context.TODO()

	b, err := json.Marshal(st)
	if err != nil {
		return err
	}

	if err := ps.store.Put(ctx, datastore.NewKey(id), b); err != nil {
		return err
	}

	return nil
}

func (ps *ProvingService) GetJobStatus(id string) (*JobStatus, error) {
	ctx := context.TODO()

	data, err := ps.store.Get(ctx, datastore.NewKey(id))
	if err != nil {
		return nil, err
	}

	var st JobStatus
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}

	return &st, nil
}

func (ps *ProvingService) StartWork(req *WorkRequest) (string, error) {
	// TODO: check queue length and error if too long

	jobid := uuid.New().String()

	if err := ps.SetJobStatus(jobid, &JobStatus{
		Status:  "starting",
		Request: req,
	}); err != nil {
		return "", err
	}

	go func(req *WorkRequest) {
		ctx := context.Background()

		switch req.ProofType {
		case ProofClassProveCommit:
			ps.semaPC <- struct{}{}
			defer func() {
				<-ps.semaPC
			}()

			if err := ps.SetJobStatus(jobid, &JobStatus{
				Status:  "running",
				Request: req,
			}); err != nil {
				log.Errorf("failed to update job status after lock acquisition: %s", err)
			}

			resp, err := ps.papi.ComputePoRep(ctx, req.ProveCommitRequest.Sector.ToNative(), req.ProveCommitRequest.C1o)
			if err != nil {
				log.Errorf("compute porep call failed: %s", err)
				if suberr := ps.SetJobStatus(jobid, &JobStatus{
					Status:  "failed",
					Request: req,
					Result: &ProofResult{
						Error: err.Error(),
					},
				}); suberr != nil {
					log.Errorf("failed to update job status after proof failure: %s", suberr)
				}
			}

			if suberr := ps.SetJobStatus(jobid, &JobStatus{
				Status:  "complete",
				Request: req,
				Result: &ProofResult{
					Proof: resp,
				},
			}); suberr != nil {
				log.Errorf("failed to update job status after proof completion: %s", suberr)
			}

		}
	}(req)

	return jobid, nil
}

func (ps *ProvingService) ValidateRequest(req *WorkRequest) error {
	switch req.ProofType {
	case ProofClassProveCommit:
		if req.ProveCommitRequest == nil {
			return fmt.Errorf("no request params for selected proof type")
		}
	default:
		return fmt.Errorf("unknown proof type")
	}

	exp, accept, err := ps.priceForProof(req.ProofClass, req.ProofType)
	if err != nil {
		return err
	}

	if !accept {
		return ErrNotAcceptingProofType
	}

	if err := ps.validatePayment(req.Payment, exp); err != nil {
		return err
	}

	return nil
}

func (ps *ProvingService) validatePayment(p *PaymentData, exp abi.TokenAmount) error {
	ctx := context.TODO()

	_, err := ps.vapi.PaychVoucherAdd(ctx, p.Voucher.ChannelAddr, p.Voucher, nil, exp)
	if err != nil {
		return err
	}

	return nil
}

type JobID = string

type StatusRequest struct {
	JobID JobID
}

type Status uint64

const (
	StatusInProgress Status = iota
	StatusDone
	StatusFailed
)

type StatusResponse struct {
	Status Status
	Error  string
	Result *ProofResult

	// todo return voucher on fail maybe
}

type ProofResult struct {
	Proof storage.Proof
	Error string
}

func (ps *ProvingService) handleStatusRequest(s inet.Stream) {
	var req StatusRequest
	if err := cborutil.ReadCborRPC(s, &req); err != nil {
		log.Errorf("failed to read status rpc: %s", err)
		return
	}

	resp := ps.HandleStatusRequest(&req)

	if err := cborutil.WriteCborRPC(s, resp); err != nil {
		log.Errorf("failed to write status rpc: %s", err)
		return
	}
}

func (ps *ProvingService) HandleStatusRequest(req *StatusRequest) *StatusResponse {
	st, err := ps.GetJobStatus(req.JobID)
	if err != nil {
		return &StatusResponse{
			Status: StatusFailed,
			Error:  err.Error(),
		}
	}

	if st.Result == nil {
		return &StatusResponse{
			Status: StatusInProgress,
			Result: st.Result,
		}
	}

	return &StatusResponse{
		Status: StatusDone,
		Result: st.Result,
	}
}
