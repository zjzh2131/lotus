package snarky

import (
	"github.com/filecoin-project/go-state-types/abi"
	sto "github.com/filecoin-project/specs-storage/storage"
	"github.com/libp2p/go-libp2p-core/host"
	inet "github.com/libp2p/go-libp2p-core/network"
)

const ProvServProtocol = "provingserver"
const ProvServPriceProtocol = "provingserverprice"
const ProvServStatusProtocol = "provingserverprice"

const (
	ProofTypeProveCommit = iota
)

type ProvingService struct {
	host host.Host
	cfg  *Config
}

type Config struct {
	ProveCommitEnable bool
	//WindowPoStEnable  bool
	//WinningPoStEnable bool
}

func NewProvingService(h host.Host, cfg *Config) (*ProvingService, error) {
	ps := &ProvingService{
		host: h,
		cfg:  cfg,
	}

	h.SetStreamHandler(ProvServPriceProtocol, ps.HandlePriceRequest)
	h.SetStreamHandler(ProvServProtocol, ps.HandleWorkRequest)
	h.SetStreamHandler(ProvServStatusProtocol, ps.HandleStatusRequest)

	return ps, nil
}

type PriceRequest struct {
	ProofType int
}

type PriceResponse struct {
	// Accept denotes whether or not the prover is even considering this type of request
	Accept bool

	Price abi.TokenAmount

	Error string
}

func (ps *ProvingService) HandlePriceRequest(s inet.Stream) {

}

func (ps *ProvingService) handlePriceRequest(req *PriceRequest) (*PriceResponse, error) {
	resp := &PriceResponse{}

	switch req.ProofType {
	case ProofTypeProveCommit:
		price := abi.NewTokenAmount(1000000)

		if ps.cfg.ProveCommitEnable {
			resp.Price = price
			resp.Accept = true
		}
	default:
		resp.Error = "unknown proof type"
	}

	return resp, nil
}

type ProveCommitRequest struct {
	Sector sto.SectorRef
	C1o    sto.Commit1Out

	// params for prove commit
}

type WorkRequest struct {
	Payment            []byte // todo: payment channel voucher
	ProveCommitRequest *ProveCommitRequest
}

type WorkResponse struct {
	JobID string
	Error string
}

func (ps *ProvingService) HandleWorkRequest(s inet.Stream) {

}

type StatusRequest struct {
	JobID string
}

type StatusResponse struct {
	Status string
	Error  string
	Result *ProofResult
}

type ProofResult struct {
	Proof sto.Proof
}

func (ps *ProvingService) HandleStatusRequest(s inet.Stream) {

}
