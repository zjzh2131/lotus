package main

import (
	"context"
	"fmt"
	"io"

	big2 "github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/chain/actors/builtin"

	"github.com/filecoin-project/lotus/chain/consensus/filcns"

	"math/big"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/actors/builtin/power"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/specs-actors/v4/actors/util/adt"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var minerTypesCmd = &cli.Command{
	Name:  "miner-types",
	Usage: "Scrape state to report on how many miners of each WindowPoStProofType exist", Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := context.TODO()

		if !cctx.Args().Present() {
			return fmt.Errorf("must pass state root")
		}

		sroot, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("failed to parse input: %w", err)
		}

		fsrepo, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return err
		}

		lkrepo, err := fsrepo.Lock(repo.FullNode)
		if err != nil {
			return err
		}

		defer lkrepo.Close() //nolint:errcheck

		bs, err := lkrepo.Blockstore(ctx, repo.UniversalBlockstore)
		if err != nil {
			return fmt.Errorf("failed to open blockstore: %w", err)
		}

		defer func() {
			if c, ok := bs.(io.Closer); ok {
				if err := c.Close(); err != nil {
					log.Warnf("failed to close blockstore: %s", err)
				}
			}
		}()

		mds, err := lkrepo.Datastore(context.Background(), "/metadata")
		if err != nil {
			return err
		}

		cs := store.NewChainStore(bs, bs, mds, filcns.Weight, nil)
		defer cs.Close() //nolint:errcheck

		cst := cbor.NewCborStore(bs)
		store := adt.WrapStore(ctx, cst)

		tree, err := state.LoadStateTree(cst, sroot)
		if err != nil {
			return err
		}

		typeMap := make(map[abi.RegisteredPoStProof]int64)
		pa, err := tree.GetActor(power.Address)
		if err != nil {
			return err
		}

		ps, err := power.Load(store, pa)
		if err != nil {
			return err
		}

		dc := 0
		dz := power.Claim{
			RawBytePower:    abi.NewStoragePower(0),
			QualityAdjPower: abi.NewStoragePower(0),
		}

		err = tree.ForEach(func(addr address.Address, act *types.Actor) error {
			if builtin.IsStorageMinerActor(act.Code) {
				ms, err := miner.Load(store, act)
				if err != nil {
					return err
				}

				mi, err := ms.Info()
				if err != nil {
					return err
				}

				if mi.WindowPoStProofType == abi.RegisteredPoStProof_StackedDrgWindow64GiBV1 {
					mp, f, err := ps.MinerPower(addr)
					if err != nil {
						return err
					}

					if f && mp.RawBytePower.Cmp(big.NewInt(10<<40)) >= 0 && mp.RawBytePower.Cmp(big.NewInt(20<<40)) < 0 {
						dc = dc + 1
						dz.RawBytePower = big2.Add(dz.RawBytePower, mp.RawBytePower)
						dz.QualityAdjPower = big2.Add(dz.QualityAdjPower, mp.QualityAdjPower)
					}
				}

				c, f := typeMap[mi.WindowPoStProofType]
				if !f {
					typeMap[mi.WindowPoStProofType] = 1
				} else {
					typeMap[mi.WindowPoStProofType] = c + 1
				}
			}
			return nil
		})
		if err != nil {
			return xerrors.Errorf("failed to loop over actors: %w", err)
		}

		for k, v := range typeMap {
			fmt.Println("Type:", k, " Count: ", v)
		}

		fmt.Println("Mismatched power (raw, QA): ", dz.RawBytePower, " ", dz.QualityAdjPower)
		fmt.Println("Mismatched 64 GiB miner count: ", dc)

		return nil
	},
}

var minerPowerAuditCmd = &cli.Command{
	Name:  "miner-pledge-audit",
	Usage: "Scrape state to calculate total miner pledge invariant", Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := context.TODO()

		if !cctx.Args().Present() {
			return fmt.Errorf("must pass state root")
		}

		sroot, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("failed to parse input: %w", err)
		}

		fsrepo, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return err
		}

		lkrepo, err := fsrepo.Lock(repo.FullNode)
		if err != nil {
			return err
		}

		defer lkrepo.Close() //nolint:errcheck

		bs, err := lkrepo.Blockstore(ctx, repo.UniversalBlockstore)
		if err != nil {
			return fmt.Errorf("failed to open blockstore: %w", err)
		}

		defer func() {
			if c, ok := bs.(io.Closer); ok {
				if err := c.Close(); err != nil {
					log.Warnf("failed to close blockstore: %s", err)
				}
			}
		}()

		mds, err := lkrepo.Datastore(context.Background(), "/metadata")
		if err != nil {
			return err
		}

		cs := store.NewChainStore(bs, bs, mds, filcns.Weight, nil)
		defer cs.Close() //nolint:errcheck

		cst := cbor.NewCborStore(bs)
		store := adt.WrapStore(ctx, cst)

		tree, err := state.LoadStateTree(cst, sroot)
		if err != nil {
			return err
		}

		pa, err := tree.GetActor(power.Address)
		if err != nil {
			return err
		}

		ps, err := power.Load(store, pa)
		if err != nil {
			return err
		}

		pstp, err := ps.TotalLocked()
		if err != nil {
			return err
		}

		tlf := miner.LockedFunds{
			VestingFunds:             big2.Zero(),
			PreCommitDeposits:        big2.Zero(),
			InitialPledgeRequirement: big2.Zero(),
		}

		err = tree.ForEach(func(addr address.Address, act *types.Actor) error {
			if builtin.IsStorageMinerActor(act.Code) {
				ms, err := miner.Load(store, act)
				if err != nil {
					return err
				}

				lf, err := ms.LockedFunds()
				if err != nil {
					return err
				}

				tlf = miner.AddLockedFunds(tlf, lf)

			}
			return nil
		})
		if err != nil {
			return xerrors.Errorf("failed to loop over actors: %w", err)
		}

		if pstp.Equals(tlf.InitialPledgeRequirement) {
			fmt.Println("nothing to see here folks")
		} else {
			fmt.Println("mismatch")
			fmt.Println("power state locked: ", pstp)
			fmt.Println("total miner locked: ", big2.Add(tlf.PreCommitDeposits, tlf.InitialPledgeRequirement))
			fmt.Println("miner pcd locked: ", tlf.PreCommitDeposits)
			fmt.Println("miner ip locked: ", tlf.InitialPledgeRequirement)
		}

		return nil
	},
}
