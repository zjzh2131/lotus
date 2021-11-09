//go:build fissionnet
// +build fissionnet

package build

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/ipfs/go-cid"

	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
)

var DrandSchedule = map[abi.ChainEpoch]DrandEnum{
	0: DrandMainnet,
}

const GenesisNetworkVersion = network.Version14

const BootstrappersFile = "fissionnet.pi"
const GenesisFile = "fissionnet.car"

const UpgradeBreezeHeight = -1
const BreezeGasTampingDuration = -2
const UpgradeSmokeHeight = -3
const UpgradeIgnitionHeight = -4
const UpgradeRefuelHeight = -5
const UpgradeLiftoffHeight = -6
const UpgradeAssemblyHeight = abi.ChainEpoch(-7)
const UpgradeTapeHeight = -8
const UpgradeKumquatHeight = -9
const UpgradeCalicoHeight = -10
const UpgradePersianHeight = -11
const UpgradeClausHeight = -12
const UpgradeOrangeHeight = -13
const UpgradeTrustHeight = -14
const UpgradeNorwegianHeight = -15
const UpgradeTurboHeight = -16
const UpgradeHyperdriveHeight = -17
const UpgradeChocolateHeight = -18

func init() {
	policy.SetConsensusMinerMinPower(abi.NewStoragePower(5368709120))
	policy.SetMinVerifiedDealSize(abi.NewStoragePower(1048576))

	policy.SetSupportedProofTypes(abi.RegisteredSealProof_StackedDrg512MiBV1)

	// Lower the most time-consuming parts of PoRep
	policy.SetPreCommitChallengeDelay(10)

	// TODO - make this a variable
	//miner.WPoStChallengeLookback = abi.ChainEpoch(2)

	Devnet = false

	BuildType = BuildFissionnet

}

const BlockDelaySecs = uint64(builtin2.EpochDurationSeconds)

const PropagationDelaySecs = uint64(6)

// BootstrapPeerThreshold is the minimum number peers we need to track for a sync worker to start
const BootstrapPeerThreshold = 1

var WhitelistedBlock = cid.Undef
