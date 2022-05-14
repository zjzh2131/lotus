package main

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"os"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	"github.com/filecoin-project/lotus/node/repo"

	proversvc "github.com/filecoin-project/lotus/snarky"
)

var log = logging.Logger("main")

const FlagSealProviderRepo = "sealprovider-repo"

func main() {

	lotuslog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	app := &cli.App{
		Name:                 "sealmarket-provider",
		Usage:                "Sealing market provider",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagSealProviderRepo,
				EnvVars: []string{"SEAL_PROVIDER_PATH"},
				Value:   "~/.sealmarket", // TODO: Consider XDG_DATA_HOME
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"LOTUS_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.lotusworker", // should follow --repo default
			},
			&cli.StringFlag{
				Name:    "miner-repo",
				Aliases: []string{"storagerepo"},
				EnvVars: []string{"LOTUS_MINER_PATH", "LOTUS_STORAGE_PATH"},
				Value:   "~/.lotusminer", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify miner repo path. flag storagerepo and env LOTUS_STORAGE_PATH are DEPRECATION, will REMOVE SOON"),
			},
			&cli.StringFlag{
				Name:    "repo",
				EnvVars: []string{"LOTUS_PATH"},
				Value:   "~/.lotus",
				Usage:   fmt.Sprintf("Specify lotus node path."),
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in LOTUS_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagSealProviderRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = SealProviderRepo

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start sealmarket provider",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "libp2p-listen",
			Usage: "host address and port the provider api will listen on",
			Value: "/ip4/0.0.0.0/tcp/7763",
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "specify wallet to receive payment for proof services into",
		},
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting seal market client")

		// Connect to storage-miner
		ctx := lcli.ReqContext(cctx)

		var minerApi api.StorageMiner
		var closer func()
		var err error

		for {
			minerApi, closer, err = lcli.GetStorageMinerAPI(cctx, cliutil.StorageMinerUseHttp)
			if err == nil {
				_, err = minerApi.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()

		var fullNodeApi api.FullNode
		var lcloser func()
		for {
			fullNodeApi, closer, err = lcli.GetFullNodeAPIV1(cctx) // todo use http
			if err == nil {
				_, err = fullNodeApi.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to full node API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer lcloser()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Open repo

		repoPath := cctx.String(FlagSealProviderRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(SealProviderRepo); err != nil {
				return err
			}

			lr, err := r.Lock(SealProviderRepo)
			if err != nil {
				return err
			}

			var localPaths []stores.LocalPath

			if err := lr.SetStorage(func(sc *stores.StorageConfig) {
				sc.StoragePaths = append(sc.StoragePaths, localPaths...)
			}); err != nil {
				return xerrors.Errorf("set storage config: %w", err)
			}

			{
				// init datastore for r.Exists
				_, err := lr.Datastore(context.Background(), "/metadata")
				if err != nil {
					return err
				}
			}
			if err := lr.Close(); err != nil {
				return xerrors.Errorf("close repo: %w", err)
			}
		}

		lr, err := r.Lock(SealProviderRepo)
		if err != nil {
			return err
		}
		defer func() {
			if err := lr.Close(); err != nil {
				log.Error("closing repo", err)
			}
		}()

		var addr address.Address
		if w := cctx.String("wallet"); w != "" {
			a, err := address.NewFromString(w)
			if err != nil {
				return err
			}

			addr = a
		}
		if addr == address.Undef {
			defaddr, err := fullNodeApi.WalletDefaultAddress(ctx)
			if err != nil {
				return err
			}

			addr = defaddr
		}

		ds, err := lr.Datastore(context.Background(), "/metadata")
		if err != nil {
			return err
		}

		// Create / expose the provider

		host, err := libp2p.New(libp2p.ListenAddrStrings(cctx.String("libp2p-listen")))
		if err != nil {
			return err
		}

		addrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
			ID:    host.ID(),
			Addrs: host.Addrs(),
		})
		if err != nil {
			return xerrors.Errorf("getting addrs: %w", err)
		}
		fmt.Println("---- BEGIN PROVIDER ADDRS ----")
		for _, multiaddr := range addrs {
			fmt.Println(multiaddr)
		}
		fmt.Println("---- END PROVIDER ADDRS ----")

		cfg := &proversvc.Config{
			ProveCommitEnable:  true,
			ProveCommitParJobs: 5,
			Address:            addr,
		}

		svc, err := proversvc.NewProvingService(host, cfg, minerApi, fullNodeApi, ds)
		if err != nil {
			return err
		}

		_ = svc

		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	},
}

type repoType struct {
}

var SealProviderRepo repoType

func (repoType) Type() string {
	return "Provider"
}

func (repoType) Config() interface{} {
	return &struct{}{}
}

func (repoType) APIFlags() []string {
	return []string{}
}

func (repoType) RepoFlags() []string {
	return []string{"worker-repo"}
}

func (repoType) APIInfoEnvVars() (primary string, fallbacks []string, deprecated []string) {
	return "PROVIDER_API_INFO", nil, nil
}
