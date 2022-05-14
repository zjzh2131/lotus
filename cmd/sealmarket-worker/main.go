package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/filecoin-project/lotus/cmd/sealmarket-worker/sealmarket"
	"github.com/google/uuid"

	logging "github.com/ipfs/go-log/v2"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/lotus/node/repo"
)

var log = logging.Logger("main")

const FlagSealmarketRepo = "sealmarket-repo"

func main() {
	api.RunningNodeType = api.NodeWorker

	lotuslog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
		setCmd,
		waitQuietCmd,
	}

	app := &cli.App{
		Name:                 "sealmarket-worker",
		Usage:                "Sealing market worker",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagSealmarketRepo,
				EnvVars: []string{"SEALMARKET_PATH"},
				Value:   "~/.sealmarket-worker", // TODO: Consider XDG_DATA_HOME
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
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagSealmarketRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = SealMarketRepo

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start sealmarket worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "host address and port the worker api will listen on",
			Value: "0.0.0.0:4456",
		},
		&cli.StringFlag{
			Name:   "address",
			Hidden: true,
		},
		&cli.StringFlag{
			Name:  "timeout",
			Usage: "used when 'listen' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function",
			Value: "30m",
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "specify wallet to pay for proof services from",
		},
		&cli.StringFlag{
			Name:  "libp2p-listen",
			Usage: "host address and port the worker p2p will listen on",
			Value: "/ip4/0.0.0.0/tcp/6377",
		},
	},
	Before: func(cctx *cli.Context) error {
		if cctx.IsSet("address") {
			log.Warnf("The '--address' flag is deprecated, it has been replaced by '--listen'")
			if err := cctx.Set("listen", cctx.String("address")); err != nil {
				return err
			}
		}

		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting seal market client")

		// Connect to storage-miner
		ctx := lcli.ReqContext(cctx)

		var nodeApi api.StorageMiner
		var closer func()
		for {
			var err error
			nodeApi, closer, err = lcli.GetStorageMinerAPI(cctx, cliutil.StorageMinerUseHttp)
			if err == nil {
				_, err = nodeApi.Version(ctx)
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
			var err error
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

		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}
		if v.APIVersion != api.MinerAPIVersion0 {
			return xerrors.Errorf("lotus-miner API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.MinerAPIVersion0})
		}
		log.Infof("Remote version %s", v)

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

		// Check params

		act, err := nodeApi.ActorAddress(ctx)
		if err != nil {
			return err
		}

		_ = act // todo

		// Open repo

		repoPath := cctx.String(FlagSealmarketRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(SealMarketRepo); err != nil {
				return err
			}

			lr, err := r.Lock(SealMarketRepo)
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

		lr, err := r.Lock(SealMarketRepo)
		if err != nil {
			return err
		}
		defer func() {
			if err := lr.Close(); err != nil {
				log.Error("closing repo", err)
			}
		}()
		ds, err := lr.Datastore(context.Background(), "/metadata")
		if err != nil {
			return err
		}

		_ = ds // todo

		log.Info("Opening local storage; connecting to master")
		const unspecifiedAddress = "0.0.0.0"
		address := cctx.String("listen")
		addressSlice := strings.Split(address, ":")
		if ip := net.ParseIP(addressSlice[0]); ip != nil {
			if ip.String() == unspecifiedAddress {
				timeout, err := time.ParseDuration(cctx.String("timeout"))
				if err != nil {
					return err
				}
				rip, err := extractRoutableIP(timeout)
				if err != nil {
					return err
				}
				address = rip + ":" + addressSlice[1]
			}
		}

		// Create / expose the worker

		// TODO real fake url
		discovery := "https://gist.githubusercontent.com/magik6k/e154ec350e9d1346141de50311160c85/raw/eb04a10108036e3abd2e7ff452e9045f175e6964/local.pi"
		if err != nil {
			panic(err)
		}
		wc, err := sealmarket.NewWorkerCalls(cctx.Context, fullNodeApi, nodeApi, addr, discovery, cctx.String("libp2p-listen"))
		if err != nil {
			panic(err)
		}
		workerApi := &sealmarket.Worker{
			WorkerCalls: wc,
			Sess:        uuid.New(),
		}

		log.Info("Setting up control endpoint at " + address)

		srv := &http.Server{
			Handler: sealmarket.WorkerHandler(nodeApi.AuthVerify, workerApi, true),
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "lotus-worker"))
				return ctx
			},
		}

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		{
			a, err := net.ResolveTCPAddr("tcp", address)
			if err != nil {
				return xerrors.Errorf("parsing address: %w", err)
			}

			ma, err := manet.FromNetAddr(a)
			if err != nil {
				return xerrors.Errorf("creating api multiaddress: %w", err)
			}

			if err := lr.SetAPIEndpoint(ma); err != nil {
				return xerrors.Errorf("setting api endpoint: %w", err)
			}

			ainfo, err := lcli.GetAPIInfo(cctx, repo.StorageMiner)
			if err != nil {
				return xerrors.Errorf("could not get miner API info: %w", err)
			}

			// TODO: ideally this would be a token with some permissions dropped
			if err := lr.SetAPIToken(ainfo.Token); err != nil {
				return xerrors.Errorf("setting api token: %w", err)
			}
		}

		minerSession, err := nodeApi.Session(ctx)
		if err != nil {
			return xerrors.Errorf("getting miner session: %w", err)
		}

		waitQuietCh := func() chan struct{} {
			out := make(chan struct{})
			go func() {
				//workerApi.LocalWorker.WaitQuiet() todo
				close(out)
			}()
			return out
		}

		go func() {
			heartbeats := time.NewTicker(stores.HeartbeatInterval)
			defer heartbeats.Stop()

			var readyCh chan struct{}
			for {
				if readyCh == nil {
					log.Info("Making sure no local tasks are running")
					readyCh = waitQuietCh()
				}

				for {
					curSession, err := nodeApi.Session(ctx)
					if err != nil {
						log.Errorf("heartbeat: checking remote session failed: %+v", err)
					} else {
						if curSession != minerSession {
							minerSession = curSession
							break
						}
					}

					select {
					case <-readyCh:
						if err := nodeApi.WorkerConnect(ctx, "http://"+address+"/rpc/v0"); err != nil {
							log.Errorf("Registering worker failed: %+v", err)
							cancel()
							return
						}

						log.Info("Worker registered successfully, waiting for tasks")

						readyCh = nil
					case <-heartbeats.C:
					case <-ctx.Done():
						return // graceful shutdown
					}
				}

				log.Errorf("LOTUS-MINER CONNECTION LOST")
			}
		}()

		return srv.Serve(nl)
	},
}

func extractRoutableIP(timeout time.Duration) (string, error) {
	minerMultiAddrKey := "MINER_API_INFO"
	deprecatedMinerMultiAddrKey := "STORAGE_API_INFO"
	env, ok := os.LookupEnv(minerMultiAddrKey)
	if !ok {
		// TODO remove after deprecation period
		_, ok = os.LookupEnv(deprecatedMinerMultiAddrKey)
		if ok {
			log.Warnf("Using a deprecated env(%s) value, please use env(%s) instead.", deprecatedMinerMultiAddrKey, minerMultiAddrKey)
		}
		return "", xerrors.New("MINER_API_INFO environment variable required to extract IP")
	}
	minerAddr := strings.Split(env, "/")
	conn, err := net.DialTimeout("tcp", minerAddr[2]+":"+minerAddr[4], timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck

	localAddr := conn.LocalAddr().(*net.TCPAddr)

	return strings.Split(localAddr.IP.String(), ":")[0], nil
}

type repoType struct {
}

var SealMarketRepo repoType

func (repoType) Type() string {
	return "Sealmarket"
}

func (repoType) Config() interface{} {
	return &struct{}{}
}

func (repoType) APIFlags() []string {
	return []string{"sealmarket-api-url"}
}

func (repoType) RepoFlags() []string {
	return []string{"sealmarket-repo"}
}

func (repoType) APIInfoEnvVars() (primary string, fallbacks []string, deprecated []string) {
	return "SEALMARKET_API_INFO", nil, nil
}
