package sealmarket

import (
	"context"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/filecoin-project/lotus/lib/addrutil"
)

// TODO with lotus api we could instead start up a process which trawls
// latest state for non-empty miner infos with peer ids and addrs
// we could then connect to their snarky ask protocol to see if
//they are running it and if so add to a table of providers
//
// Probably we could also lift lotus dht peer discovery mechanism too

func DiscoverProviders(ctx context.Context, totallyDecentralizedURL string) ([]provider, error) {
	resp, err := http.Get(totallyDecentralizedURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// format is newline separated "PeerAddresses" (multiaddr + peerid)
	raw, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	peerAddrs := strings.Split(string(raw), "\n")
	addrInfos, err := addrutil.ParseAddresses(ctx, peerAddrs)
	if err != nil {
		return nil, err
	}
	ret := make([]provider, 0)
	for _, p := range addrInfos {
		ret = append(ret, provider{
			peer: p,
		})
	}
	return ret, nil
}
