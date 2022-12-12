package ipfs

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/thirdparty/verifbs"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type Config struct {
	OfflineMode           bool
	SecretKey             crypto.PrivKey
	ListenAddrs           []multiaddr.Multiaddr
	Bootstrappers         []peer.AddrInfo
	MinConnections        int
	MaxConnections        int
	ConnectionGracePeriod time.Duration
	Datastore             datastore.Batching
	Blockstore            blockstore.Blockstore
	ReprovideInterval     time.Duration
	CidBuilder            cid.Builder
}

func fillConfigDefaults(ctx context.Context, c Config) (Config, error) {
	if c.SecretKey == nil {
		sk, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		if err != nil {
			return Config{}, err
		}
		c.SecretKey = sk
	}

	if len(c.ListenAddrs) == 0 {
		for _, addr := range []string{
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic",
			"/ip6/::/tcp/0",
			"/ip6/::/udp/0/quic",
		} {

			maddr, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				return Config{}, err
			}
			c.ListenAddrs = append(c.ListenAddrs, maddr)
		}
	}

	if len(c.Bootstrappers) == 0 {
		bootstrappers, err := config.DefaultBootstrapPeers()
		if err != nil {
			return Config{}, err
		}
		c.Bootstrappers = bootstrappers
	}

	if c.MinConnections == 0 {
		c.MinConnections = 100
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 600
	}
	if c.ConnectionGracePeriod == 0 {
		c.ConnectionGracePeriod = time.Minute
	}

	if c.Datastore == nil {
		c.Datastore = dssync.MutexWrap(datastore.NewMapDatastore())
	}

	if c.Blockstore == nil {
		bs, err := blockstore.CachedBlockstore(
			ctx,
			&verifbs.VerifBS{
				Blockstore: blockstore.NewBlockstore(c.Datastore),
			},
			blockstore.DefaultCacheOpts(),
		)
		if err != nil {
			return Config{}, err
		}
		c.Blockstore = blockstore.NewIdStore(bs)
	}

	if c.ReprovideInterval == 0 {
		c.ReprovideInterval = 12 * time.Hour
	}

	if c.CidBuilder == nil {
		c.CidBuilder = merkledag.V1CidPrefix()
	}

	return c, nil
}
