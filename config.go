package ipfs

import (
	"context"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-verifcid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
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
		bootstrappers, err := defaultBootstrapPeers()
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
			&VerifBS{
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

// The following code is copied from "github.com/ipfs/kubo/config" to avoid
// dependency on kubo package which introduces conflicts on libp2p-core.
// DefaultBootstrapAddresses are the hardcoded bootstrap addresses
// for IPFS. they are nodes run by the IPFS team. docs on these later.
// As with all p2p networks, bootstrap is an important security concern.
var defaultBootstrapAddresses = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",      // mars.i.ipfs.io
	"/ip4/104.131.131.82/udp/4001/quic/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
}

// defaultBootstrapPeers returns the (parsed) set of default bootstrap peers.
// if it fails, it returns a meaningful error for the user.
// This is here (and not inside cmd/ipfs/init) because of module dependency problems.
func defaultBootstrapPeers() ([]peer.AddrInfo, error) {
	ps, err := parseBootstrapPeers(defaultBootstrapAddresses)
	if err != nil {
		return nil, fmt.Errorf(`failed to parse hardcoded bootstrap peers: %s
This is a problem with the ipfs codebase. Please report it to the dev team`, err)
	}
	return ps, nil
}

// ParseBootstrapPeer parses a bootstrap list into a list of AddrInfos.
func parseBootstrapPeers(addrs []string) ([]peer.AddrInfo, error) {
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, err
		}
	}
	return peer.AddrInfosFromP2pAddrs(maddrs...)
}

// Copied from github.com/ipfs/kubo/thirdparty/verifbs to avoid dependency on
// Kubo, which introduces version conflicts for libp2p-core
type VerifBS struct {
	blockstore.Blockstore
}

func (bs *VerifBS) Put(ctx context.Context, b blocks.Block) error {
	if err := verifcid.ValidateCid(b.Cid()); err != nil {
		return err
	}
	return bs.Blockstore.Put(ctx, b)
}

func (bs *VerifBS) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, b := range blks {
		if err := verifcid.ValidateCid(b.Cid()); err != nil {
			return err
		}
	}
	return bs.Blockstore.PutMany(ctx, blks)
}

func (bs *VerifBS) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if err := verifcid.ValidateCid(c); err != nil {
		return nil, err
	}
	return bs.Blockstore.Get(ctx, c)
}
