package ipfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/ipfs/go-bitswap"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	blockservice "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	provider "github.com/ipfs/go-ipfs-provider"
	"github.com/ipfs/go-ipfs-provider/queue"
	"github.com/ipfs/go-ipfs-provider/simple"
	offroute "github.com/ipfs/go-ipfs-routing/offline"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-ipns"
	"github.com/ipfs/go-merkledag"
	ufsio "github.com/ipfs/go-unixfs/io"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"

	"github.com/photon-storage/go-common/log"
)

var (
	ErrDataSourceTypeNotSupported = errors.New("data source type not supported")
)

// Node is a customized IPFS lite-node.
// The code was originally forked github.com/hsanjuan/ipfs-lite with
// substantial refactoring with relay support.
type Node struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cfg      Config
	host     host.Host
	disc     mdns.Service
	dht      *dual.DHT
	exch     exchange.Interface
	bserv    blockservice.BlockService
	provider provider.System
	dserv    ipld.DAGService
}

// New creates a new Node instance.
// An empty Config which fallbacks to default settings should work.
func New(ctx context.Context, cfg Config) (*Node, error) {
	cfg, err := fillConfigDefaults(ctx, cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	// Register block decoders.
	ipld.Register(cid.DagProtobuf, merkledag.DecodeProtobufBlock)
	ipld.Register(cid.Raw, merkledag.DecodeRawBlock)
	ipld.Register(cid.DagCBOR, cbor.DecodeBlock) // need to decode CBOR

	connMgr, err := connmgr.NewConnManager(
		cfg.MinConnections,
		cfg.MaxConnections,
		connmgr.WithGracePeriod(cfg.ConnectionGracePeriod),
	)
	if err != nil {
		return nil, err
	}

	var h host.Host
	var disc mdns.Service
	var ddht *dual.DHT
	var exch exchange.Interface
	var prov provider.Provider
	var reprov provider.Reprovider
	peerCh := make(chan peer.AddrInfo)
	if !cfg.OfflineMode {
		h, err = libp2p.New(
			libp2p.Identity(cfg.SecretKey),
			libp2p.ListenAddrs(cfg.ListenAddrs...),
			libp2p.DefaultTransports,
			libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
			libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
			libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
				var err error
				ddht, err = dual.New(
					ctx,
					h,
					dual.DHTOption(
						dht.Concurrency(10),
						dht.Mode(dht.ModeAuto),
						dht.Validator(record.NamespacedValidator{
							"pk":   record.PublicKeyValidator{},
							"ipns": ipns.Validator{KeyBook: h.Peerstore()},
						}),
					),
					dual.WanDHTOption(dht.BootstrapPeers(cfg.Bootstrappers...)),
				)
				return ddht, err
			}),
			libp2p.ConnectionManager(connMgr),
			libp2p.Security(libp2ptls.ID, libp2ptls.New),
			libp2p.Security(noise.ID, noise.New),
			libp2p.EnableNATService(),
			libp2p.NATPortMap(),
			libp2p.EnableRelay(),
			libp2p.EnableRelayService(),
			libp2p.EnableAutoRelay(autorelay.WithPeerSource(
				func(ctx context.Context, numPeers int) <-chan peer.AddrInfo {
					ret := make(chan peer.AddrInfo)
					go func() {
						defer close(ret)
						for i := 0; i < numPeers; i++ {
							peer, ok := <-peerCh
							if !ok {
								return
							}
							ret <- peer
						}
					}()
					return ret
				},
				time.Minute,
			)),
			libp2p.EnableHolePunching(),
		)
		if err != nil {
			return nil, err
		}

		disc = mdns.NewMdnsService(
			h,
			"",
			newDiscoveryHandler(ctx, h),
		)

		// Bitswap.
		exch = bitswap.New(
			ctx,
			bsnetwork.NewFromIpfsHost(h, ddht),
			cfg.Blockstore,
		)

		// Provider & reprovider.
		queue, err := queue.NewQueue(ctx, "repro", cfg.Datastore)
		if err != nil {
			return nil, err
		}
		prov = simple.NewProvider(ctx, queue, ddht)
		reprov = simple.NewReprovider(
			ctx,
			cfg.ReprovideInterval,
			ddht,
			simple.NewBlockstoreProvider(cfg.Blockstore),
		)
	} else {
		pstore, err := pstoremem.NewPeerstore()
		if err != nil {
			return nil, err
		}

		// Bitswap.
		exch = offline.Exchange(cfg.Blockstore)
		offrout := offroute.NewOfflineRouter(
			cfg.Datastore,
			record.NamespacedValidator{
				"pk":   record.PublicKeyValidator{},
				"ipns": ipns.Validator{KeyBook: pstore},
			},
		)

		// Provider & reprovider.
		queue, err := queue.NewQueue(ctx, "repro", cfg.Datastore)
		if err != nil {
			return nil, err
		}
		prov = simple.NewProvider(ctx, queue, offrout)
		reprov = simple.NewReprovider(
			ctx,
			cfg.ReprovideInterval,
			offrout,
			simple.NewBlockstoreProvider(cfg.Blockstore),
		)
	}

	// Blockservice.
	bserv := blockservice.NewWriteThrough(cfg.Blockstore, exch)
	n := &Node{
		ctx:      ctx,
		cancel:   cancel,
		cfg:      cfg,
		host:     h,
		disc:     disc,
		dht:      ddht,
		exch:     exch,
		bserv:    bserv,
		provider: provider.NewSystem(prov, reprov),
		dserv:    merkledag.NewDAGService(bserv),
	}
	go n.autoclose()

	if !cfg.OfflineMode {
		if err := n.disc.Start(); err != nil {
			cancel()
			return nil, err
		}

		n.provider.Run()
		go n.feedRelayPeers(peerCh)
		n.bootstrap()
	}

	return n, nil
}

func (n *Node) autoclose() {
	<-n.ctx.Done()
	n.bserv.Close()
	if !n.cfg.OfflineMode {
		n.provider.Close()
		n.disc.Close()
	}
}

// feedRelayPeers periodically scans DHT's closest peers and signal them
// to relay service as relay nodes.
func (n *Node) feedRelayPeers(peerCh chan peer.AddrInfo) {
	// Feed peers more often right after the bootstrap, then backoff
	bo := &backoff.ExponentialBackOff{
		InitialInterval:     15 * time.Second,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          3,
		MaxInterval:         1 * time.Hour,
		MaxElapsedTime:      0, // never stop
		Clock:               backoff.SystemClock,
	}
	bo.Reset()
	t := backoff.NewTicker(bo)
	defer t.Stop()

	for {
		select {
		case <-t.C:
		case <-n.ctx.Done():
			return
		}

		closestPeers, err := n.dht.WAN.GetClosestPeers(n.ctx, n.host.ID().String())
		if err != nil {
			// no-op: usually 'failed to find any peer in table' during startup
			continue
		}

		feeded := 0
		for _, pid := range closestPeers {
			addrs := n.host.Peerstore().Addrs(pid)
			if len(addrs) == 0 {
				continue
			}

			select {
			case peerCh <- peer.AddrInfo{ID: pid, Addrs: addrs}:
				feeded++

			case <-n.ctx.Done():
				return
			}
		}

		if feeded > 0 {
			log.Info("new relay peers added", "count", feeded)
		}
	}
}

// bootstrap builds connections to configured bootstrappers and bootstrap
// the Peer DHT (and Bitswap). This is a best-effort function.
func (n *Node) bootstrap() {
	var wg sync.WaitGroup
	wg.Add(len(n.cfg.Bootstrappers))
	for _, pinfo := range n.cfg.Bootstrappers {
		go func(pinfo peer.AddrInfo) {
			defer wg.Done()

			if err := n.host.Connect(n.ctx, pinfo); err != nil {
				log.Warn("error connecting to peer", "error", err)
				return
			}

			log.Info("connected bootstrapper", "peer", pinfo.ID)
		}(pinfo)
	}

	wg.Wait()

	log.Info("bootstrapping done", "num_peers", len(n.host.Network().Peers()))

	if err := n.dht.Bootstrap(n.ctx); err != nil {
		log.Error("error bootstraping DHT", "error", err)
	}
}

type DagType int

const (
	DagBalanced DagType = 0
	DagTrickle          = 1
)

// PutOpts contains configurable parameters for file DAG building.
type PutOpts struct {
	// DataType sets which dag type to generate for the object.
	DagType DagType
	// Number of linkes per block in DAG.
	LinksPerBlock int
	// RawLeaves sets if leaf nodes are generated as RawNode.
	RawLeaves bool
	// Tag is custom data to be passed through all the way to blockstore.
	Tag any
}

// PutObject chunks data supplied by io.Reader and builds a DAG for splitted
// data nodes. The root ipld.Node is returned.
func (n *Node) PutObject(
	ctx context.Context,
	src any,
	opts *PutOpts,
) (ipld.Node, error) {
	if opts == nil {
		opts = &PutOpts{}
	}

	var dp dataProvider
	switch v := src.(type) {
	case dataProvider:
		dp = v
	case []byte:
		dp = NewDataProvider(
			bytes.NewReader(v),
			chunk.DefaultBlockSize,
		)
	case io.Reader:
		dp = NewDataProvider(v, chunk.DefaultBlockSize)
	default:
		return nil, ErrDataSourceTypeNotSupported
	}

	linksPerBlock := opts.LinksPerBlock
	if linksPerBlock == 0 {
		linksPerBlock = defaultLinksPerBlock
	}

	db := newDagBuilder(
		ctx,
		dp,
		n.cfg.CidBuilder,
		n.dserv,
		opts,
		linksPerBlock,
	)

	var gen func(db *dagBuilder) (*ExtendedNode, error)
	switch opts.DagType {
	case DagBalanced:
		gen = genBalancedDag
	case DagTrickle:
		gen = genTrickleDag
	default:
		return nil, errors.New("invalid DAG type")
	}

	ed, err := gen(db)
	if err != nil {
		return nil, err
	}

	if err := n.provider.Provide(ed.Cid()); err != nil {
		return nil, err
	}

	return ed.Node, nil
}

// A ReadSeekCloser implements interfaces to read, seek and close.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// GetObject returns a reader to a data blob as identified by the given root
// CID. The object must have been added as a UnixFS DAG (default for IPFS).
func (n *Node) GetObject(ctx context.Context, c cid.Cid) (ReadSeekCloser, error) {
	nd, err := n.dserv.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	return ufsio.NewDagReader(ctx, nd, n.dserv)
}

// HasBlock returns whether a given block is available locally.
func (n *Node) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	return n.BlockStore().Has(ctx, c)
}

// Session returns a session-based NodeGetter.
func (n *Node) Session(ctx context.Context) ipld.NodeGetter {
	return merkledag.NewSession(ctx, n.dserv)
}

// BlockStore returns the blockstore.
func (n *Node) BlockStore() blockstore.Blockstore {
	return n.cfg.Blockstore
}

// Exchange returns the bitswap.
func (n *Node) Exchange() exchange.Interface {
	return n.exch
}

// BlockService returns the underlying blockservice implementation.
func (n *Node) BlockService() blockservice.BlockService {
	return n.bserv
}
