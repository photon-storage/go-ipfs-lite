package ipfs

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/photon-storage/go-common/log"
)

const discoveryConnTimeout = time.Second * 30

// discoveryHandler defines a simple connection handler used by libp2p
// discovery service.
type discoveryHandler struct {
	ctx  context.Context
	host host.Host
}

func newDiscoveryHandler(ctx context.Context, host host.Host) *discoveryHandler {
	return &discoveryHandler{
		ctx:  ctx,
		host: host,
	}
}

func (dh *discoveryHandler) HandlePeerFound(p peer.AddrInfo) {
	log.Info("Connecting to discovered peer", "peer_id", p.ID)
	ctx, cancel := context.WithTimeout(dh.ctx, discoveryConnTimeout)
	defer cancel()

	if err := dh.host.Connect(ctx, p); err != nil {
		log.Warn("Error connecting to peer", "peer_id", p.ID, "error", err)
	}
}
