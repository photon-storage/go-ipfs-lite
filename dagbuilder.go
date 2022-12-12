package ipfs

import (
	"context"
	"errors"
	"io"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	pb "github.com/ipfs/go-unixfs/pb"
)

var (
	// BlockSizeLimit specifies the maximum size an imported block can have.
	BlockSizeLimit = 1048576 // 1 MB

	// rough estimates on expected sizes
	// 8KB
	roughLinkBlockSize = 1 << 13
	// sha256 multihash + size + no name + protobuf framing
	roughLinkSize = 34 + 8 + 5

	// defaultLinksPerBlock governs how the importer decides how many links
	// there will be per block. This calculation is based on expected
	// distributions of:
	//  * the expected distribution of block sizes
	//  * the expected distribution of link sizes
	//  * desired access speed
	// For now, we use:
	//
	//   var roughLinkBlockSize = 1 << 13 // 8KB
	//   var roughLinkSize = 34 + 8 + 5   // sha256 multihash + size + no name
	//                                    // + protobuf framing
	//   var defaultLinksPerBlock = (roughLinkBlockSize / roughLinkSize)
	//                            = ( 8192 / 47 )
	//                            = (approximately) 174
	defaultLinksPerBlock = roughLinkBlockSize / roughLinkSize
)

var (
	// ErrSizeLimitExceeded signals that a block is larger than BlockSizeLimit.
	ErrSizeLimitExceeded = errors.New("object size limit exceeded")
)

// dagBuilder wraps together a bunch of objects needed to
// efficiently create unixfs dag trees
type dagBuilder struct {
	ctx        context.Context
	dp         dataProvider
	cidBuilder cid.Builder
	dserv      ipld.DAGService
	opts       *PutOpts
	maxLinks   int
	nextData   *Chunk // the next item to return.
	recvdErr   error
}

// newDagBuilder creates a new dagBuilder instance.
func newDagBuilder(
	ctx context.Context,
	dp dataProvider,
	cidBuilder cid.Builder,
	dserv ipld.DAGService,
	opts *PutOpts,
	maxlinks int,
) *dagBuilder {
	if maxlinks == 0 {
		maxlinks = defaultLinksPerBlock
	}
	return &dagBuilder{
		ctx:        ctx,
		dp:         dp,
		cidBuilder: cidBuilder,
		dserv:      dserv,
		opts:       opts,
		maxLinks:   maxlinks,
	}
}

// prepareNextData consumes the next item from the dataProvider and puts
// it in the nextData field. it is idempotent-- if nextData is full it
// will do nothing.
func (db *dagBuilder) prepareNextData() {
	// if we already have data waiting to be consumed, we're ready
	if db.nextData != nil || db.recvdErr != nil {
		return
	}

	db.nextData, db.recvdErr = db.dp.Next()
	if db.recvdErr == io.EOF {
		db.recvdErr = nil
	}
}

// done returns whether or not we're done consuming the incoming data.
func (db *dagBuilder) done() bool {
	// ensure we have an accurate perspective on data
	// as `done` this may be called before `next`.
	db.prepareNextData() // idempotent

	if db.recvdErr != nil {
		return false
	}
	return db.nextData == nil
}

// getCidBuilder returns the internal `cid.CidBuilder` set in the builder.
func (db *dagBuilder) getCidBuilder() cid.Builder {
	return db.cidBuilder
}

// getOpts returns the put options.
func (db *dagBuilder) getOpts() *PutOpts {
	return db.opts
}

// getMaxLinks returns the configured maximum number for links for nodes
// built with this helper.
func (db *dagBuilder) getMaxLinks() int {
	return db.maxLinks
}

// addToDAG inserts the given node in the DAGService.
func (db *dagBuilder) addToDAG(en *ExtendedNode) error {
	return db.dserv.Add(db.ctx, en)
}

// NewLeafNode creates a leaf node filled with data. If rawLeaves is
// defined then a raw leaf will be returned. Otherwise, it will create
// and return `cacheProtoNode` with `fsNodeType`.
func (db *dagBuilder) newLeafNode(t pb.Data_DataType) (*dagNode, error) {
	db.prepareNextData() // idempotent
	if db.recvdErr != nil {
		return nil, db.recvdErr
	}

	piece := db.nextData
	if piece.Size() > BlockSizeLimit {
		return nil, ErrSizeLimitExceeded
	}
	db.nextData = nil // signal we've consumed it

	if db.opts.RawLeaves {
		// Encapsulate the data in a raw node.
		nd, err := merkledag.NewRawNodeWPrefix(piece.Data, db.cidBuilder)
		if err != nil {
			return nil, err
		}

		return newFinalizedDagNode(
			nd,
			uint64(piece.Size()),
			piece,
			db.opts.Tag,
		), nil
	}

	dn := NewDagNode(t, db.getCidBuilder(), db.getOpts().Tag)
	dn.SetData(piece)
	return dn, nil
}
