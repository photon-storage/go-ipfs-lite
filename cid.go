package ipfs

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	chunk "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
)

// CidOpts contains configurable parameters for generating CID.
type CidOpts struct {
	// DataType sets which dag type to generate for the object.
	DagType DagType
	// ChunkSize sets the split size for each leave node, 0 for using default size.
	ChunkSize int64
	// Number of linkes per block in DAG.
	LinksPerBlock int
	// RawLeaves sets if leaf nodes are generated as RawNode.
	RawLeaves bool
	// CidBuilder v0 or v1. Default to v1.
	CidBuilder cid.Builder
}

// GenCid calculates CID for the given data source.
func GenCid(
	ctx context.Context,
	src any,
	opts CidOpts,
) (cid.Cid, error) {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = chunk.DefaultBlockSize
	}
	if opts.LinksPerBlock == 0 {
		opts.LinksPerBlock = defaultLinksPerBlock
	}
	if opts.CidBuilder == nil {
		opts.CidBuilder = merkledag.V1CidPrefix()
	}

	var dp dataProvider
	switch v := src.(type) {
	case dataProvider:
		dp = v
	case []byte:
		dp = NewDataProvider(
			bytes.NewReader(v),
			opts.ChunkSize,
		)
	case io.Reader:
		dp = NewDataProvider(v, opts.ChunkSize)
	default:
		return cid.Undef, ErrDataSourceTypeNotSupported
	}

	db := newDagBuilder(
		ctx,
		dp,
		opts.CidBuilder,
		&NoopDAGService{},
		&PutOpts{
			DagType:       opts.DagType,
			ChunkSize:     opts.ChunkSize,
			LinksPerBlock: opts.LinksPerBlock,
			RawLeaves:     opts.RawLeaves,
			Tag:           nil,
		},
		opts.LinksPerBlock,
	)

	var gen func(db *dagBuilder) (*ExtendedNode, error)
	switch opts.DagType {
	case DagBalanced:
		gen = genBalancedDag
	case DagTrickle:
		gen = genTrickleDag
	default:
		return cid.Undef, errors.New("invalid DAG type")
	}

	ed, err := gen(db)
	if err != nil {
		return cid.Undef, err
	}

	return ed.Cid(), nil
}

type NoopDAGService struct {
}

// Get implements the DAGService interface.
func (_ NoopDAGService) Get(context.Context, cid.Cid) (ipld.Node, error) {
	return nil, nil
}

// GetMany implements the DAGService interface.
func (_ NoopDAGService) GetMany(
	context.Context,
	[]cid.Cid,
) <-chan *ipld.NodeOption {
	return nil
}

// Add implements the DAGService interface.
func (_ NoopDAGService) Add(context.Context, ipld.Node) error {
	return nil
}

// AddMany implements the DAGService interface.
func (_ NoopDAGService) AddMany(context.Context, []ipld.Node) error {
	return nil
}

// Remove implements the DAGService interface.
func (_ NoopDAGService) Remove(context.Context, cid.Cid) error {
	return nil
}

// RemoveMany implements the DAGService interface.
func (_ NoopDAGService) RemoveMany(context.Context, []cid.Cid) error {
	return nil
}
