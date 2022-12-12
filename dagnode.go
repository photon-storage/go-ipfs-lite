package ipfs

import (
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	ft "github.com/ipfs/go-unixfs"
	unixfs "github.com/ipfs/go-unixfs"
	pb "github.com/ipfs/go-unixfs/pb"
)

// ExtendedNode extends ipld.Node interface with extra file size and origin
// chunk data.
type ExtendedNode struct {
	ipld.Node
	// True if this node is the root of a DAG tree.
	isRoot bool
	// File size. It should be the size of a file portion in leaf node or
	// aggregated size of leaf nodes under an internal node.
	// Note: this is different from ipld.Node.Size() which is the serialized
	// node size.
	size uint64
	// origin is not nil if this is a leaf node and contains chunk data
	// supplied data provider. The chunk can contain custom tag data if
	// supplied data provider.
	origin *Chunk
	// tag is a global tag data supplied in the PutOpts. Both the this tag
	// and chunk's tag are passed through straight to blockstore, which can
	// interpret them if needed.
	tag any
}

func NewExtendedNode(
	nd ipld.Node,
	size uint64,
	c *Chunk,
	tag any,
) *ExtendedNode {
	return &ExtendedNode{
		Node:   nd,
		size:   size,
		origin: c,
		tag:    tag,
	}
}

// SetRoot marks the node as DAG root.
func (n *ExtendedNode) SetRoot() {
	n.isRoot = true
}

// Root returns if the node is a DAG root.
func (n *ExtendedNode) Root() bool {
	return n.isRoot
}

// FileSize returns the file (or portion of it) size represented by
// the current node.
func (n *ExtendedNode) FileSize() uint64 {
	return n.size
}

// Chunk returns the original chunk supplied by dataProvider.
// Nil for non-leaf node.
func (n *ExtendedNode) Chunk() *Chunk {
	return n.origin
}

// PutOptsTag returns the passed through tag from PutOpts.
func (n *ExtendedNode) PutOptsTag() any {
	return n.tag
}

// Base returns the base ipld.Node type.
func (n *ExtendedNode) Base() ipld.Node {
	return n.Node
}

// dagNode is a helper node for building DAG internal node with children.
// The dagNode includes an FSNode that serves as a cache for the ProtoNode.
// Instead of just having a single ipld.Node that would need to be constantly
// (un)packed to access and modify its internal FSNode in the process of
// creating a UnixFS DAG, this structure stores an FSNode cache to manipulate
// it (add child nodes) directly. When get() is called, the cache is finalized
// to stored into ProtoNode. Each dagNode can only be finalized once. Updates
// come after finalization won't be included.
type dagNode struct {
	pn     *merkledag.ProtoNode
	cache  *unixfs.FSNode
	origin *Chunk
	tag    any
	// finalized node.
	finalized *ExtendedNode
}

// NewDagNode creates a new empty dagNode.
func NewDagNode(t pb.Data_DataType, cb cid.Builder, tag any) *dagNode {
	dn := &dagNode{
		pn:    &merkledag.ProtoNode{},
		cache: ft.NewFSNode(t),
		tag:   tag,
	}

	dn.pn.SetCidBuilder(cb)
	return dn
}

// newFinalizedDagNode creates a new finalized dagNode.
// This is used to carry RawNode for leaf.
func newFinalizedDagNode(nd ipld.Node, size uint64, c *Chunk, tag any) *dagNode {
	return &dagNode{
		finalized: NewExtendedNode(nd, size, c, tag),
	}
}

// Get returns the finalized ProtoNode, its size and associated chunk given
// by dataProvider. If the dagNode has not been finalized, it first encodes
// the cached FSNode and store the resulting data into ProtoNode. The finalized
// node is returned as a single ExtendedNode.
func (n *dagNode) Get() (*ExtendedNode, error) {
	if n.finalized == nil {
		data, err := n.cache.GetBytes()
		if err != nil {
			return nil, err
		}

		n.pn.SetData(data)
		n.finalized = NewExtendedNode(n.pn, n.cache.FileSize(), n.origin, n.tag)
	}

	return n.finalized, nil
}

// SetData stores the provided chunk data in the FSNode. It should be used
// only when it represents a leaf node (internal nodes don't carry data,
// just file size).
func (n *dagNode) SetData(c *Chunk) {
	n.cache.SetData(c.Data)
	n.origin = c
}

// addChild updates both ProtoNode node and cached FSNode to reflect the change.
// The ProtoNode creates a link to the child node while the FSNode stores
// its file size (that is, not the size of the node but the size of the file
// data that it is storing at the UnixFS layer).
func (n *dagNode) addChild(child *dagNode) error {
	en, err := child.Get()
	if err != nil {
		return err
	}

	if err := n.pn.AddNodeLink("", en); err != nil {
		return err
	}

	n.cache.AddBlockSize(en.FileSize())
	return nil
}

// removeChild deletes the child node at the given index.
func (n *dagNode) removeChild(index int) {
	n.cache.RemoveBlockSize(index)
	n.pn.SetLinks(append(n.pn.Links()[:index], n.pn.Links()[index+1:]...))
}

// numChildren returns the number of children of the FSNode.
func (n *dagNode) numChildren() int {
	return n.cache.NumChildren()
}

// fileSize returns the Filesize attribute from the underlying representation
// of the FSNode.
func (n *dagNode) fileSize() uint64 {
	return n.cache.FileSize()
}
