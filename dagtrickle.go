package ipfs

import (
	ft "github.com/ipfs/go-unixfs"
)

// depthRepeat specifies how many times to append a child tree of a
// given depth. Higher values increase the width of a given node, which
// improves seek speeds.
const depthRepeat = 4

// genTrickleDag builds a new DAG with the trickle format using the provided
// dagBuilder. In this type of DAG, non-leave nodes are first filled with
// data leaves, and then incorporate "layers" of subtrees as additional links.
//
// Each layer is a trickle sub-tree and is limited by an increasing maximum
// depth. Thus, the nodes first layer can only hold leaves (depth 1) but
// subsequent layers can grow deeper. By default, this module places 4 nodes per
// layer (that is, 4 subtrees of the same maximum depth before increasing it).
//
// Trickle DAGs are very good for sequentially reading data, as the first data
// leaves are directly reachable from the root and those coming next are always
// nearby. They are suited for things like streaming applications.
func genTrickleDag(db *dagBuilder) (*ExtendedNode, error) {
	root := NewDagNode(ft.TFile, db.getCidBuilder(), db.getOpts().Tag)
	if err := fillTrickleRec(db, root, -1); err != nil {
		return nil, err
	}

	en, err := root.Get()
	if err != nil {
		return nil, err
	}
	en.SetRoot()
	if err := db.addToDAG(en); err != nil {
		return nil, err
	}

	return en, nil
}

// fillNodeLayer will add data nodes as children to the give node until
// it is full in this layer or no more data.
func fillNodeLayer(db *dagBuilder, dn *dagNode) error {
	// while we have room AND we're not done
	for dn.numChildren() < db.maxLinks && !db.done() {
		child, err := db.newLeafNode(ft.TRaw)
		if err != nil {
			return err
		}

		if err := dn.addChild(child); err != nil {
			return err
		}

		en, err := child.Get()
		if err != nil {
			return err
		}
		return db.addToDAG(en)
	}

	return nil
}

// fillTrickleRec creates a trickle (sub-)tree with an optional maximum
// specified depth in the case maxDepth is greater than zero, or with unlimited
// depth otherwise (where the DAG builder will signal the end of data to end
// the function).
func fillTrickleRec(db *dagBuilder, dn *dagNode, maxDepth int) error {
	// Always do this, even in the base case
	if err := fillNodeLayer(db, dn); err != nil {
		return err
	}

	// For each depth in [1, `maxDepth`) (or without limit if `maxDepth` is -1,
	// initial call from `Layout`) add `depthRepeat` sub-graphs of that depth.
	for depth := 1; maxDepth == -1 || depth < maxDepth; depth++ {
		if db.done() {
			break
			// No more data, stop here, posterior append calls will figure out
			// where we left off.
		}

		for repeatIndex := 0; repeatIndex < depthRepeat && !db.done(); repeatIndex++ {
			child := NewDagNode(ft.TFile, db.getCidBuilder(), db.getOpts().Tag)
			if err := fillTrickleRec(db, child, depth); err != nil {
				return err
			}

			if err := dn.addChild(child); err != nil {
				return err
			}

			en, err := child.Get()
			if err != nil {
				return err
			}
			if err := db.addToDAG(en); err != nil {
				return err
			}
		}
	}

	return nil
}
