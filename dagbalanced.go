package ipfs

import (
	"errors"

	ft "github.com/ipfs/go-unixfs"
)

var (
	ErrEmptyData = errors.New("empty data to build DAG")
)

// This code is forked and modified based on:
// https://github.com/ipfs/go-unixfs/blob/master/importer/balanced/builder.go

// genBalancedDag builds a balanced DAG layout. In a balanced DAG of depth 1,
// leaf nodes with data are added to a single root until the maximum number
// of links is reached. Then, to continue adding more data leaf nodes, a
// newRoot is created pointing to the old root (which will now become and
// intermediary node), increasing the depth of the DAG to 2. This will
// increase the maximum number of data leaf nodes the DAG can have
// (Maxlinks() ^ depth). The fillNodeRec function will add more
// intermediary child nodes to newRoot (which already has root as child)
// that in turn will have leaf nodes with data added to them. After that
// process is completed (the maximum number of links is reached), fillNodeRec
// will return and the loop will be repeated: the newRoot created will become
// the old root and a new root will be created again to increase the depth of
// the DAG. The process is repeated until there is no more data to add (i.e.
// the dagBuilder’s done() function returns true).
//
// The nodes are filled recursively, so the DAG is built from the bottom up.
// Leaf nodes are created first using the chunked file data and its size. The
// size is then bubbled up to the parent (internal) node, which aggregates all
// the sizes of its children and bubbles that combined size up to its parent,
// and so on up to the root. This way, a balanced DAG acts like a B-tree when
// seeking to a byte offset in the file the graph represents: each internal
// node uses the file size of its children as an index when seeking.
//
//      gen creates a root and hands it off to be filled:
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//       ( fillNodeRec fills in the )
//       ( chunks on the root.      )
//                    |
//             +------+------+
//             |             |
//        + - - - - +   + - - - - +
//        | Chunk 1 |   | Chunk 2 |
//        + - - - - +   + - - - - +
//
//                           ↓
//      When the root is full but there's more data...
//                           ↓
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//             +------+------+
//             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//
//                           ↓
//      ...gen's job is to create a new root.
//                           ↓
//
//                            +-------------+
//                            |   Root 2    |
//                            +-------------+
//                                  |
//                    +-------------+ - - - - - - - - +
//                    |                               |
//             +-------------+            ( fillNodeRec creates the )
//             |   Node 1    |            ( branch that connects    )
//             +-------------+            ( "Root 2" to "Chunk 3."  )
//                    |                               |
//             +------+------+             + - - - - -+
//             |             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//
func genBalancedDag(db *dagBuilder) (*ExtendedNode, error) {
	if db.done() {
		return nil, ErrEmptyData
	}

	// The first root will be a single leaf node with data (corner case),
	// after that subsequent root nodes will always be internal nodes
	// (with a depth > 0) that can be handled by the loop.
	root, err := db.newLeafNode(ft.TFile)
	if err != nil {
		return nil, err
	}

	// Each time a DAG of a certain depth is filled (because it has reached
	// its maximum capacity of db.Maxlinks() per node) extend it by making
	// it a sub-DAG of a bigger DAG with depth+1.
	for depth := 1; !db.done(); depth++ {
		// Add the old root as a child of the newRoot.
		newRoot := NewDagNode(ft.TFile, db.getCidBuilder(), db.getOpts().Tag)
		if err := newRoot.addChild(root); err != nil {
			return nil, err
		}
		en, err := root.Get()
		if err != nil {
			return nil, err
		}
		if err := db.addToDAG(en); err != nil {
			return nil, err
		}

		// Fill the newRoot (that has the old root already as child)
		// and make it the current root for the next iteration (when
		// it will become "old").
		if err := fillNodeRec(db, newRoot, depth); err != nil {
			return nil, err
		}
		root = newRoot
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

// fillNodeRec will "fill" the given internal (non-leaf) node with data by
// adding child nodes to it, either leaf data nodes (if depth is 1) or more
// internal nodes with higher depth (and calling itself recursively on them
// until *they* are filled with data). The data to fill the node with is
// provided by DagBuilderHelper.
//
// node represents a (sub-)DAG root that is being filled. If called
// recursively, it is nil, a new node is created. If it has been called from
// gen (see diagram below) it points to the new root (that increases the
// depth of the DAG), it already has a child (the old root). New children will
// be added to this new root, and those children will in turn be filled
// (calling fillNodeRec recursively).
//
//                      +-------------+
//                      |     node    |
//                      |  (new root) |
//                      +-------------+
//                            |
//              +-------------+ - - - - - - + - - - - - - - - - - - +
//              |                           |                       |
//      +--------------+             + - - - - -  +           + - - - - -  +
//      |  (old root)  |             |  new child |           |            |
//      +--------------+             + - - - - -  +           + - - - - -  +
//              |                          |                        |
//       +------+------+             + - - + - - - +
//       |             |             |             |
//  +=========+   +=========+   + - - - - +    + - - - - +
//  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |    | Chunk 4 |
//  +=========+   +=========+   + - - - - +    + - - - - +
//
// warning: **children** pinned indirectly, but input node IS NOT pinned.
func fillNodeRec(db *dagBuilder, dn *dagNode, depth int) error {
	if depth < 1 {
		return errors.New("attempt to fillNode at depth < 1")
	}

	// Child node created on every iteration to add to parent node.
	// It can be a leaf node or another internal node.
	var child *dagNode

	// While we have room and there is data available to be added.
	for dn.numChildren() < db.getMaxLinks() && !db.done() {
		if depth == 1 {
			var err error
			// Base case: add leaf node with data.
			if child, err = db.newLeafNode(ft.TFile); err != nil {
				return err
			}
		} else {
			// Recursion case: create an internal node to in turn keep
			// descending in the DAG and adding child nodes to it.
			child = NewDagNode(ft.TFile, db.getCidBuilder(), db.getOpts().Tag)
			if err := fillNodeRec(db, child, depth-1); err != nil {
				return err
			}
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

	return nil
}
