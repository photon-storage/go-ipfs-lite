package ipfs

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	blockservice "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	u "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	dag "github.com/ipfs/go-merkledag"
	uio "github.com/ipfs/go-unixfs/io"

	"github.com/photon-storage/go-common/testing/require"
)

type countedBlockstore struct {
	t *testing.T
	blockstore.Blockstore
	leafCount *int64
	rawLeaves useRawLeaves
	seenRoot  int
}

func newCountedBlockstore(
	t *testing.T,
	cnt *int64,
	rawLeaves useRawLeaves,
) *countedBlockstore {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	return &countedBlockstore{
		t:          t,
		Blockstore: blockstore.NewBlockstore(ds),
		leafCount:  cnt,
		rawLeaves:  rawLeaves,
	}
}

func (s *countedBlockstore) Put(ctx context.Context, b blocks.Block) error {
	en, ok := b.(*ExtendedNode)
	require.True(s.t, ok)
	if en.Chunk() != nil {
		*s.leafCount++
	}
	tag, ok := en.PutOptsTag().(useRawLeaves)
	require.True(s.t, ok)
	require.Equal(s.t, s.rawLeaves, tag)
	if en.Root() {
		s.seenRoot++
	}
	return s.Blockstore.Put(ctx, b)
}

func (s *countedBlockstore) hasSeenRoot() bool {
	return s.seenRoot == 1
}

func buildBalancedDag(
	t *testing.T,
	ctx context.Context,
	size int64,
	blksize int64,
	rawLeaves useRawLeaves,
) (ipld.Node, []byte, ipld.DAGService) {
	data := make([]byte, size)
	u.NewTimeSeededRand().Read(data)

	cbCount := int64(0)
	bstore := newCountedBlockstore(t, &cbCount, rawLeaves)
	ds := dag.NewDAGService(
		blockservice.New(
			bstore,
			offline.Exchange(bstore),
		),
	)
	db := newDagBuilder(
		ctx,
		NewDataProvider(bytes.NewReader(data), blksize),
		merkledag.V1CidPrefix(),
		ds,
		&PutOpts{
			RawLeaves: bool(rawLeaves),
			Tag:       rawLeaves,
		},
		0,
	)

	ed, err := genBalancedDag(db)
	require.NoError(t, err)
	require.True(t, bstore.hasSeenRoot())
	require.Equal(t, (size+blksize-1)/blksize, cbCount)

	return ed.Node, data, ds
}

//Test where calls to read are smaller than the chunk size
func TestSizeBasedSplit(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()
	testFileConsistency := func(
		t *testing.T,
		nbytes int64,
		blksize int64,
		rawLeaves useRawLeaves,
	) {
		nd, should, ds := buildBalancedDag(t, ctx, nbytes, blksize, rawLeaves)
		r, err := uio.NewDagReader(ctx, nd, ds)
		require.NoError(t, err)
		checkReaderContent(t, r, should)
	}

	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			testFileConsistency(t, 32*512, 512, rawLeaves)
			testFileConsistency(t, 32*4096, 4096, rawLeaves)
			// Uneven offset
			testFileConsistency(t, 31*4095, 4096, rawLeaves)
			testFileConsistency(t, 100000, chunker.DefaultBlockSize, rawLeaves)
		},
	)
}

func TestNoChunking(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildBalancedDag(t, ctx, 1000, 2000, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			checkReaderContent(t, r, should)
		},
	)
}

func TestTwoChunks(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildBalancedDag(t, ctx, 2000, 1000, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			checkReaderContent(t, r, should)
		},
	)
}

func TestIndirectBlocks(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildBalancedDag(t, ctx, 1024*1024, 512, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			checkReaderContent(t, r, should)
		},
	)
}

func TestSeekingBasic(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildBalancedDag(t, ctx, 10*1024, 500, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			start := int64(4000)
			seeked, err := r.Seek(start, io.SeekStart)
			require.NoError(t, err)
			require.Equal(t, start, seeked)
			checkReaderContent(t, r, should[start:])
		},
	)
}

func TestSeekToBegin(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildBalancedDag(t, ctx, 10*1024, 500, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			n, err := io.CopyN(ioutil.Discard, r, 1024*4)
			require.NoError(t, err)
			require.Equal(t, int64(4096), n)
			seeked, err := r.Seek(0, io.SeekStart)
			require.NoError(t, err)
			require.Equal(t, int64(0), seeked)
			checkReaderContent(t, r, should)
		},
	)
}

func TestSeekToAlmostBegin(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildBalancedDag(t, ctx, 10*1024, 500, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			n, err := io.CopyN(ioutil.Discard, r, 1024*4)
			require.NoError(t, err)
			require.Equal(t, int64(4096), n)
			seeked, err := r.Seek(1, io.SeekStart)
			require.NoError(t, err)
			require.Equal(t, int64(1), seeked)
			checkReaderContent(t, r, should[1:])
		},
	)
}

func TestSeekEnd(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(50 * 1024)
			nd, _, ds := buildBalancedDag(t, ctx, nbytes, 500, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			seeked, err := r.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.Equal(t, nbytes, seeked)
		},
	)
}

func TestSeekEndSingleBlockFile(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(100)
			nd, _, ds := buildBalancedDag(t, ctx, nbytes, 5000, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			seeked, err := r.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.Equal(t, nbytes, seeked)
		},
	)
}

func TestSeekingStress(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(1024 * 1024)
			nd, should, ds := buildBalancedDag(t, ctx, nbytes, 1000, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)

			testbuf := make([]byte, nbytes)
			for i := 0; i < 50; i++ {
				offset := mrand.Intn(int(nbytes))
				l := int(nbytes) - offset
				seeked, err := r.Seek(int64(offset), io.SeekStart)
				require.NoError(t, err)
				require.Equal(t, int64(offset), seeked)

				nread, err := r.Read(testbuf[:l])
				require.NoError(t, err)
				require.Equal(t, l, nread)

				require.DeepEqual(t, should[offset:offset+l], testbuf[:l])
			}
		},
	)
}

func TestSeekingConsistency(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(128 * 1024)
			nd, should, ds := buildBalancedDag(t, ctx, nbytes, 500, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)

			out := make([]byte, nbytes)
			for coff := nbytes - 4096; coff >= 0; coff -= 4096 {
				seeked, err := r.Seek(coff, io.SeekStart)
				require.NoError(t, err)
				require.Equal(t, coff, seeked)
				nread, err := r.Read(out[coff : coff+4096])
				require.NoError(t, err)
				require.Equal(t, 4096, nread)
			}

			require.DeepEqual(t, should, out)
		},
	)
}

func checkReaderContent(t *testing.T, r io.Reader, should []byte) {
	out, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.DeepEqual(t, should, out)
}
