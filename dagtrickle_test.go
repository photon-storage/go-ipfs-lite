package ipfs

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"testing"

	blockservice "github.com/ipfs/go-blockservice"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	u "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	dag "github.com/ipfs/go-merkledag"
	uio "github.com/ipfs/go-unixfs/io"

	"github.com/photon-storage/go-common/testing/require"
)

type useRawLeaves bool

const (
	protoBufLeaves useRawLeaves = false
	rawLeaves      useRawLeaves = true
)

func runBothSubtests(t *testing.T, tfunc func(*testing.T, useRawLeaves)) {
	t.Run("leaves=ProtoBuf", func(t *testing.T) { tfunc(t, protoBufLeaves) })
	t.Run("leaves=Raw", func(t *testing.T) { tfunc(t, rawLeaves) })
}

func buildTrickleDag(
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

	en, err := genTrickleDag(db)
	require.NoError(t, err)
	require.True(t, bstore.hasSeenRoot())
	require.Equal(t, (size+blksize-1)/blksize, cbCount)

	return en.Base(), data, ds
}

// Test where calls to read are smaller than the chunk size
func TestTrickleSizeBasedSplit(t *testing.T) {
	t.Skip()

	ctx := context.Background()
	testFileConsistency := func(
		t *testing.T,
		nbytes int64,
		blksize int64,
		rawLeaves useRawLeaves,
	) {
		nd, should, ds := buildTrickleDag(t, ctx, nbytes, blksize, rawLeaves)
		r, err := uio.NewDagReader(ctx, nd, ds)
		require.NoError(t, err)

		checkReaderContent(t, r, should)
	}

	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			testFileConsistency(t, 32*512, 512, rawLeaves)
			testFileConsistency(t, 32*4096, 512, rawLeaves)
			// Uneven offset
			testFileConsistency(t, 31*4095, 512, rawLeaves)
		},
	)
}

func TestTrickleBuilderConsistency(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildTrickleDag(t, ctx, 100000, 256*1024, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			checkReaderContent(t, r, should)
		},
	)
}

func TestTrickleIndirectBlocks(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildTrickleDag(t, ctx, 1024*1024, 512, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)
			checkReaderContent(t, r, should)
		},
	)
}

func TestTrickleSeekingBasic(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildTrickleDag(t, ctx, 10*1024, 512, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)

			start := int64(4000)
			n, err := r.Seek(start, io.SeekStart)
			require.NoError(t, err)
			require.Equal(t, start, n)
			checkReaderContent(t, r, should[start:])
		},
	)
}

func TestTrickleSeekToBegin(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildTrickleDag(t, ctx, 10*1024, 500, rawLeaves)
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

func TestTrickleSeekToAlmostBegin(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nd, should, ds := buildTrickleDag(t, ctx, 10*1024, 500, rawLeaves)
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

func TestTrickleSeekEnd(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(50 * 1024)
			nd, _, ds := buildTrickleDag(t, ctx, nbytes, 500, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)

			seeked, err := r.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.Equal(t, nbytes, seeked)
		},
	)
}

func TestTrickleSeekEndSingleBlockFile(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(100)
			nd, _, ds := buildTrickleDag(t, ctx, nbytes, 5000, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)

			seeked, err := r.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.Equal(t, nbytes, seeked)
		},
	)
}

func TestTrickleSeekingStress(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(1024 * 1024)
			nd, should, ds := buildTrickleDag(t, ctx, nbytes, 1000, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)

			testbuf := make([]byte, nbytes)
			for i := 0; i < 50; i++ {
				offset := mrand.Intn(int(nbytes))
				l := int(nbytes) - offset
				n, err := r.Seek(int64(offset), io.SeekStart)
				require.NoError(t, err)
				require.Equal(t, int64(offset), n)

				nread, err := r.Read(testbuf[:l])
				require.NoError(t, err)
				require.Equal(t, l, nread)
				require.DeepEqual(t, should[offset:offset+l], testbuf[:l])
			}
		},
	)
}

func TestTrickleSeekingConsistency(t *testing.T) {
	ctx := context.Background()
	runBothSubtests(
		t,
		func(t *testing.T, rawLeaves useRawLeaves) {
			nbytes := int64(128 * 1024)
			nd, should, ds := buildTrickleDag(t, ctx, nbytes, 500, rawLeaves)
			r, err := uio.NewDagReader(ctx, nd, ds)
			require.NoError(t, err)

			out := make([]byte, nbytes)
			for coff := nbytes - 4096; coff >= 0; coff -= 4096 {
				n, err := r.Seek(coff, io.SeekStart)
				require.NoError(t, err)
				require.Equal(t, coff, n)

				nread, err := r.Read(out[coff : coff+4096])
				require.NoError(t, err)
				require.Equal(t, 4096, nread)
			}

			require.DeepEqual(t, should, out)
		},
	)
}
