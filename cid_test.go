package ipfs_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-merkledag"
	"github.com/photon-storage/go-common/testing/require"

	ipfs "github.com/photon-storage/go-ipfs-lite"
)

func TestCidTest(t *testing.T) {
	dagTypes := []ipfs.DagType{
		ipfs.DagBalanced,
		ipfs.DagTrickle,
	}
	chunkSizes := []int64{
		1023,
		1024,
		1025,
		65535,
		65536,
		65537,
		262143,
		262144,
		262145,
	}

	data := make([]byte, 4<<20+123)
	_, err := rand.Reader.Read(data)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for _, dt := range dagTypes {
		for _, cs := range chunkSizes {
			for _, lpb := range []int{2, 7, 16} {
				for _, rl := range []bool{true, false} {
					cid0, err := ipfs.GenCid(
						ctx,
						data,
						ipfs.CidOpts{
							DagType:       dt,
							ChunkSize:     cs,
							LinksPerBlock: lpb,
							RawLeaves:     rl,
							CidBuilder:    merkledag.V1CidPrefix(),
						},
					)
					require.NoError(t, err)

					cid1, err := ipfs.GenCid(
						ctx,
						bytes.NewReader(data),
						ipfs.CidOpts{
							DagType:       dt,
							ChunkSize:     cs,
							LinksPerBlock: lpb,
							RawLeaves:     rl,
							CidBuilder:    merkledag.V1CidPrefix(),
						},
					)
					require.NoError(t, err)

					cid2, err := ipfs.GenCid(
						ctx,
						ipfs.NewDataProvider(
							bytes.NewReader(data),
							cs,
						),
						ipfs.CidOpts{
							DagType:       dt,
							ChunkSize:     cs,
							LinksPerBlock: lpb,
							RawLeaves:     rl,
							CidBuilder:    merkledag.V1CidPrefix(),
						},
					)
					require.NoError(t, err)

					require.Equal(t, cid0.String(), cid1.String())
					require.Equal(t, cid1.String(), cid2.String())
				}
			}
		}
	}
}

func TestCidConsistencyWithPut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	node, err := ipfs.New(ctx, ipfs.Config{
		OfflineMode: true,
	})
	require.NoError(t, err)

	// Wait 60 seconds for node to initialize.
	// time.Sleep(60 * time.Second)

	content := fmt.Sprintf("hello world test at unix nano %v", time.Now().UnixNano())

	// Put object.
	nd, err := node.PutObject(
		ctx,
		bytes.NewReader([]byte(content)),
		ipfs.PutOpts{
			DagType:   ipfs.DagBalanced,
			RawLeaves: false,
		},
	)
	require.NoError(t, err)

	cid, err := ipfs.GenCid(
		ctx,
		bytes.NewReader([]byte(content)),
		ipfs.CidOpts{
			DagType:   ipfs.DagBalanced,
			RawLeaves: false,
		},
	)
	require.NoError(t, err)
	require.Equal(t, nd.Cid().String(), cid.String())
}
