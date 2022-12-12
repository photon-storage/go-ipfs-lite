package ipfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/photon-storage/go-common/log"
	"github.com/photon-storage/go-common/testing/require"

	"github.com/photon-storage/go-photon/depot/ipfs"
	"github.com/photon-storage/go-photon/testing/wait"
)

func TestNodePutGetRoundtrip(t *testing.T) {
	// This test curls external URL thus can be unreliable.
	// Remove skip to run test.
	t.Skip()

	log.Init(log.DebugLevel, log.TextFormat, true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	nodeA, err := ipfs.New(ctx, ipfs.Config{})
	require.NoError(t, err)
	nodeB, err := ipfs.New(ctx, ipfs.Config{})
	require.NoError(t, err)

	// Wait 60 seconds for node to initialize.
	time.Sleep(60 * time.Second)

	fmt.Printf("************************* start ********************************\n")

	content := fmt.Sprintf("hello world test at unix nano %v", time.Now().UnixNano())

	// Put object.
	nd, err := nodeA.PutObject(
		ctx,
		bytes.NewReader([]byte(content)),
		&ipfs.PutOpts{
			DagType:   ipfs.DagBalanced,
			RawLeaves: false,
		},
	)
	require.NoError(t, err)
	cid := nd.Cid()
	fmt.Printf("object uploaded, cid = %v\n", cid)

	// Get object.
	r, err := nodeB.GetObject(ctx, cid)
	require.NoError(t, err)
	defer r.Close()

	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
	fmt.Printf("local peer fetched object successfully, content = %v\n", string(data))

	// Get object through external source.
	wait.ForCond(
		t,
		ctx,
		func() bool {
			data, err := ipfs.ExternFetchCid(cid)
			if err != nil {
				fmt.Printf("error fetching object from external source: %v\n", err)
				time.Sleep(10 * time.Second)
				return false
			}

			if content != string(data) {
				fmt.Printf("object data from external source mismatch: %v\n", string(data))
				time.Sleep(10 * time.Second)
				return false
			}

			return true
		},
	)
}
