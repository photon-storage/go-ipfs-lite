package ipfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	ipfslog "github.com/ipfs/go-log/v2"

	"github.com/photon-storage/go-common/log"
	"github.com/photon-storage/go-common/testing/require"

	ipfs "github.com/photon-storage/go-ipfs-lite"
)

func TestNodePutGetRoundtrip(t *testing.T) {
	// This test curls external URL thus can be unreliable.
	// Remove skip to run test.
	t.Skip()

	ipfslog.SetAllLoggers(ipfslog.LevelWarn)
	log.Init(log.DebugLevel, log.TextFormat, true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	nodeA, err := ipfs.New(ctx, ipfs.Config{
		MinConnections: 100,
		MaxConnections: 200,
	})
	require.NoError(t, err)
	nodeB, err := ipfs.New(ctx, ipfs.Config{
		MinConnections: 100,
		MaxConnections: 200,
	})
	require.NoError(t, err)

	// Wait 60 seconds for node to initialize.
	time.Sleep(60 * time.Second)

	fmt.Printf("************************* start ********************************\n")

	content := fmt.Sprintf("hello world test at unix nano %v",
		time.Now().UnixNano())

	// Put object.
	nd, err := nodeA.PutObject(
		ctx,
		bytes.NewReader([]byte(content)),
		ipfs.PutOpts{
			DagType:   ipfs.DagBalanced,
			RawLeaves: false,
		},
	)
	require.NoError(t, err)
	cid := nd.Cid()
	fmt.Printf("Object uploaded, cid = %v\n", cid)

	// Get object.
	r, err := nodeB.GetObject(ctx, cid)
	require.NoError(t, err)
	defer r.Close()

	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
	fmt.Printf("Local peer fetched object successfully, content = %v\n",
		string(data))

	// Get object through external source.
	tk := time.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			fmt.Printf("Current connection count, node_a = %v, node_b = %v\n",
				nodeA.NumConns(),
				nodeB.NumConns(),
			)

			data, err := ipfs.ExternFetchCid(cid)
			if err != nil {
				fmt.Printf("error fetching object from external source: %v\n", err)
				time.Sleep(10 * time.Second)
				break
			}

			if content != string(data) {
				fmt.Printf("object data from external source mismatch: %v\n", string(data))
				time.Sleep(10 * time.Second)
				break
			}
			return

		case <-ctx.Done():
			require.Fail(t, "Expected data not fetched before deadline")
		}
	}
}
