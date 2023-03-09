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

	log.Warn("******************** start ***************************")

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
	log.Info("Object uploaded", "cid", cid)

	// Get object.
	r, err := nodeB.GetObject(ctx, cid)
	require.NoError(t, err)
	defer r.Close()

	data, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
	log.Info("Local peer fetched object successfully",
		"content", string(data))

	// Get object through external source.
	tk := time.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			data, err := ipfs.ExternFetchCid(cid)
			if err != nil {
				log.Error("Error fetching object from external source",
					"error", err)
				time.Sleep(10 * time.Second)
				break
			}

			if content != string(data) {
				log.Error("Object data from external source mismatch",
					"data", string(data))
				time.Sleep(10 * time.Second)
				break
			}
			return

		case <-ctx.Done():
			require.Fail(t, "Expected data not fetched before deadline")
		}
	}
}

func TestNodeContinuousPut(t *testing.T) {
	// Remove skip to run test.
	t.Skip()

	ipfslog.SetAllLoggers(ipfslog.LevelError)
	log.Init(log.DebugLevel, log.TextFormat, true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	nodeA, err := ipfs.New(ctx, ipfs.Config{
		MinConnections:    100,
		MaxConnections:    800,
		ConnectionLogging: true,
	})
	require.NoError(t, err)

	log.Warn("******************** start ***************************")

	go func() {
		putTicker := time.NewTicker(60 * time.Second)
		for {
			select {
			case <-putTicker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	tk := time.NewTicker(10 * time.Second)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
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
			log.Info("Object uploaded", "cid", nd.Cid())

		case <-ctx.Done():
			require.Fail(t, "Expected data not fetched before deadline")
		}
	}
}
