package ipfs

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"
)

// ExternFetchCid fetches IPFS block data for the given CID from an external
// source. If the fetch is successful, that means data becomes visible by
// IPFS peers.
func ExternFetchCid(cid cid.Cid) ([]byte, error) {
	resp, err := http.Get(fmt.Sprintf("https://api.ipfsbrowser.com/ipfs/get.php?hash=%v", cid))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-type")
	if strings.Contains(contentType, "text/plain") ||
		strings.Contains(contentType, "application/octet-stream") {
		return ioutil.ReadAll(resp.Body)
	}

	return nil, fmt.Errorf("content type is unknown: %v", resp.Header.Get("Content-type"))
}
