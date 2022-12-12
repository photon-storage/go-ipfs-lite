package ipfs

import (
	"io"

	chunker "github.com/ipfs/go-ipfs-chunker"
)

// Chunk defines a chunk returned by dataProvider. The chunk is passed back to
// blockstore after DAG building. The tag can be any data which is treated by
// the DAG builder. User can use tag to track chunk data.
type Chunk struct {
	Data []byte
	Tag  any
}

func NewChunk(d []byte, tag any) *Chunk {
	return &Chunk{
		Data: d,
		Tag:  tag,
	}
}

func (p *Chunk) Size() int {
	return len(p.Data)
}

// dataProvider defines the interface that supplies data in chunks for
// publishing data to IPFS network.
type dataProvider interface {
	Next() (*Chunk, error)
}

type defaultDataProvider struct {
	spl chunker.Splitter
}

// NewDataProvider creates a default dataProvider that splits data from
// the provided io.Reader in splitSize.
func NewDataProvider(r io.Reader, splitSize int64) dataProvider {
	return &defaultDataProvider{
		spl: chunker.NewSizeSplitter(r, splitSize),
	}
}

func (p *defaultDataProvider) Next() (*Chunk, error) {
	data, err := p.spl.NextBytes()
	if err != nil {
		return nil, err
	}

	return &Chunk{
		Data: data,
	}, nil
}
