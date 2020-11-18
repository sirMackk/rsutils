package rsutils

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/klauspost/reedsolomon"
)

type ShardCreator struct {
	dataSources  []io.Reader
	size         int64
	dataShards   int
	parityShards int
}

func NewShardCreator(src []io.Reader, size int64, dataShards, parityShards int) *ShardCreator {
	return &ShardCreator{
		dataSources:  src,
		size:         size,
		dataShards:   dataShards,
		parityShards: parityShards,
	}
}

func (p *ShardCreator) Encode(parityDst []io.Writer) (*Metadata, error) {
	RSEncoder, err := reedsolomon.NewStream(p.dataShards, p.parityShards)
	if err != nil {
		return nil, fmt.Errorf("Error creating reedsolomon encoder: %s", err)
	}

	hashers := make([]hash.Hash, p.dataShards+p.parityShards)
	for i := range hashers {
		hashers[i] = sha256.New()
	}
	hashingReaders := make([]io.Reader, p.dataShards)
	for i := range hashingReaders {
		hashingReaders[i] = io.TeeReader(p.dataSources[i], hashers[i])
	}
	hashingWriters := make([]io.Writer, p.parityShards)
	for i := range hashingWriters {
		hashingWriters[i] = io.MultiWriter(parityDst[i], hashers[p.dataShards+i])
	}

	err = RSEncoder.Encode(hashingReaders, hashingWriters)
	if err != nil {
		return nil, fmt.Errorf("Error encoding: %s", err)
	}

	hashes := make([]string, len(hashers))
	for i := range hashers {
		hashes[i] = fmt.Sprintf("%x", hashers[i].Sum(nil))
	}
	return &Metadata{
		Size:         p.size,
		Hashes:       hashes,
		DataShards:   p.dataShards,
		ParityShards: p.parityShards,
	}, nil
}
