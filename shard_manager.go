package rsutils

import (
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
)

type ShardManager struct {
	dataSources []io.ReadWriteSeeker
	metadata    *Metadata
}

func NewShardManager(src []io.ReadWriteSeeker, meta *Metadata) *ShardManager {
	return &ShardManager{
		dataSources: src,
		metadata:    meta,
	}
}

func (p *ShardManager) findCorruptShards() ([]int, error) {
	brokenShards := make([]int, 0)
	for i := 0; i < len(p.metadata.Hashes); i++ {
		hasher := sha256.New()
		defer p.dataSources[i].Seek(0, 0)
		_, err := io.Copy(hasher, p.dataSources[i])
		if err != nil {
			return nil, fmt.Errorf("Error hashing shard %d: %s", i, err)
		}
		if fmt.Sprintf("%x", hasher.Sum(nil)) != p.metadata.Hashes[i] {
			brokenShards = append(brokenShards, i)
		}
	}
	return brokenShards, nil
}

func (p *ShardManager) Read(dataDst io.Writer) error {
	for _, dataSource := range p.dataSources {
		_, err := io.Copy(dataDst, dataSource)
		if err != nil {
			return fmt.Errorf("Error while reading: %s", err)
		}
	}
	return nil
}

func (p *ShardManager) CheckHealth() error {
	brokenShardIndexes, err := p.findCorruptShards()
	if err != nil {
		return fmt.Errorf("Error while checking shard integrity: %s", err)
	}
	if len(brokenShardIndexes) > 0 {
		return fmt.Errorf("Corrupted shards: %s", fmt.Sprintf("%v", brokenShardIndexes))
	}
	return nil
}

func (p *ShardManager) Repair() error {
	brokenShardIndexes, err := p.findCorruptShards()
	if err != nil {
		return fmt.Errorf("Error while checking shard integrity: %s", err)
	}
	if len(brokenShardIndexes) == 0 {
		return nil
	}

	if bsCount := len(brokenShardIndexes); bsCount > p.metadata.ParityShards {
		return fmt.Errorf("Cannot repair data: %d shards corrupt, only have %d parity shards", bsCount, p.metadata.ParityShards)
	}

	shardCount := p.metadata.DataShards + p.metadata.ParityShards
	shardReaders := make([]io.Reader, shardCount)
	shardWriters := make([]io.Writer, shardCount)

	for i := range p.dataSources {
		shardReaders[i] = p.dataSources[i].(io.Reader)
	}

	// mark shards as broken, mark which shards to write
	for _, shardIndex := range brokenShardIndexes {
		shardReaders[shardIndex] = nil
		shardWriters[shardIndex] = p.dataSources[shardIndex].(io.Writer)
	}

	RSEncoder, err := reedsolomon.NewStream(p.metadata.DataShards, p.metadata.ParityShards)
	if err != nil {
		return fmt.Errorf("Error creating reedsolomon encoder: %s", err)
	}

	err = RSEncoder.Reconstruct(shardReaders, shardWriters)
	if err != nil {
		return fmt.Errorf("Error reconstructing data: %s", err)
	}
	return nil
}
