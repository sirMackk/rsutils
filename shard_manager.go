package rsutils

import (
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
)

type ShardManager struct {
	DataSources []io.ReadWriteSeeker
	Metadata    *Metadata
}

func NewShardManager(src []io.ReadWriteSeeker, meta *Metadata) *ShardManager {
	return &ShardManager{
		DataSources: src,
		Metadata:    meta,
	}
}

func (p *ShardManager) findCorruptShards() ([]int, error) {
	brokenShards := make([]int, 0)
	for i := 0; i < len(p.Metadata.Hashes); i++ {
		hasher := sha256.New()
		defer p.DataSources[i].Seek(0, 0)
		_, err := io.Copy(hasher, p.DataSources[i])
		if err != nil {
			return nil, fmt.Errorf("Error hashing shard %d: %s", i, err)
		}
		if newHash := fmt.Sprintf("%x", hasher.Sum(nil)); newHash != p.Metadata.Hashes[i] {
			brokenShards = append(brokenShards, i)
		}
	}
	return brokenShards, nil
}

func (p *ShardManager) Read(dataDst io.Writer) error {
	for _, dataSource := range p.DataSources[:p.Metadata.DataShards] {
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

	if bsCount := len(brokenShardIndexes); bsCount > p.Metadata.ParityShards {
		return fmt.Errorf("Cannot repair data: %d shards corrupt, only have %d parity shards", bsCount, p.Metadata.ParityShards)
	}

	shardCount := p.Metadata.DataShards + p.Metadata.ParityShards
	shardReaders := make([]io.Reader, shardCount)
	shardWriters := make([]io.Writer, shardCount)

	for i := range p.DataSources {
		shardReaders[i] = p.DataSources[i].(io.Reader)
	}

	// mark shards as broken, mark which shards to write
	for _, shardIndex := range brokenShardIndexes {
		shardReaders[shardIndex] = nil
		shardWriters[shardIndex] = p.DataSources[shardIndex].(io.Writer)
	}

	RSEncoder, err := reedsolomon.NewStream(p.Metadata.DataShards, p.Metadata.ParityShards)
	if err != nil {
		return fmt.Errorf("Error creating reedsolomon encoder: %s", err)
	}

	err = RSEncoder.Reconstruct(shardReaders, shardWriters)
	if err != nil {
		return fmt.Errorf("Error reconstructing data: %s", err)
	}
	return nil
}
