// Package rsutils provides a hgih level API for https://github.com/klauspost/reedsolomon
package rsutils

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"github.com/klauspost/reedsolomon"
)

// Encode reads an *os.File f, divides it into dataShards shards, and outputs parity shard data to parityWriters.
// It returns a Metadata object that contains information useful in reading or reconstructing the data again.
func Encode(f *os.File, dataShards int, parityWriters []io.Writer) (*Metadata, error) {
	parityShards := len(parityWriters)

	fstat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fsize := fstat.Size()
	paddedChunks := SplitIntoPaddedChunks(f, fsize, dataShards)

	hashers := make([]hash.Hash, dataShards+parityShards)
	for i := range hashers {
		hashers[i] = sha256.New()
	}
	hashingReaders := make([]io.Reader, dataShards)
	for i := range paddedChunks {
		hashingReaders[i] = io.TeeReader(paddedChunks[i], hashers[i])
	}
	hashingWriters := make([]io.Writer, parityShards)
	for i := range hashingWriters {
		hashingWriters[i] = io.MultiWriter(parityWriters[i], hashers[dataShards+i])
	}

	encoder, err := reedsolomon.NewStream(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	err = encoder.Encode(hashingReaders, hashingWriters)
	if err != nil {
		return nil, err
	}

	hashes := make([]string, dataShards+parityShards)
	for i := range hashers {
		hashes[i] = fmt.Sprintf("%x", hashers[i].Sum(nil))
	}

	return &Metadata{
		Size:         fsize,
		Hashes:       hashes,
		DataShards:   dataShards,
		ParityShards: parityShards,
	}, nil
}

type FileDecoder struct {
	data         *os.File
	parityFiles  []*os.File
	md           *Metadata
	dataMTime    time.Time
	parityMTimes []time.Time
}

// Open accepts a data file, some parityFiles, and a Metadata object. It returns a
// FileDecoder object which can be used to Read the data back.
func Open(data *os.File, parityFiles []*os.File, md *Metadata) (*FileDecoder, error) {
	if len(parityFiles) != md.ParityShards {
		return nil, fmt.Errorf("Cannot open encoded files: need %d parity shards, got %d", md.ParityShards, len(parityFiles))
	}
	return &FileDecoder{
		data:         data,
		parityFiles:  parityFiles,
		md:           md,
		dataMTime:    time.Time{},
		parityMTimes: make([]time.Time, md.ParityShards),
	}, nil
}

type CorruptShard struct {
	index int
	hash  string
}

func (f *FileDecoder) checkDataShardHealth() ([]*CorruptShard, error) {
	var _stat os.FileInfo
	corruptShards := make([]*CorruptShard, 0)

	_stat, err := f.data.Stat()
	if err != nil {
		return nil, err
	}
	dataMTime := _stat.ModTime()
	var checkDataShards bool = dataMTime != f.dataMTime
	f.dataMTime = dataMTime
	if checkDataShards {
		for i, chunk := range SplitIntoPaddedChunks(f.data, f.md.Size, f.md.DataShards) {
			hasher := sha256.New()
			_, err := io.Copy(hasher, chunk)
			if err != nil {
				return nil, err
			}
			dShardHash := fmt.Sprintf("%x", hasher.Sum(nil))
			desiredDShardHash := f.md.Hashes[i]

			if dShardHash != desiredDShardHash {
				corruptShards = append(corruptShards, &CorruptShard{index: i, hash: dShardHash})
			}
		}
	}
	return corruptShards, nil
}

func (f *FileDecoder) checkParityShardsHealth() ([]*CorruptShard, error) {
	corruptShards := make([]*CorruptShard, 0)
	modifiedParityShards := make(map[int]time.Time, 0)

	for i := range f.parityFiles {
		_stat, err := f.parityFiles[i].Stat()
		if err != nil {
			return nil, err
		}

		if parityMTime := _stat.ModTime(); parityMTime != f.parityMTimes[i] {
			modifiedParityShards[i] = parityMTime
			f.parityMTimes[i] = parityMTime
		}
	}
	if len(modifiedParityShards) != 0 {
		for i := range f.parityFiles {
			hasher := sha256.New()
			_, err := io.Copy(hasher, f.parityFiles[i])
			defer f.parityFiles[i].Seek(0, os.SEEK_SET)
			if err != nil {
				return nil, err
			}
			parityShardIdx := f.md.DataShards + i
			pShardHash := fmt.Sprintf("%x", hasher.Sum(nil))
			desiredPShardHash := f.md.Hashes[parityShardIdx]

			if pShardHash != desiredPShardHash {
				corruptShards = append(corruptShards, &CorruptShard{index: parityShardIdx, hash: pShardHash})
			}
		}
	}
	return corruptShards, nil
}

func (f *FileDecoder) checkShardHealth() ([]*CorruptShard, error) {
	corruptDataShards, err := f.checkDataShardHealth()
	if err != nil {
		return nil, err
	}
	corruptParityShards, err := f.checkParityShardsHealth()
	if err != nil {
		return nil, err
	}

	return append(corruptDataShards, corruptParityShards...), nil
}

func (f *FileDecoder) attemptRepair(corruptShards []*CorruptShard) error {
	if len(corruptShards) > len(f.parityFiles) {
		return fmt.Errorf("Cannot repair data: %d shards corrupt, only have %d parity shards", len(corruptShards), len(f.parityFiles))
	}
	paddedChunks := SplitIntoPaddedChunks(f.data, f.md.Size, f.md.DataShards)

	shardCount := len(paddedChunks) + len(f.parityFiles)
	shardReaders := make([]io.Reader, shardCount)
	shardWriters := make([]io.Writer, shardCount)

	for i := range paddedChunks {
		shardReaders[i] = paddedChunks[i]
	}
	for i := range f.parityFiles {
		shardReaders[f.md.DataShards+i] = f.parityFiles[i]
	}

	var corruptIdx int
	for _, corruptShard := range corruptShards {
		corruptIdx = corruptShard.index
		shardReaders[corruptIdx] = nil

		if corruptIdx < f.md.DataShards {
			shardWriters[corruptIdx] = paddedChunks[corruptIdx]
		} else {
			shardWriters[corruptIdx] = f.parityFiles[corruptIdx-f.md.DataShards]
		}
	}

	encoder, err := reedsolomon.NewStream(f.md.DataShards, f.md.ParityShards)
	if err != nil {
		return err
	}

	err = encoder.Reconstruct(shardReaders, shardWriters)
	if err != nil {
		return err
	}
	return nil
}

// Read attempts to read the Reed-Solomon-encoded data into []byte p.
// It will check the integrity of the data first and use file modified time to
// keep track whether it needs to check the integrity again in the case that the file
// changed between calling Open and FileEncoder.Read.
// If data or parity shards are corrupted, calling Read will trigger an attempt to 
// repair the data. This will make the Read call take longer than when the data is 
// not corrupted. It may fail if the corruption is too extensive.
// It returns the number of bytes read or an error.
func (f *FileDecoder) Read(p []byte) (int, error) {
	corruptShards, err := f.checkShardHealth()
	if err != nil {
		return 0, err
	}
	if len(corruptShards) != 0 {
		err := f.attemptRepair(corruptShards)
		if err != nil {
			return 0, err
		}
	}

	return f.data.Read(p)
}
