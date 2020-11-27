package rsutils

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func corruptShard(shard io.ReadWriteSeeker, shardSize int) error {
	byteIdxToCorrupt := rand.Intn(shardSize)
	_, err := shard.Seek(int64(byteIdxToCorrupt), io.SeekStart)
	if err != nil {
		return fmt.Errorf("Unable to Seek to %d", byteIdxToCorrupt)
	}
	_, err = shard.Write([]byte{0x00})
	if err != nil {
		return fmt.Errorf("Unable to write 0x00 to %d because: %s", byteIdxToCorrupt, err)
	}
	_, err = shard.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("Unable to Seek to 0")
	}
	return nil
}

func cloneFileTmp(t *testing.T, src *os.File) io.ReadWriteSeeker {
	dst, err := ioutil.TempFile("", "rsutil_clone")
	if err != nil {
		t.Errorf("Unable to create tmp clone file: %s", err)
	}
	t.Cleanup(func() {
		dst.Close()
		os.Remove(dst.Name())
	})
	_, err = io.Copy(dst, src)

	if err != nil {
		t.Errorf("Unable to clone tmp file: %s", err)
	}
	_, err = dst.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("Unable to seek to 0 in tmp file: %s", err)
	}
	return dst
}

func getShards(t *testing.T) []io.ReadWriteSeeker {
	shards := []*os.File{
		getTestFile(t, "input1"),
		getTestFile(t, "input2"),
		getTestFile(t, "parity1"),
	}
	clonedShards := make([]io.ReadWriteSeeker, len(shards))
	for i := range shards {
		clonedShards[i] = cloneFileTmp(t, shards[i])
	}

	return clonedShards
}

func TestShardManagerRead(t *testing.T) {
	shards := getShards(t)
	md := getMetadata()
	expectedSha256 := "86526dcd6bccd815ede7c9fb936c03ab2259233e73103dc30c29e9ce0d1fd53c"

	manager := NewShardManager(shards, md)

	var readBuffer bytes.Buffer
	hasher := sha256.New()
	err := manager.Read(io.MultiWriter(hasher, &readBuffer))
	if err != nil {
		t.Errorf("Unexpected error while reading: %s", err)
	}

	sha256checkSum := fmt.Sprintf("%x", hasher.Sum(nil))
	if sha256checkSum != expectedSha256 {
		t.Errorf("Incorrect checksum on reading: got %s, expected %s\n\nGot text:\nMARKER\n%s\nMARKER", sha256checkSum, expectedSha256, readBuffer.String())
	}
}

func TestShardManagerCheckHealth(t *testing.T) {
	md := getMetadata()
	corruptedTests := []struct {
		name            string
		shardsToCorrupt []int
		expectedErrMsg  string
	}{
		{"0 corrupt", []int{}, ""},
		{"1 corrupt", []int{0}, "Corrupted shards: [0]"},
		{"2 corrupt", []int{0, 1}, "Corrupted shards: [0 1]"},
		{"3 corrupt", []int{0, 1, 2}, "Corrupted shards: [0 1 2]"},
	}

	for _, tt := range corruptedTests {
		t.Run(tt.name, func(t *testing.T) {
			shards := getShards(t)
			manager := NewShardManager(shards, md)
			for _, corruptIdx := range tt.shardsToCorrupt {
				err := corruptShard(shards[corruptIdx], int(md.Size)/md.DataShards)
				if err != nil {
					t.Errorf("Cannot corrupt shard %d: %s", corruptIdx, err)
				}
			}
			err := manager.CheckHealth()

			if len(tt.shardsToCorrupt) == 0 && err != nil {
				t.Errorf("Got %s, expected err to be nil", err)
			}

			if len(tt.shardsToCorrupt) > 0 && err.Error() != tt.expectedErrMsg {
				t.Errorf("Got '%s', expected '%s'", err, tt.expectedErrMsg)
			}
		})
	}
}

func TestShardManagerCheckHealthRewindsDataSources(t *testing.T) {
	shards := getShards(t)
	md := getMetadata()

	manager := NewShardManager(shards, md)

	err := manager.CheckHealth()
	if err != nil {
		t.Errorf("Got '%s', expected nil error", err)
	}

	var readBuf bytes.Buffer
	manager.Read(&readBuf)
	if readBuf.Len() != int(md.Size) {
		t.Errorf("Got %d, expected %d", readBuf.Len(), md.Size)
	}
}

func TestShardManagerRepairBadDataShard(t *testing.T) {
	md := getMetadata()
	badShardTests := []struct {
		name        string
		badShardIdx int
	}{
		{"bad data shard", 1},
		{"bad parity shard", 2},
	}

	for _, tt := range badShardTests {
		t.Run(tt.name, func(t *testing.T) {
			shards := getShards(t)

			err := corruptShard(shards[tt.badShardIdx], int(md.Size)/md.DataShards)
			if err != nil {
				t.Errorf("Unable to corrupt shard %d: %s", tt.badShardIdx, err)
			}

			manager := NewShardManager(shards, md)
			err = manager.Repair()
			if err != nil {
				t.Errorf("Got '%s', expected nil error", err)
			}

			for _, shard := range shards {
				_, err = shard.Seek(0, io.SeekStart)
				if err != nil {
					t.Errorf("Unable to rewind shard: %s", err)
				}
			}

			newManager := NewShardManager(shards, md)
			err = newManager.CheckHealth()
			if err != nil {
				// check shard hash, read, comare text
				t.Errorf("Got '%s', expected nil error", err)
			}
		})
	}
}

func TestShardManagerRepairNotEnoughShards(t *testing.T) {
	shards := getShards(t)
	md := getMetadata()
	expectedErrMsg := "Cannot repair data: 2 shards corrupt, only have 1 parity shards"

	for _, i := range []int{1, 2} {
		err := corruptShard(shards[i], int(md.Size)/md.DataShards)
		if err != nil {
			t.Errorf("Unable to corrupt shard %d: %s", i, err)
		}
	}
	manager := NewShardManager(shards, md)
	err := manager.Repair()

	if err.Error() != expectedErrMsg {
		t.Errorf("Got error '%s', expected '%s'", err, expectedErrMsg)
	}
}

func TestE2EUnevenInput(t *testing.T) {
	dataShards := 2
	parityShards := 1
	parityFile, err := ioutil.TempFile("", "rsutils")
	if err != nil {
		t.Fatal(err)
	}
	defer parityFile.Close()
	t.Cleanup(func() {
		os.Remove(parityFile.Name())
	})
	unevenFile, err := os.Open("testdata/uneven_input1")
	if err != nil {
		t.Fatal(err)
	}
	defer unevenFile.Close()
	unevenFileStat, err := unevenFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	paddedFileChunks := SplitIntoPaddedChunks(unevenFile, unevenFileStat.Size(), dataShards)
	chunkReaders := make([]io.Reader, len(paddedFileChunks))
	for i := range paddedFileChunks {
		chunkReaders[i] = paddedFileChunks[i]
	}
	shardCreator := NewShardCreator(chunkReaders, unevenFileStat.Size(), dataShards, parityShards)
	md, err := shardCreator.Encode([]io.Writer{parityFile})
	if err != nil {
		t.Fatal(err)
	}

	for _, chunk := range paddedFileChunks {
		_, err = chunk.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = parityFile.Seek(0, io.SeekStart)

	shards := make([]io.ReadWriteSeeker, dataShards+parityShards)
	shards[0] = paddedFileChunks[0]
	shards[1] = paddedFileChunks[1]
	shards[2] = parityFile

	shardManager := NewShardManager(shards, md)

	err = shardManager.CheckHealth()
	if err != nil {
		t.Errorf("Got health error %s, expected nil", err)
	}
}
