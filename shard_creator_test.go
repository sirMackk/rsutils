package rsutils

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func getTestFile(t *testing.T, name string) *os.File {
	path := fmt.Sprintf("testdata/%s", name)
	f, err := os.Open(path)
	if err != nil {
		t.Errorf("Error opening %s: %s", path, err)
	}
	t.Cleanup(func() {
		f.Close()
	})
	return f
}

func getMetadata() *Metadata {
	return &Metadata{
		Size: int64(808),
		Hashes: []string{
			"aa8b8979f1486fe03d54d1bdd4a32018386285a2ad0dc9a2820f0da3d6293e72",
			"64163fa75b3eadb78f376dd7ab84e48595e9748dadbfb50e2126bef20481baa1",
			"e32a8903342ab6dc68d46462df727f6812f6fbb728c4a1240b625331b811c147",
		},
		DataShards:   2,
		ParityShards: 1,
	}
}

func TestShareCreatorEncode(t *testing.T) {
	input1 := getTestFile(t, "input1")
	input2 := getTestFile(t, "input2")

	inputStat, err := input1.Stat()
	if err != nil {
		t.Errorf("Error while stating %s: %s", input1.Name(), err)
	}
	inputSize := inputStat.Size()
	fixtureMd := getMetadata()
	dataShards := fixtureMd.DataShards
	parityShards := fixtureMd.ParityShards
	expectedHashes := fixtureMd.Hashes

	creator := NewShardCreator([]io.Reader{input1, input2}, inputSize*2, dataShards, parityShards)

	var parityBuffer bytes.Buffer
	md, err := creator.Encode([]io.Writer{&parityBuffer})
	if err != nil {
		t.Errorf("Error while encoding test input: %s", err)
	}
	if md.Size != inputSize*2 {
		t.Errorf("Incorrect metadata Size: got %d, expected %d", md.Size, inputSize*2)
	}
	if md.DataShards != dataShards || md.ParityShards != parityShards {
		t.Errorf("Incorrect metadata ds/ps: got %d/%d, expected %d/%d", md.DataShards, md.ParityShards, dataShards, parityShards)
	}
	if lenHashes := len(md.Hashes); lenHashes != dataShards+parityShards {
		t.Errorf("Incorrect number of hashes: got %d, expected %d", lenHashes, dataShards+parityShards)
	}

	for i := range md.Hashes {
		if md.Hashes[i] != expectedHashes[i] {
			t.Errorf("Incorrect metadata hash: got '%s', expected %s", md.Hashes[i], expectedHashes[i])
		}
	}
}

func TestEncodeFails(t *testing.T) {
	input1 := getTestFile(t, "input1")
	// inputs are not equal, expect Encode to fail
	input2 := bytes.NewReader([]byte("ABCD"))
	input1Stat, err := input1.Stat()
	if err != nil {
		t.Errorf("Error while stating %s: %s", input1.Name(), err)
	}
	inputSize := input1Stat.Size()

	creator := NewShardCreator([]io.Reader{input1, input2}, inputSize*2, 2, 1)

	var parityBuffer bytes.Buffer
	_, err = creator.Encode([]io.Writer{&parityBuffer})
	if err == nil || err.Error() != "Error encoding: shard sizes do not match" {
		t.Errorf("%s", err)
	}
}
