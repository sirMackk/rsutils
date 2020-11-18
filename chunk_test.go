package rsutils

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestSplitIntoPaddedChunkReader(t *testing.T) {
	testReaders := []struct {
		input          []byte
		size           int64
		numChunks      int
		expectedOutput [][]byte
	}{
		{[]byte("ABCDEFGH"), 8, 2, [][]byte{[]byte("ABCD"), []byte("EFGH")}},
		{[]byte("ABCDEFGHI"), 9, 2, [][]byte{[]byte("ABCDE"), []byte{0x46, 0x47, 0x48, 0x49, 0}}},
		{[]byte("ABCDEFGH"), 8, 3, [][]byte{[]byte("ABC"), []byte("DEF"), []byte{0x47, 0x48, 0}}},
	}

	for _, tt := range testReaders {
		t.Run(string(tt.input), func(t *testing.T) {
			readers := SplitIntoPaddedChunks(bytes.NewReader(tt.input), tt.size, tt.numChunks)
			for i, reader := range readers {
				b, err := ioutil.ReadAll(reader)
				if err != nil {
					t.Errorf("Error while testing %#v: %s", tt.input, err)
				}
				if !bytes.Equal(b, tt.expectedOutput[i]) {
					fmt.Printf("%d - %d - %d\n", reader.offset, reader.limit, reader.bytesRead)
					t.Errorf("Got %#v, expected %#v when testing %#v (%d)", b, tt.expectedOutput[i], tt.input, i)
				}
			}
		})
	}
}

func TestSplitIntoChunkWriters(t *testing.T) {
	testWriters := []struct {
		inputs    [][]byte
		output    []byte
		size      int64
		numChunks int
	}{
		{[][]byte{[]byte("AB"), []byte("CD")}, []byte("ABCD"), 4, 2},
		{[][]byte{[]byte("ABCDE"), []byte("FGHI")}, []byte("ABCDEFGHI"), 9, 2},
		{[][]byte{[]byte("ABC"), []byte("DEF"), []byte("GH")}, []byte("ABCDEFGH"), 8, 3},
	}

	for _, tt := range testWriters {
		t.Run(string(tt.output), func(t *testing.T) {
			// TODO replace for in-memory buffer w/ WriteAt
			tmpFile, err := ioutil.TempFile("", "rsbackup_test")
			if err != nil {
				t.Errorf("Error while creating tmp file: %s", err)
			}
			writers := SplitIntoChunkWriters(tmpFile, tt.size, tt.numChunks)
			for i, writer := range writers {
				_, err := writer.Write(tt.inputs[i])
				if err != nil {
					t.Errorf("Writing to tmp file failed: %s", err)
				}
			}
			_, err = tmpFile.Seek(0, 0)
			if err != nil {
				t.Errorf("Error while rewinding tmp file: %s", err)
			}

			b, _ := ioutil.ReadAll(tmpFile)
			if !bytes.Equal(b, tt.output) {
				t.Errorf("Expected %v, got %v", tt.output, b)
			}
		})
	}
}

func TestChunkWriterLimit(t *testing.T) {
	testWriters := []struct {
		name         string
		offset       int64
		limit        int64
		bytesWritten int64
	}{
		{"empty", 0, 4, 0},
		{"half-written", 0, 4, 2},
		{"empty-2nd", 4, 8, 0},
		{"half-written-2nd", 4, 8, 2},
	}

	for _, tt := range testWriters {
		t.Run(tt.name, func(t *testing.T) {
			cw := &ChunkWriter{
				dest:         nil,
				offset:       tt.offset,
				limit:        tt.limit,
				bytesWritten: tt.bytesWritten,
			}
			n, err := cw.Write([]byte("TOOBIG"))
			if err == nil {
				t.Errorf("Given %d offset, %d limit, and %d bytesWritten, Write should fail", tt.offset, tt.limit, tt.bytesWritten)
			}
			if n != 0 {
				t.Errorf("Given a failed write, n should be 0, got %d", n)
			}
		})
	}
}
