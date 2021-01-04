package rsutils

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func CreateTMPFile(t *testing.T, input []byte) *os.File {
	tmpFile, err := ioutil.TempFile("", "rsutils_test")
	if err != nil {
		t.Errorf("Error while creating tmp file: %s", err)
	}
	t.Cleanup(func() {
		os.Remove(tmpFile.Name())
	})
	_, err = tmpFile.Write(input)
	if err != nil {
		t.Errorf("Error while writing test data to tmp file: %s", err)
	}
	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		t.Errorf("Error while rewinding tmp file: %s", err)
	}
	return tmpFile
}

func TestPaddedFileChunkReading(t *testing.T) {
	testReaders := []struct {
		input          []byte
		size           int64
		numChunks      int
		expectedOutput [][]byte
	}{
		{[]byte("ABCDEFGH"), 8, 2, [][]byte{[]byte("ABCD"), []byte("EFGH")}},
		{[]byte("ABCDEFGHI"), 9, 2, [][]byte{[]byte("ABCDE"), {0x46, 0x47, 0x48, 0x49, 0}}},
		{[]byte("ABCDEFGH"), 8, 3, [][]byte{[]byte("ABC"), []byte("DEF"), {0x47, 0x48, 0}}},
	}

	for _, tt := range testReaders {
		t.Run(string(tt.input), func(t *testing.T) {
			tmpFile := CreateTMPFile(t, tt.input)
			readers := SplitIntoPaddedChunks(tmpFile, tt.size, tt.numChunks)
			for i, reader := range readers {
				b, err := ioutil.ReadAll(reader)
				if err != nil {
					t.Errorf("Error while testing %#v: %s", tt.input, err)
				}
				if !bytes.Equal(b, tt.expectedOutput[i]) {
					fmt.Printf("%d - %d - %d\n", reader.offset, reader.limit, reader.position)
					t.Errorf("Got %#v, expected %#v when testing %#v (%d)", b, tt.expectedOutput[i], tt.input, i)
				}
			}
		})
	}
}

func TestPaddedFileChunkReadingLimit(t *testing.T) {
	input := []byte("ABCDEFGH")
	tmpFile := CreateTMPFile(t, input)
	var size int64 = 8
	numChunks := 2
	expectedOut1 := []byte("ABCD")
	expectedOut2 := []byte("EFGH")

	bufOut1 := make([]byte, 4)
	bufOut2 := make([]byte, 4)

	readers := SplitIntoPaddedChunks(tmpFile, size, numChunks)

	_, err := readers[0].Read(bufOut1)
	if err != nil {
		t.Errorf("Could not read chunk: %s", err)
	}
	_, err = readers[1].Read(bufOut2)
	if err != nil {
		t.Errorf("Could not read chunk: %s", err)
	}

	if !bytes.Equal(bufOut1, expectedOut1) {
		t.Errorf("Got %#v, expected %#v", string(bufOut1), string(expectedOut1))
	}
	if !bytes.Equal(bufOut2, expectedOut2) {
		t.Errorf("Got %#v, expected %#v", string(bufOut1), string(expectedOut1))
	}

	buf := make([]byte, 4)
	n, err := readers[0].Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("Got %d/%v, expected 0/%v", n, err, io.EOF)
	}
	n, err = readers[1].Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("Got %d/%v, expected 0/%v", n, err, io.EOF)
	}
}

func TestPaddedFileChunkWriting(t *testing.T) {
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
			tmpFile := CreateTMPFile(t, []byte{})
			writers := SplitIntoPaddedChunks(tmpFile, tt.size, tt.numChunks)
			for i, writer := range writers {
				_, err := writer.Write(tt.inputs[i])
				if err != nil {
					t.Errorf("Writing to tmp file failed: %s", err)
				}
			}
			_, err := tmpFile.Seek(0, 0)
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

func TestPaddedFileChunkLimits(t *testing.T) {
	testWriters := []struct {
		name     string
		offset   int64
		limit    int64
		position int64
	}{
		{"empty", 0, 4, 0},
		{"half-written", 0, 4, 2},
		{"empty-2nd", 4, 8, 0},
		{"half-written-2nd", 4, 8, 2},
	}

	for _, tt := range testWriters {
		t.Run(tt.name, func(t *testing.T) {
			cw := &PaddedFileChunk{
				data:     nil,
				offset:   tt.offset,
				limit:    tt.limit,
				position: tt.position,
			}
			n, err := cw.Write([]byte("TOOBIG"))
			if err == nil {
				t.Errorf("Given %d offset, %d limit, and %d position, Write should fail", tt.offset, tt.limit, tt.position)
			}
			if n != 0 {
				t.Errorf("Given a failed write, n should be 0, got %d", n)
			}
		})
	}
}

func TestPaddedFileChunkReadLimits(t *testing.T) {
	tests := []struct{
		name string
		bufLen int
		content []byte
		expectedOutput []byte
	} {
		{"even, large buf", 12, []byte("ABCDEFGH"), []byte{0x41, 0x42, 0x43, 0x44, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x45, 0x46, 0x47, 0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
		{"even, small buf", 2, []byte("ABCDEFGH"), []byte{0x41, 0x42, 0x43, 0x44, 0x0, 0x0, 0x45, 0x46, 0x47, 0x48, 0x0, 0x0}},
		{"odd, large buf", 12, []byte("ABCDEFGHI"), []byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x46, 0x47, 0x48, 0x49, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
		{"odd, small buf", 2, []byte("ABCDEFGHI"), []byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x0, 0x0, 0x0, 0x46, 0x47, 0x48, 0x49, 0x0, 0x0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := CreateTMPFile(t, tt.content)
			cw := SplitIntoPaddedChunks(tmpFile, int64(len(tt.content)), 2)

			readers := make([]io.Reader, len(cw))
			for i := range cw {
				readers[i] = cw[i]
			}

			var readBackContent bytes.Buffer
			for _, chunk := range cw {
				for {
					buf := make([]byte, tt.bufLen)
					_, err := chunk.Read(buf)
					if err != nil {
						if err == io.EOF {
							readBackContent.Write(buf)
							break
						}
						t.Errorf("Expected nil error: got %s", err)
					}
					readBackContent.Write(buf)
				}
			}
			if !bytes.Equal(readBackContent.Bytes(), tt.expectedOutput) {
				t.Errorf("Expected content:\n%#v\nGot content:\n%#v\n", tt.expectedOutput, readBackContent.Bytes())
			}
		})
	}
}

func TestPaddedFileChunkSeeking(t *testing.T) {
	testSeekers := []struct {
		input          []byte
		size           int64
		numChunks      int
		offset         int64
		whence         int
		toReadFirst    int
		toReadSecond   int
		expectedOutput []byte
	}{
		{[]byte("ABCDEFGH"), 8, 1, 0, 0, 4, 4, []byte("ABCD")},
		{[]byte("ABCDEFGH"), 8, 1, 2, 0, 4, 2, []byte("CD")},
		{[]byte("ABCDEFGH"), 8, 1, 2, 1, 2, 2, []byte("EF")},
		{[]byte("ABCDEFGH"), 8, 1, -2, 1, 2, 2, []byte("AB")},
		{[]byte("ABCDEFGH"), 8, 1, -2, 2, 2, 2, []byte("GH")},
		{[]byte("ABCDEFGH"), 8, 1, -8, 2, 2, 2, []byte("AB")},
	}

	for _, tt := range testSeekers {
		t.Run(string(tt.input), func(t *testing.T) {
			tmpFile := CreateTMPFile(t, tt.input)
			seekers := SplitIntoPaddedChunks(tmpFile, tt.size, tt.numChunks)

			buf := make([]byte, tt.toReadFirst)
			_, err := seekers[0].Read(buf)
			if err != nil {
				t.Errorf("Unable to read from chunk: %s", err)
			}
			_, err = seekers[0].Seek(tt.offset, tt.whence)
			if err != nil {
				t.Errorf("Unable to seek in chunk: %s", err)
			}
			buf = make([]byte, tt.toReadSecond)
			_, err = seekers[0].Read(buf)
			if !bytes.Equal(buf, tt.expectedOutput) {
				t.Errorf("Got %v, expected %v", string(buf), string(tt.expectedOutput))
			}
		})
	}
}

func TestPaddedFileChunkSeekingErrors(t *testing.T) {
	input := []byte("ABCDEFGH")
	var size int64 = 8
	numChunks := 2
	testSeekers := []struct {
		name           string
		offset         int
		whence         int
		expectedPos    int64
		expectedErrMsg string
	}{
		{"Bad whence", 0, 4, 0, "Got 4, expected one of: io.SeekStart, io.SeekCurrent, io.SeekEnd"},
		{"Bad whence 2", 0, -2, 0, "Got -2, expected one of: io.SeekStart, io.SeekCurrent, io.SeekEnd"},
		{"Offset > limit 1", 5, 0, 0, "Requested position 5 is larger than chunk limit 4"},
		{"Offset > limit 2", 5, 1, 0, "Requested position 5 is larger than chunk limit 4"},
		{"Offset > limit 3", 2, 2, 0, "Requested position 6 is larger than chunk limit 4"},
		{"Offset < limit 1", -1, 0, 0, "Requested position -1 is smaller than chunk beginning 0"},
		{"Offset < limit 2", -1, 1, 0, "Requested position -1 is smaller than chunk beginning 0"},
		{"Offset < limit 2", -6, 2, 0, "Requested position -2 is smaller than chunk beginning 0"},
	}

	for _, tt := range testSeekers {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := CreateTMPFile(t, input)
			defer os.Remove(tmpFile.Name())
			seekers := SplitIntoPaddedChunks(tmpFile, size, numChunks)

			pos, err := seekers[0].Seek(int64(tt.offset), tt.whence)
			if pos != tt.expectedPos {
				t.Errorf("Got position %d, expected %d", pos, tt.expectedPos)
			}
			if err == nil {
				t.Errorf("Got nil error, expected %s", tt.expectedErrMsg)
			}
			if err.Error() != tt.expectedErrMsg {
				t.Errorf("Got error msg '%s', expected '%s'", err.Error(), tt.expectedErrMsg)
			}
		})
	}
}
