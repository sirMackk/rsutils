package rsutils

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestEncode(t *testing.T) {
	testFile := getTestFile(t, "input3")
	dataShards := 2
	fixtureMd := getMetadata()

	var parityBuffer bytes.Buffer
	md, err := Encode(testFile, dataShards, []io.Writer{&parityBuffer})
	if err != nil {
		t.Fatal(err)
	}

	if md.Size != fixtureMd.Size {
		t.Errorf("Expected md.Size == %d, got %d", fixtureMd.Size, md.Size)
	}
	if md.DataShards != fixtureMd.DataShards || md.ParityShards != fixtureMd.ParityShards {
		t.Errorf("Incorrect metadata ds/ps: got %d/%d, expected %d/%d", md.DataShards, md.ParityShards, fixtureMd.DataShards, fixtureMd.ParityShards)
	}
	if len(md.Hashes) != len(fixtureMd.Hashes) {
		t.Errorf("Incorrect number of hashes: got %d, expected %d", len(md.Hashes), len(fixtureMd.Hashes))
	}

	for i := range md.Hashes {
		if md.Hashes[i] != fixtureMd.Hashes[i] {
			t.Errorf("Incorrect metadata hash: got '%s', expected %s", md.Hashes[i], fixtureMd.Hashes[i])
		}
	}
}

func TestOpen(t *testing.T) {
	// successful
	fixtureMd := getMetadata()
	testFile := getTestFile(t, "input3")
	parityFile := getTestFile(t, "parity1")

	_, err := Open(testFile, []*os.File{parityFile}, fixtureMd)
	if err != nil {
		t.Errorf("Expected no error, got %s", err)
	}

	// insufficient parity files
	expectedErrMsg := "Cannot open encoded files: need 1 parity shards, got 0"
	_, err = Open(testFile, []*os.File{}, fixtureMd)
	if err == nil || err.Error() != expectedErrMsg {
		t.Errorf("Expected error '%s', got '%s'", expectedErrMsg, err)
	}
}

func TestDecode(t *testing.T) {
	readSize := 64
	expectedOutput := `The Tyger, by William Blake!

Tyger Tyger, burning bright,
In th`

	tests := []struct {
		name             string
		dataFileName     string
		parityFileName   string
		output           []byte
		expectedErrorMsg string
	}{
		{"successful read", "input3", "parity1", []byte(expectedOutput), ""},
		{"success - corrupt data", "input4_corrupt", "parity1", []byte(expectedOutput), ""},
		{"success - corrupt parity", "input3", "parity2_corrupt", []byte(expectedOutput), ""},
		{"failure - corrupt data+parity", "input4_corrupt", "parity2_corrupt", make([]byte, readSize), "Cannot repair data: 2 shards corrupt, only have 1 parity shards"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataInput := cloneFileTmp(t, getTestFile(t, tt.dataFileName)).(*os.File)
			parityInput := cloneFileTmp(t, getTestFile(t, tt.parityFileName)).(*os.File)
			md := getMetadata()

			decoder, err := Open(dataInput, []*os.File{parityInput}, md)
			if err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, readSize)
			n, err := decoder.Read(buf)
			if err != nil {
				if n != 0 {
					t.Errorf("Expected to read 0 bytes, got %d", n)
				}
				if !bytes.Equal(buf, tt.output) {
					t.Errorf("Expected empty output, got '%v'", buf)
				}
				if err.Error() != tt.expectedErrorMsg {
					t.Errorf("Expected error '%s', got '%s'", tt.expectedErrorMsg, err)
				}
				return
			}
			if n != readSize {
				t.Errorf("Expected to read 20 bytes, got %d", n)
			}
			if err != nil {
				t.Errorf("Expected read to succeed, got error: %s", err)
			}
			if !bytes.Equal(buf, tt.output) {
				t.Errorf("Expected output '%s', but got '%s'", tt.output, buf)
			}
		})
	}
}

func TestFileSharderUnevenInputE2E(t *testing.T) {
	unevenFileSize := 547
	dataShards := 2

	parityOutput, err := ioutil.TempFile("", "rsutils")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		parityOutput.Close()
		os.Remove(parityOutput.Name())
	})

	unevenFile, err := os.Open("testdata/uneven_input1")
	if err != nil {
		t.Fatal(err)
	}

	md, err := Encode(unevenFile, dataShards, []io.Writer{parityOutput})
	if err != nil {
		t.Fatalf("Expected nil error, got %s", err)
	}

	_, err = unevenFile.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatal(err)
	}
	_, err = parityOutput.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatal(err)
	}

	decoder, err := Open(unevenFile, []*os.File{parityOutput}, md)
	if err != nil {
		t.Fatalf("Expected nil err, got %s", err)
	}

	contents := make([]byte, unevenFileSize)
	_, err = decoder.Read(contents)
	if err != nil {
		t.Fatalf("Expected nil err, got %s", err)
	}

	_, err = unevenFile.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatal(err)
	}
	expectedContents, err := ioutil.ReadAll(unevenFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, expectedContents) {
		t.Errorf("Expected output\n%s\nBut got:\n%s", expectedContents, contents)
	}
}
