package rsutils

import (
	"fmt"
	"io"
)

type ReadAtWriteAtSeeker interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker
}

type PaddedFileChunk struct {
	data     ReadAtWriteAtSeeker
	offset   int64
	limit    int64
	position int64
}

func SplitIntoPaddedChunks(src ReadAtWriteAtSeeker, size int64, numChunks int) []*PaddedFileChunk {
	chunkSize := size / int64(numChunks)
	if size%int64(numChunks) != 0 {
		chunkSize += 1
	}
	readWriteSeekers := make([]*PaddedFileChunk, numChunks)
	for i := 0; i < numChunks; i++ {
		readWriteSeekers[i] = &PaddedFileChunk{
			data:   src,
			offset: int64(i) * chunkSize,
			limit:  int64(i+1) * chunkSize,
		}
	}
	return readWriteSeekers
}

func (pfc *PaddedFileChunk) Read(p []byte) (n int, err error) {
	// chunk is all read
	if pfc.position == pfc.limit-pfc.offset {
		return 0, io.EOF
	}

	var writeTarget []byte = p
	pBufLen := len(p)
	bufShrunk := false
	// if buffer is larger than the chunk
	if bytesLeft := pfc.limit - pfc.offset + pfc.position; int64(pBufLen) > bytesLeft {
		writeTarget = make([]byte, bytesLeft)
		pBufLen = int(bytesLeft)
		bufShrunk = true
	}
	n, err = pfc.data.ReadAt(writeTarget, pfc.offset+pfc.position)
	if err != nil {
		if err == io.EOF {
			// fill rest of buffer with 0's
			copy(writeTarget[n:pBufLen], make([]byte, pBufLen-n))
			n = pBufLen
		}
	}
	pfc.position += int64(n)
	if bufShrunk {
		copy(p, writeTarget)
	}
	return n, err
}

func (pfc *PaddedFileChunk) Write(p []byte) (n int, err error) {
	lp := int64(len(p))
	if bytesLeft := pfc.limit - pfc.offset - pfc.position; lp > bytesLeft {
		return 0, fmt.Errorf("Cannot write %d bytes to chunk; Only %d bytes left", lp, bytesLeft)
	}
	n, err = pfc.data.WriteAt(p, pfc.offset+pfc.position)
	if err != nil {
		return 0, err
	}
	pfc.position += int64(n)
	return n, nil
}

func (pfc *PaddedFileChunk) Seek(offset int64, whence int) (int64, error) {
	var position int64
	switch whence {
	case io.SeekStart:
		position = offset
	case io.SeekCurrent:
		position = pfc.position + offset
	case io.SeekEnd:
		position = pfc.limit + offset
	default:
		return pfc.offset, fmt.Errorf("Got %d, expected one of: io.SeekStart, io.SeekCurrent, io.SeekEnd", whence)
	}
	if pfc.offset+position > pfc.limit {
		return pfc.offset, fmt.Errorf("Requested position %d is larger than chunk limit %d", position, pfc.limit)
	} else if pfc.offset+position < pfc.offset {
		return pfc.offset, fmt.Errorf("Requested position %d is smaller than chunk beginning %d", position, pfc.offset)
	} else {
		pfc.position = position
		return pfc.position, nil
	}
}
