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

func NewPaddedFileChunk(src ReadAtWriteAtSeeker, size int64, numChunks int) []*PaddedFileChunk {
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
	if pfc.position == pfc.limit-pfc.offset {
		return 0, io.EOF
	}
	pBufLen := int64(len(p))
	n, err = pfc.data.ReadAt(p, pfc.offset+pfc.position)
	if err != nil {
		if err == io.EOF {
			copy(p[n:pBufLen], make([]byte, pBufLen-int64(n)))
			pfc.position += int64(n)
			// return chunksize as length n to zero out p
			return int(pfc.limit - pfc.offset), io.EOF
		}
		return n, err
	}
	pfc.position += int64(n)
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
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = pfc.offset + offset
	case io.SeekCurrent:
		newOffset = pfc.position + offset
	case io.SeekEnd:
		newOffset = pfc.limit + offset
	default:
		return pfc.offset, fmt.Errorf("Got %d, expected one of: io.SeekStart, io.SeekCurrent, io.SeekEnd", whence)
	}
	if newOffset > pfc.limit {
		return pfc.offset, fmt.Errorf("Requested offset %d is larger than chunk limit %d", newOffset, pfc.limit)
	} else if newOffset < pfc.offset {
		return pfc.offset, fmt.Errorf("Requested offset %d is smaller than chunk beginning %d", newOffset, pfc.offset)
	} else {
		pfc.position = newOffset
		return pfc.position, nil
	}
}
