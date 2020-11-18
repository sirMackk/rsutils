package rsutils

import (
	"fmt"
	"io"
)

type PaddedChunkedReader struct {
	source    io.ReaderAt
	offset    int64
	limit     int64
	bytesRead int64
}

func SplitIntoPaddedChunks(src io.ReaderAt, size int64, numChunks int) []*PaddedChunkedReader {
	chunkSize := size / int64(numChunks)
	if size%int64(numChunks) != 0 {
		chunkSize += 1
	}
	readers := make([]*PaddedChunkedReader, numChunks)
	for i := 0; i < numChunks; i++ {
		readers[i] = &PaddedChunkedReader{
			source: src,
			offset: int64(i) * chunkSize,
			limit:  int64(i+1) * chunkSize,
		}
	}
	return readers
}

func (pcr *PaddedChunkedReader) Read(p []byte) (n int, err error) {
	if pcr.bytesRead == pcr.limit-pcr.offset {
		return 0, io.EOF
	}
	pBufLen := int64(len(p))
	n, err = pcr.source.ReadAt(p[:pcr.limit-pcr.offset-pcr.bytesRead], pcr.offset+pcr.bytesRead)
	if err != nil {
		if err == io.EOF {
			copy(p[n:pBufLen], make([]byte, pBufLen-int64(n)))
			pcr.bytesRead += int64(n)
			// return chunksize as length n to zero out p
			return int(pcr.limit - pcr.offset), io.EOF
		}
		return n, err
	}
	pcr.bytesRead += int64(n)
	return n, err
}

type ChunkWriter struct {
	dest         io.WriterAt
	offset       int64
	limit        int64
	bytesWritten int64
}

func SplitIntoChunkWriters(dst io.WriterAt, size int64, numChunks int) []*ChunkWriter {
	chunkSize := size / int64(numChunks)
	if size%int64(numChunks) != 0 {
		chunkSize += 1
	}
	writers := make([]*ChunkWriter, numChunks)
	for i := 0; i < numChunks; i++ {
		writers[i] = &ChunkWriter{
			dest:   dst,
			offset: int64(i) * chunkSize,
			limit:  int64(i+1) * chunkSize,
		}
	}
	return writers
}

func (c *ChunkWriter) Write(p []byte) (n int, err error) {
	lp := int64(len(p))
	if bytesLeft := c.limit - c.offset - c.bytesWritten; lp > bytesLeft {
		return 0, fmt.Errorf("Cannot write %d bytes to chunk; Only %d bytes left", lp, bytesLeft)
	}
	n, err = c.dest.WriteAt(p, c.offset+c.bytesWritten)
	if err != nil {
		return 0, err
	}
	c.bytesWritten += int64(n)
	return n, nil
}
