# rsutils

_rsutils_ is a thin layer on top of [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) that makes using it a little easier.

Specifically:

- Combine reed-solomon encoding with hashing, padding and metadata creation. In many applications of rs encoding, you will need to pad files, hash the shards (for error detection), and save metadata like size, hashes, number of data/parity shards.
- Make it easier to check encoded data for corruption and reconstruct the corrupted shards.

**NOTE**: This code is below 1.0.0 (it doesn't even have semver!) so the API will likely evolve.


[Reed-Solomon error correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction) is an interesting and popular way of repairing corrupted data.

## Example Usage - Stable, high-level API

This API is set up to take a data file and produce n parity shards and metadata information.

The parity shards are characterized by:
- each parity shard's size equals `(data file size / data shards)`.
- they can number at least 1 but less than 256.

The metadata is needed to read and repair the data. It looks like:

```go
type Metadata struct {
	Size         int64
	Hashes       []string
	DataShards   int
	ParityShards int
}
```

Creating parity shards is done using the `Encode` function:

```go
dataShards := 3
parity1Output, _ := os.Create("parity1")
parity2Output, _ := os.Create("parity2")
dataFile, _ := os.Open("dataFile")

meta, _ := rsutils.Encode(dataFile, dataShards, []io.Writer{parity1Output, parity2Output})
// Save "meta" somewhere, eg. a json file, a database, etc.
```

Reading data back is done using an io.Reader interface that checks integrity and attempts to repair files without the user knowing. If the data corruption is too great, it will error. The reason why the inputs to Read are all *os.File is because the need to read, write, and seek them in case of repairing corrupt data. This would be expensive to do over the network so the more common use case is to do it on local files.

```go
dataFile, _ := os.Open("dataFile")
parity1, _ := os.Open("parity1")
parity2, _ := os.Open("parity2")
// re-use metadata object from previous example.
decoder, _ := rsutils.Open(dataFile, []*os.File{parity1, parity2}, meta)

buf := make([]byte, 512)
// Checks integrity on first read. If corruption is detected and have enough
// data, will repair data and parity files in place, then continue with Read operation.
decoder.Read(buf)
// buf contains the first 512 bytes of dataFile
```

## Example Usage - Experimental, lower-level API

This API may change without notice!

Creating shards creates a piece of metadata that's required to check/repair the data later.

### Metadata

```go
type Metadata struct {
	Size         int64
	Hashes       []string
	DataShards   int
	ParityShards int
}
```

Note: `Hashes` contains sha256 hashes of each data and parity shard to check their integrity.


### Creating parity shards

Use a ShardCreator to generate parity shards:

```go
dataShards = 10
parityShards = 4
dataSizeBytes = 400
// We need <dataShards> number of readers that contain data. These could be streams, chunks of a file, or even different files (if they are the same size).
dataSources := make([]io.Reader, 10)
// We need <parityShards> number of writers that could be streams or files.
parityWriters := make([]io.Writer, 4)
creator := NewShardCreator(dataSources, dataSizeBytes, dataShards, parityShards)
// We want to save the metadata somewhere like a json file or database to keep track of the shard hashes.
metadata, err := creator.Encode(parityWriters)
```

### Checking data integrity

Use a ShardManager to check data/parity integrity and repair broken data:

```go
dataShards = 10
parityShards = 4
// We need the metadata output by ShardCreator.Encode
md := &Metadata{<bla bla>}
// We need the data and parity shards as ReadWriteSeekers to check the integrity of each shard.
shards := make([]io.ReadWriteSeeker, dataShards+parityShards)
manager := NewShardManager(shards, md)
// err = nil if all shards are good.
err := manager.CheckHealth()
```

### Repairing data

 Use a ShardManager to repair data when you know it's broken:

```go
dataShards = 10
parityShards = 4
// We need the metadata output by ShardCreator.Encode
md := &Metadata{<bla bla>}
// We need the data and parity shards as ReadWriteSeekers to fix the data/parity in place.
shards := make([]io.ReadWriteSeeker, dataShards+parityShards)
manager := NewShardManager(shards, md)
// Note: if the number of broken shards is bigger than available parity shards, this will fail. 
err := manager.Repair()
```

### Extra: Chunking a file

In many cases, you will be working with files, so there's a utility called function `SplitIntoPaddedChunks` that chunks a file into _n_ streams that expose Read/Write/Seek methods.

This function takes anything that implements that `ReadAtWriteAtSeeker` interface:

```go
type ReadAtWriteAtSeeker interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker
}
```

A Go *os.File will work nicely here.

For example:

```go
dataFile, _ := os.Open(filePath)
defer dataFile.Close()
// We need the size of the input file to know how to pad the last chunk.
dataFileStat, _ := dataFile.Stat()
dataFileSize := dataFileStat.Size()

// How many chunks do we want?
numChunks := 12

dataChunks := SplitIntoPaddedChunks(dataFile, dataFileSize, numChunks)

// len(dataChunks) == numChunks
// Each dataChunk fulfills the io.Reader, io.Writer, and io.Seeker interfaces.
//  Before feeding it to either ShardCreator or ShardManager, you will have to make them into io.Readers or io.ReadWriteSeekers through an explicit cast.
```

## TODO

1. ~Extend README with example usage.~
2. Extend code documentation.

## License

Copyright (c) 2020 sirMackk

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see http://www.gnu.org/licenses/.
