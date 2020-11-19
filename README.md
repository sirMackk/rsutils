# rsutils

_rsutils_ is a thin layer on top of [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) that makes using it a little easier.

Specifically:

- Combine reed-solomon encoding with hashing, padding and metadata creation. In many applications of rs encoding, you will need to pad files, hash the shards (for error detection), and save metadata like size, hashes, number of data/parity shards.
- Make it easier to check encoded data for corruption and reconstruct the corrupted shards.

**NOTE**: This code is below 1.0.0 (it doesn't even have semver!) so the API will likely evolve.


[Reed-Solomon error correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction) is an interesting and popular way of repairing corrupted data.

## TODO

1. Extend README with example usage.
2. Extend code documentation.

## License

Copyright (c) 2020 sirMackk

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see http://www.gnu.org/licenses/.
