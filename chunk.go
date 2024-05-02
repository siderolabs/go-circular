// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package circular

type chunk struct {
	// compressed data
	compressed []byte
	// start offset of the chunk, as it was in the circular buffer when the chunk was created
	startOffset int64
	// uncompressed size of the chunk
	size int64
	// [TODO]: have a unique (incrementing?) chunk ID for file-based storage
}
