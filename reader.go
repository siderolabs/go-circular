// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package circular

import (
	"io"
	"sync/atomic"
)

// Reader implements seekable reader with local position in the Buffer which
// reads from the fixed part of the buffer, or performs streaming reads.
//
// Reader is not safe to be used with concurrent Read/Seek operations.
type Reader struct {
	buf *Buffer

	// if reading from a compressed chunk, chunk is set to non-nil value
	// decompressedChunk is used to store the decompressed chunk, and also re-used as a decompression buffer
	chunk             *chunk
	decompressedChunk []byte

	startOff, endOff int64
	off              int64

	// if streaming, endOff should be set to MaxInt64
	streaming bool

	closed atomic.Bool
}

// StreamingReader is a backwards compatible type alias.
type StreamingReader = Reader

// Read implements io.Reader.
//
//nolint:gocognit
func (r *Reader) Read(p []byte) (n int, err error) {
	if r.closed.Load() {
		return n, ErrClosed
	}

	if r.off == r.endOff {
		return n, io.EOF
	}

	if len(p) == 0 {
		return n, nil
	}

	if r.chunk != nil {
		// how much we can read from the current chunk
		nn := min(r.endOff, r.chunk.startOffset+r.chunk.size) - r.off

		if nn == 0 {
			// switch to the next chunk, or if no chunk is found, switch to the circular buffer
			// at this point, (r.chunk.startOffset + r.chunk.size) == r.off
			r.resetChunk()

			r.buf.mu.Lock()
			r.seekChunk()
			r.buf.mu.Unlock()

			if r.chunk != nil {
				nn = min(r.endOff, r.chunk.startOffset+r.chunk.size) - r.off
			}
		}

		// if r.chunk == nil, we need to switch to the last chunk as a circular buffer, so fallthrough below
		if r.chunk != nil {
			if len(r.decompressedChunk) == 0 {
				r.decompressedChunk, err = r.buf.opt.Compressor.Decompress(r.chunk.compressed, r.decompressedChunk)
				if err != nil {
					return n, err
				}
			}

			if nn > int64(len(p)) {
				nn = int64(len(p))
			}

			copy(p, r.decompressedChunk[r.off-r.chunk.startOffset:r.off-r.chunk.startOffset+nn])

			n = int(nn)
			r.off += nn

			return n, nil
		}
	}

	// from this point down, reading from the current chunk
	r.buf.mu.Lock()
	defer r.buf.mu.Unlock()

	if r.off < r.buf.off-int64(r.buf.opt.MaxCapacity) {
		// check if there is a chunk that has r.off in its range
		r.seekChunk()

		if r.chunk != nil {
			return r.Read(p)
		}

		// reader is falling too much behind
		if !r.streaming {
			return n, ErrOutOfSync
		}

		// reset the offset to the first available position
		r.off = r.buf.off - int64(r.buf.opt.MaxCapacity)
	}

	for r.streaming && r.off == r.buf.off {
		r.buf.cond.Wait()

		if r.closed.Load() {
			return n, ErrClosed
		}
	}

	if r.streaming {
		n = int(r.buf.off - r.off)
	} else {
		n = int(r.endOff - r.off)
	}

	if n > len(p) {
		n = len(p)
	}

	i := int(r.off % int64(r.buf.opt.MaxCapacity))

	if l := r.buf.opt.MaxCapacity - i; l < n {
		copy(p, r.buf.data[i:])
		copy(p[l:], r.buf.data[:n-l])
	} else {
		copy(p, r.buf.data[i:i+n])
	}

	r.off += int64(n)

	return n, err
}

// Close implements io.Closer.
func (r *Reader) Close() error {
	if !r.closed.Swap(true) {
		// wake up readers
		r.buf.cond.Broadcast()
	}

	return nil
}

// Seek implements io.Seeker.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	newOff := r.off

	endOff := r.endOff

	if r.streaming {
		r.buf.mu.Lock()
		endOff = r.buf.off
		r.buf.mu.Unlock()
	}

	switch whence {
	case io.SeekCurrent:
		newOff += offset
	case io.SeekEnd:
		newOff = endOff + offset
	case io.SeekStart:
		newOff = r.startOff + offset
	}

	if newOff < r.startOff {
		return r.off - r.startOff, ErrSeekBeforeStart
	}

	if newOff > endOff {
		newOff = endOff
	}

	r.off = newOff

	if r.chunk != nil {
		if r.off < r.chunk.startOffset || r.off >= r.chunk.startOffset+r.chunk.size {
			// we fell out of the chunk
			r.resetChunk()
		} else {
			// we are within the same chunk
			return r.off - r.startOff, nil
		}
	}

	r.buf.mu.Lock()
	defer r.buf.mu.Unlock()

	r.seekChunk()

	// in streaming mode, make sure the offset is within the buffer
	if r.streaming && r.chunk == nil {
		if len(r.buf.chunks) > 0 {
			if r.off < r.buf.chunks[0].startOffset {
				r.off = r.buf.chunks[0].startOffset
			}
		} else {
			if r.off < endOff-int64(r.buf.opt.MaxCapacity-r.buf.opt.SafetyGap) {
				r.off = endOff - int64(r.buf.opt.MaxCapacity-r.buf.opt.SafetyGap)
			}
		}
	}

	return r.off - r.startOff, nil
}

// seekChunk tries to find a chunk that contains the current reading offset.
//
// seekChunk assumes that r.chunk == nil and r.decompressedChunk is reset before the call.
// seekChunk should be called with r.buf.mu locked.
func (r *Reader) seekChunk() {
	for i, c := range r.buf.chunks {
		if r.off >= c.startOffset && r.off < c.startOffset+c.size {
			// we found the chunk
			r.chunk = &r.buf.chunks[i]

			break
		}
	}
}

// resetChunk resets the current chunk and decompressed chunk.
func (r *Reader) resetChunk() {
	r.chunk = nil

	if r.decompressedChunk != nil {
		r.decompressedChunk = r.decompressedChunk[:0]
	}
}
