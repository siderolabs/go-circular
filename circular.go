// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package circular provides a buffer with circular semantics.
package circular

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/siderolabs/gen/optional"
)

// Buffer implements circular buffer with a thread-safe writer,
// that supports multiple readers each with its own offset.
type Buffer struct {
	// waking up streaming readers on new writes
	cond *sync.Cond

	// channel for persistence commands from the writer to the persistence goroutine
	commandCh chan persistenceCommand

	// compressed chunks, ordered from the smallest offset to the largest
	chunks []chunk

	// the last uncompressed chunk, might grow up to MaxCapacity, then used
	// as circular buffer
	data []byte

	// buffer options
	opt Options

	// waitgroup to wait for persistence goroutine to finish
	wg sync.WaitGroup

	// closed flag (to disable writes after close)
	closed atomic.Bool

	// synchronizing access to data, off, chunks
	mu sync.Mutex

	// write offset, always goes up, actual offset in data slice
	// is (off % cap(data))
	off int64
}

// NewBuffer creates new Buffer with specified options.
func NewBuffer(opts ...OptionFunc) (*Buffer, error) {
	buf := &Buffer{
		opt: defaultOptions(),
	}

	for _, o := range opts {
		if err := o(&buf.opt); err != nil {
			return nil, err
		}
	}

	if buf.opt.InitialCapacity > buf.opt.MaxCapacity {
		return nil, fmt.Errorf("initial capacity (%d) should be less or equal to max capacity (%d)", buf.opt.InitialCapacity, buf.opt.MaxCapacity)
	}

	if buf.opt.SafetyGap >= buf.opt.MaxCapacity {
		return nil, fmt.Errorf("safety gap (%d) should be less than max capacity (%d)", buf.opt.SafetyGap, buf.opt.MaxCapacity)
	}

	buf.data = make([]byte, buf.opt.InitialCapacity)
	buf.cond = sync.NewCond(&buf.mu)

	if err := buf.load(); err != nil {
		return nil, err
	}

	buf.run()

	return buf, nil
}

// Close closes the buffer and waits for persistence goroutine to finish.
func (buf *Buffer) Close() error {
	if buf.closed.Swap(true) {
		return nil
	}

	if buf.commandCh != nil {
		close(buf.commandCh)
	}

	buf.wg.Wait()

	return nil
}

// Write implements io.Writer interface.
//
//nolint:gocognit
func (buf *Buffer) Write(p []byte) (int, error) {
	l := len(p)
	if l == 0 {
		return 0, nil
	}

	if buf.closed.Load() {
		return 0, ErrClosed
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.off < int64(buf.opt.MaxCapacity) {
		if buf.off+int64(l) > int64(cap(buf.data)) && cap(buf.data) < buf.opt.MaxCapacity {
			// grow buffer to ensure write fits, but limit with max capacity
			size := cap(buf.data) * 2
			for size < int(buf.off)+l {
				size *= 2
			}

			if size > buf.opt.MaxCapacity {
				size = buf.opt.MaxCapacity
			}

			data := make([]byte, size)
			copy(data, buf.data)
			buf.data = data
		}
	}

	var n int
	for n < l {
		rotate := false

		i := int(buf.off % int64(buf.opt.MaxCapacity))

		nn := buf.opt.MaxCapacity - i
		if nn > len(p) {
			nn = len(p)
		} else {
			rotate = true
		}

		copy(buf.data[i:], p[:nn])

		buf.off += int64(nn)
		n += nn
		p = p[nn:]

		if rotate && buf.opt.NumCompressedChunks > 0 {
			// we can't reuse any of the chunk buffers, as they might be referenced by readers
			compressed, err := buf.opt.Compressor.Compress(buf.data, nil)
			if err != nil {
				return n, err
			}

			var maxID int64

			for _, c := range buf.chunks {
				maxID = max(c.id, maxID)
			}

			buf.chunks = append(buf.chunks, chunk{
				compressed:  compressed,
				startOffset: buf.off - int64(buf.opt.MaxCapacity),
				size:        int64(buf.opt.MaxCapacity),
				id:          maxID + 1,
			})

			if buf.commandCh != nil {
				buf.commandCh <- persistenceCommand{
					chunkID: maxID + 1,
					data:    compressed,
				}
			}

			if len(buf.chunks) > buf.opt.NumCompressedChunks {
				if buf.commandCh != nil {
					buf.commandCh <- persistenceCommand{
						chunkID: buf.chunks[0].id,
						drop:    true,
					}
				}

				buf.chunks = slices.Delete(buf.chunks, 0, 1)
			}
		}
	}

	buf.cond.Broadcast()

	return n, nil
}

// Capacity returns number of bytes allocated for the buffer.
func (buf *Buffer) Capacity() int {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	return cap(buf.data)
}

// MaxCapacity returns maximum number of (decompressed) bytes (including compressed chunks) that can be stored in the buffer.
func (buf *Buffer) MaxCapacity() int {
	return buf.opt.MaxCapacity * (buf.opt.NumCompressedChunks + 1)
}

// NumCompressedChunks returns number of compressed chunks.
func (buf *Buffer) NumCompressedChunks() int {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	return len(buf.chunks)
}

// TotalCompressedSize reports the overall memory used by the circular buffer including compressed chunks.
func (buf *Buffer) TotalCompressedSize() int64 {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	var size int64

	for _, c := range buf.chunks {
		size += int64(len(c.compressed))
	}

	return size + int64(cap(buf.data))
}

// TotalSize reports overall number of bytes available for reading in the buffer.
//
// TotalSize might be higher than TotalCompressedSize, because compressed chunks
// take less memory than uncompressed data.
func (buf *Buffer) TotalSize() int64 {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	if len(buf.chunks) == 0 {
		if buf.off < int64(cap(buf.data)) {
			return buf.off
		}

		return int64(cap(buf.data))
	}

	return buf.off - buf.chunks[0].startOffset
}

// Offset returns current write offset (number of bytes written).
func (buf *Buffer) Offset() int64 {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	return buf.off
}

// GetStreamingReader returns Reader object which implements io.ReadCloser, io.Seeker.
//
// StreamingReader starts at the most distant position in the past available.
func (buf *Buffer) GetStreamingReader() *Reader {
	r := buf.GetReader()

	r.endOff = math.MaxInt64
	r.streaming = true

	return r
}

// GetReader returns Reader object which implements io.ReadCloser, io.Seeker.
//
// Reader starts at the most distant position in the past available and goes
// to the current write position.
func (buf *Buffer) GetReader() *Reader {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	if len(buf.chunks) > 0 {
		oldestChunk := buf.chunks[0]

		return &Reader{
			buf: buf,

			chunk: optional.Some(oldestChunk),

			startOff: oldestChunk.startOffset,
			endOff:   buf.off,
			off:      oldestChunk.startOffset,
		}
	}

	off := buf.off - int64(buf.opt.MaxCapacity-buf.opt.SafetyGap)
	if off < 0 {
		off = 0
	}

	return &Reader{
		buf:      buf,
		startOff: off,
		endOff:   buf.off,
		off:      off,
	}
}
