// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package circular

import (
	"fmt"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"
)

// Options defines settings for Buffer.
type Options struct {
	Compressor Compressor

	Logger *zap.Logger

	PersistenceOptions PersistenceOptions

	InitialCapacity int
	MaxCapacity     int
	SafetyGap       int

	NumCompressedChunks int
}

// PersistenceOptions defines settings for Buffer persistence.
type PersistenceOptions struct {
	// ChunkPath is the base path to the store chunk files.
	//
	// Example: /var/log/machine/my-machine.log, chunks will be stored
	// by appending a chunk ID to this path, e.g. /var/log/machine/my-machine.log.3.
	//
	// If ChunkPath is empty, persistence is disabled.
	ChunkPath string

	// FlushInterval flushes buffer content to disk every FlushInterval (if there were any changes).
	FlushInterval time.Duration

	// FlushJitter adds random jitter to FlushInterval to avoid thundering herd problem (a ratio of FlushInterval).
	FlushJitter float64
}

// NextInterval calculates next flush interval with jitter.
func (p PersistenceOptions) NextInterval() time.Duration {
	return time.Duration(((rand.Float64()*2-1)*p.FlushJitter + 1.0) * float64(p.FlushInterval))
}

// Compressor implements an optional interface for chunk compression.
//
// Compress and Decompress append to the dest slice and return the result.
//
// Compressor should be safe for concurrent use by multiple goroutines.
// Compressor should verify checksums of the compressed data.
type Compressor interface {
	Compress(src, dest []byte) ([]byte, error)
	Decompress(src, dest []byte) ([]byte, error)
	DecompressedSize(src []byte) (int64, error)
}

// defaultOptions returns default initial values.
func defaultOptions() Options {
	return Options{
		InitialCapacity: 16384,
		MaxCapacity:     1048576,
		SafetyGap:       1024,
		Logger:          zap.NewNop(),
	}
}

// OptionFunc allows setting Buffer options.
type OptionFunc func(*Options) error

// WithInitialCapacity sets initial buffer capacity.
func WithInitialCapacity(capacity int) OptionFunc {
	return func(opt *Options) error {
		if capacity <= 0 {
			return fmt.Errorf("initial capacity should be positive: %d", capacity)
		}

		opt.InitialCapacity = capacity

		return nil
	}
}

// WithMaxCapacity sets maximum buffer capacity.
func WithMaxCapacity(capacity int) OptionFunc {
	return func(opt *Options) error {
		if capacity <= 0 {
			return fmt.Errorf("max capacity should be positive: %d", capacity)
		}

		opt.MaxCapacity = capacity

		return nil
	}
}

// WithSafetyGap sets safety gap between readers and writers to avoid buffer overrun for the reader.
//
// Reader initial position is set to be as far as possible in the buffer history, but next concurrent write
// might overwrite read position, and safety gap helps to prevent it. With safety gap, maximum available
// bytes to read are: MaxCapacity-SafetyGap.
func WithSafetyGap(gap int) OptionFunc {
	return func(opt *Options) error {
		if gap <= 0 {
			return fmt.Errorf("safety gap should be positive: %d", gap)
		}

		opt.SafetyGap = gap

		return nil
	}
}

// WithNumCompressedChunks sets number of compressed chunks to keep in the buffer.
//
// Default is to keep no compressed chunks, only uncompressed circular buffer is used.
func WithNumCompressedChunks(num int, c Compressor) OptionFunc {
	return func(opt *Options) error {
		if num < 0 {
			return fmt.Errorf("number of compressed chunks should be non-negative: %d", num)
		}

		opt.NumCompressedChunks = num
		opt.Compressor = c

		return nil
	}
}

// WithPersistence enables buffer persistence to disk.
func WithPersistence(options PersistenceOptions) OptionFunc {
	return func(opt *Options) error {
		if options.ChunkPath == "" {
			return fmt.Errorf("chunk path should be set")
		}

		if options.FlushJitter < 0 || options.FlushJitter > 1 {
			return fmt.Errorf("flush jitter should be in range [0, 1]: %f", options.FlushJitter)
		}

		if opt.Compressor == nil {
			return fmt.Errorf("compressor should be set for persistence")
		}

		opt.PersistenceOptions = options

		return nil
	}
}

// WithLogger sets logger for Buffer.
func WithLogger(logger *zap.Logger) OptionFunc {
	return func(opt *Options) error {
		opt.Logger = logger

		return nil
	}
}
