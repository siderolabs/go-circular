// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package zstd compression and decompression functions.
package zstd

import (
	"github.com/klauspost/compress/zstd"
)

// Compressor implements Compressor using zstd compression.
type Compressor struct {
	dec *zstd.Decoder
	enc *zstd.Encoder
}

// NewCompressor creates new Compressor.
func NewCompressor(opts ...zstd.EOption) (*Compressor, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, err
	}

	return &Compressor{
		dec: dec,
		enc: enc,
	}, nil
}

// Compress data using zstd.
func (c *Compressor) Compress(src, dest []byte) ([]byte, error) {
	return c.enc.EncodeAll(src, dest), nil
}

// Decompress data using zstd.
func (c *Compressor) Decompress(src, dest []byte) ([]byte, error) {
	return c.dec.DecodeAll(src, dest)
}
