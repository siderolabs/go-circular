// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package zstd_test

import (
	"crypto/rand"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/siderolabs/go-circular/zstd"
)

func TestCompressor(t *testing.T) {
	t.Parallel()

	compressor, err := zstd.NewCompressor()
	require.NoError(t, err)

	for _, test := range []struct {
		size int
	}{
		{
			size: 0,
		},
		{
			size: 1024,
		},
		{
			size: 1024 * 1024,
		},
	} {
		t.Run(strconv.Itoa(test.size), func(t *testing.T) {
			t.Parallel()

			data, err := io.ReadAll(io.LimitReader(rand.Reader, int64(test.size)))
			require.NoError(t, err)

			compressed, err := compressor.Compress(data, nil)
			require.NoError(t, err)

			decompressed, err := compressor.Decompress(compressed, nil)
			require.NoError(t, err)

			if len(data) == 0 {
				data = nil
			}

			require.Equal(t, data, decompressed)

			decompressedSize, err := compressor.DecompressedSize(compressed)
			require.NoError(t, err)

			require.Equal(t, int64(len(data)), decompressedSize)
		})
	}
}
