// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package circular_test

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/siderolabs/gen/xtesting/must"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/go-circular"
	"github.com/siderolabs/go-circular/zstd"
)

func TestLoad(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		chunkSizes []int

		numCompressedChunks int
	}{
		{
			name: "no chunks",

			numCompressedChunks: 5,
		},
		{
			name: "expected number of chunks",

			numCompressedChunks: 5,

			chunkSizes: []int{1024, 2048, 1024, 1024, 3072, 4096},
		},
		{
			name: "less chunks than expected",

			numCompressedChunks: 5,

			chunkSizes: []int{10240, 20480},
		},
		{
			name: "more chunks than expected",

			numCompressedChunks: 4,

			chunkSizes: []int{10240, 2048, 1024, 1024, 3072, 4096},
		},
		{
			name: "single chunk",

			numCompressedChunks: 2,

			chunkSizes: []int{10240},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			logger := zaptest.NewLogger(t)

			compressor := must.Value(zstd.NewCompressor())(t)

			for i, size := range test.chunkSizes {
				chunkData := bytes.Repeat([]byte{byte(i)}, size)
				chunkData = must.Value(compressor.Compress(chunkData, nil))(t)

				chunkPath := filepath.Join(dir, "chunk."+strconv.Itoa(i))

				require.NoError(t, os.WriteFile(chunkPath, chunkData, 0o644))
			}

			buf, err := circular.NewBuffer(append(test.options,
				circular.WithNumCompressedChunks(test.numCompressedChunks, compressor),
				circular.WithPersistence(circular.PersistenceOptions{
					ChunkPath: filepath.Join(dir, "chunk"),
				}),
				circular.WithLogger(logger),
			)...)
			require.NoError(t, err)

			actualData, err := io.ReadAll(buf.GetReader())
			require.NoError(t, err)

			var expectedData []byte

			firstIdx := 1

			if len(test.chunkSizes) > test.numCompressedChunks+1 {
				firstIdx = len(test.chunkSizes) - test.numCompressedChunks
			}

			for i := firstIdx; i < firstIdx-1+min(len(test.chunkSizes), test.numCompressedChunks+1); i++ {
				expectedData = append(expectedData, bytes.Repeat([]byte{byte(i)}, test.chunkSizes[i])...)
			}

			if len(test.chunkSizes) > 0 {
				expectedData = append(expectedData, bytes.Repeat([]byte{0}, test.chunkSizes[0])...)
			}

			if expectedData == nil {
				expectedData = []byte{}
			}

			require.Equal(t, expectedData, actualData)

			require.NoError(t, buf.Close())
		})
	}
}

func TestPersist(t *testing.T) {
	t.Parallel()

	for _, test := range []struct { //nolint:govet
		name string

		numCompressedChunks int

		options []circular.OptionFunc

		sizes []int64
	}{
		{
			name: "empty",

			numCompressedChunks: 5,

			sizes: []int64{0, 0},
		},
		{
			name: "some data",

			numCompressedChunks: 5,

			sizes: []int64{2048, 2048 * 2048, 131072},
		},
		{
			name: "writes close to max capacity",

			numCompressedChunks: 6,

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(128),
				circular.WithMaxCapacity(1024),
				circular.WithSafetyGap(4),
			},
			sizes: []int64{1019, 1019},
		},
		{
			name: "uneven writes",

			numCompressedChunks: 6,

			sizes: []int64{1024*1024 + 1, 1024*1024 - 1, 1024*1024 - 1, 1024*1024 - 1, 1024*1024 - 1, 1024*1024 - 1},
		},
		{
			name: "dropping old chunks",

			numCompressedChunks: 3,

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(128),
				circular.WithMaxCapacity(1024),
				circular.WithSafetyGap(1),
			},
			sizes: []int64{2048, 2048 + 256, 1024, 1024},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			logger := zaptest.NewLogger(t)

			compressor := must.Value(zstd.NewCompressor())(t)

			expectedContents := []byte{}

			for _, size := range append(test.sizes, 0) { // appending zero to run one more iteration verifying previous write
				data := make([]byte, size)

				for i := range data {
					data[i] = byte(i % 256)
				}

				buf, err := circular.NewBuffer(append(test.options,
					circular.WithNumCompressedChunks(test.numCompressedChunks, compressor),
					circular.WithPersistence(circular.PersistenceOptions{
						ChunkPath: filepath.Join(dir, "chunk"),
					}),
					circular.WithLogger(logger),
				)...)
				require.NoError(t, err)

				bufferContentsAfterLoad, err := io.ReadAll(buf.GetReader())
				require.NoError(t, err)

				require.Equal(t, expectedContents, bufferContentsAfterLoad)

				_, err = buf.Write(data)
				require.NoError(t, err)

				expectedContents = append(expectedContents, data...)

				for len(expectedContents) > buf.MaxCapacity() {
					expectedContents = slices.Delete(expectedContents, 0, buf.Capacity())
				}

				require.NoError(t, buf.Close())
			}
		})
	}
}

func TestFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logger := zaptest.NewLogger(t)

	compressor := must.Value(zstd.NewCompressor())(t)

	buf, err := circular.NewBuffer(
		circular.WithNumCompressedChunks(5, compressor),
		circular.WithPersistence(circular.PersistenceOptions{
			ChunkPath:     filepath.Join(dir, "chunk"),
			FlushInterval: 10 * time.Millisecond,
		}),
		circular.WithLogger(logger),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, buf.Close())
	})

	_, err = buf.Write([]byte("hello"))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.FileExists(t, filepath.Join(dir, "chunk.0"))
	}, time.Second, 10*time.Millisecond)

	st1, err := os.Stat(filepath.Join(dir, "chunk.0"))
	require.NoError(t, err)

	_, err = buf.Write([]byte("world"))
	require.NoError(t, err)

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		var st2 fs.FileInfo

		st2, err = os.Stat(filepath.Join(dir, "chunk.0"))
		require.NoError(t, err)

		assert.Greater(t, st2.Size(), st1.Size())
	}, time.Second, 10*time.Millisecond)

	// without closing buf, re-open it and check if data is still there
	buf2, err := circular.NewBuffer(
		circular.WithNumCompressedChunks(5, compressor),
		circular.WithPersistence(circular.PersistenceOptions{
			ChunkPath: filepath.Join(dir, "chunk"),
		}),
		circular.WithLogger(logger),
	)
	require.NoError(t, err)

	actualData, err := io.ReadAll(buf2.GetReader())
	require.NoError(t, err)

	require.Equal(t, []byte("helloworld"), actualData)

	require.NoError(t, buf2.Close())
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
