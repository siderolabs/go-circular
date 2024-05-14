// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build !race

package circular_test

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/siderolabs/gen/xtesting/must"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/go-circular"
	"github.com/siderolabs/go-circular/zstd"
)

func BenchmarkWrite(b *testing.B) {
	for _, test := range []struct {
		name string

		options []circular.OptionFunc
	}{
		{
			name: "defaults",
		},
		{
			name: "growing buffer",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(16384),
				circular.WithMaxCapacity(1048576),
			},
		},
		{
			name: "compression",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(16384),
				circular.WithMaxCapacity(131072),

				circular.WithNumCompressedChunks(9, must.Value(zstd.NewCompressor())(b)),
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			data, err := io.ReadAll(io.LimitReader(rand.Reader, 1024))
			require.NoError(b, err)

			buf, err := circular.NewBuffer(test.options...)
			require.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, err := buf.Write(data)
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkRead(b *testing.B) {
	for _, test := range []struct {
		name string

		options []circular.OptionFunc
	}{
		{
			name: "defaults",
		},
		{
			name: "growing buffer",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(16384),
				circular.WithMaxCapacity(1048576),
			},
		},
		{
			name: "compression",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(16384),
				circular.WithMaxCapacity(131072),

				circular.WithNumCompressedChunks(9, must.Value(zstd.NewCompressor())(b)),
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			data, err := io.ReadAll(io.LimitReader(rand.Reader, 1024))
			require.NoError(b, err)

			buf, err := circular.NewBuffer(test.options...)
			require.NoError(b, err)

			for range 65536 {
				_, err := buf.Write(data)
				require.NoError(b, err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				r := buf.GetReader()

				_, err := io.Copy(io.Discard, r)
				require.NoError(b, err)
			}
		})
	}
}

func testBenchmarkAllocs(t *testing.T, f func(b *testing.B), threshold int64) {
	res := testing.Benchmark(f)

	allocs := res.AllocsPerOp()
	if allocs > threshold {
		t.Fatalf("Expected AllocsPerOp <= %d, got %d", threshold, allocs)
	}
}

func TestBenchmarkWriteAllocs(t *testing.T) {
	testBenchmarkAllocs(t, BenchmarkWrite, 0)
}

func TestBenchmarkReadAllocs(t *testing.T) {
	testBenchmarkAllocs(t, BenchmarkRead, 5)
}
