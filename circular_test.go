// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package circular_test

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/siderolabs/gen/xtesting/must"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/siderolabs/go-circular"
	"github.com/siderolabs/go-circular/zstd"
)

func TestWrites(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		expectedNumCompressedChunks int
		expectedTotalCompressedSize int64
		expectedTotalSize           int64
	}{
		{
			name: "no chunks",
			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048), circular.WithMaxCapacity(100_000),
			},

			expectedNumCompressedChunks: 0,
			expectedTotalCompressedSize: 100_000,
			expectedTotalSize:           100_000,
		},
		{
			name: "chunks",
			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(100_000),
				circular.WithNumCompressedChunks(5, must.Value(zstd.NewCompressor())(t)),
			},

			expectedNumCompressedChunks: 5,
			expectedTotalCompressedSize: 100_085,
			expectedTotalSize:           554_675,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			n, err := buf.Write(nil)
			req.NoError(err)
			req.Equal(0, n)

			n, err = buf.Write(make([]byte, 100))
			req.NoError(err)
			req.Equal(100, n)

			n, err = buf.Write(make([]byte, 1000))
			req.NoError(err)
			req.Equal(1000, n)

			req.Equal(2048, buf.Capacity())
			req.EqualValues(1100, buf.Offset())

			n, err = buf.Write(make([]byte, 5000))
			req.NoError(err)
			req.Equal(5000, n)

			req.Equal(8192, buf.Capacity())
			req.EqualValues(6100, buf.Offset())

			for i := range 20 {
				l := 1 << i

				n, err = buf.Write(make([]byte, l))
				req.NoError(err)
				req.Equal(l, n)
			}

			req.Equal(100000, buf.Capacity())
			req.EqualValues(6100+(1<<20)-1, buf.Offset())

			req.EqualValues(test.expectedNumCompressedChunks, buf.NumCompressedChunks())
			req.EqualValues(test.expectedTotalCompressedSize, buf.TotalCompressedSize())
			req.EqualValues(test.expectedTotalSize, buf.TotalSize())
		})
	}
}

// TestStreamingUnboundedReadWriter tests the streaming reader and writer without any checks for all data read.
//
// Reader and writer are running concurrently, writer writes data to the buffer, and reader reads data from the buffer.
// This test is primary for testing race-conditions and other panic scenarios vs. correctness.
//
// The test TestStreamingReadWriter is similar to this test, but it checks the data read by the reader.
func TestStreamingUnboundedReadWriter(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(201),
				circular.WithMaxCapacity(65532),
			},
		},
		{
			name: "chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(137),
				circular.WithMaxCapacity(4987),
				circular.WithNumCompressedChunks(12, must.Value(zstd.NewCompressor())(t)),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)
			asrt := assert.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			r := buf.GetStreamingReader()

			size := 10 * 1048576

			data, err := io.ReadAll(io.LimitReader(cryptorand.Reader, int64(size)))
			req.NoError(err)

			var wg sync.WaitGroup
			defer wg.Wait()

			wg.Add(1)

			go func() {
				defer wg.Done()

				nRead, copyErr := io.CopyBuffer(io.Discard, r, make([]byte, 41))

				t.Logf("read %d bytes", nRead)

				asrt.Error(copyErr)
				asrt.ErrorIs(copyErr, circular.ErrClosed)
			}()

			p := data

			for i := 0; i < len(data); {
				l := 100 + int(rand.Int32N(100))

				if i+l > len(data) {
					l = len(data) - i
				}

				n, e := buf.Write(p[:l])
				req.NoError(e)

				req.Equal(l, n)

				i += l
				p = p[l:]
			}

			time.Sleep(50 * time.Millisecond) // wait for the reader goroutine to process data

			req.NoError(r.Close())
		})
	}
}

func TestStreamingReadWriter(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(65536),
			},
		},
		{
			name: "chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(16384),
				circular.WithNumCompressedChunks(4, must.Value(zstd.NewCompressor())(t)),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)
			asrt := assert.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			r := buf.GetStreamingReader()

			size := 1048576

			data, err := io.ReadAll(io.LimitReader(cryptorand.Reader, int64(size)))
			req.NoError(err)

			var wg sync.WaitGroup
			defer wg.Wait()

			wg.Add(1)

			go func() {
				defer wg.Done()

				p := data

				r := rate.NewLimiter(300_000, 1000)

				for i := 0; i < len(data); {
					l := 100 + int(rand.Int32N(100))

					if i+l > len(data) {
						l = len(data) - i
					}

					r.WaitN(context.Background(), l) //nolint:errcheck

					n, e := buf.Write(p[:l])
					if e != nil {
						panic(e)
					}

					if n != l {
						panic(fmt.Sprintf("short write: %d != %d", n, l))
					}

					i += l
					p = p[l:]
				}
			}()

			actual := make([]byte, size)

			n, err := io.ReadFull(r, actual)
			req.NoError(err)
			req.Equal(size, n)

			req.Equal(data, actual)

			s := make(chan error)

			go func() {
				_, err = r.Read(make([]byte, 1))

				s <- err
			}()

			time.Sleep(50 * time.Millisecond) // wait for the goroutine to start

			req.NoError(r.Close())

			// close should abort reader
			asrt.ErrorIs(<-s, circular.ErrClosed)

			_, err = r.Read(nil)
			req.ErrorIs(err, circular.ErrClosed)
		})
	}
}

//nolint:gocognit
func TestStreamingMultipleReaders(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(65536),
			},
		},
		{
			name: "chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(16384),
				circular.WithNumCompressedChunks(4, must.Value(zstd.NewCompressor())(t)),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			n := 5

			readers := make([]*circular.Reader, n)

			for i := range n {
				readers[i] = buf.GetStreamingReader()
			}

			size := 1048576

			data, err := io.ReadAll(io.LimitReader(cryptorand.Reader, int64(size)))
			req.NoError(err)

			var eg errgroup.Group

			for _, reader := range readers {
				eg.Go(func(reader *circular.Reader) func() error {
					return func() error {
						actual := make([]byte, size)

						nn, err := io.ReadFull(reader, actual)
						if err != nil {
							return err
						}

						if size != nn {
							return fmt.Errorf("short read: %d != %d", nn, size)
						}

						if !bytes.Equal(data, actual) {
							return fmt.Errorf("data mismatch")
						}

						return nil
					}
				}(reader))
			}

			p := data

			r := rate.NewLimiter(300_000, 1000)

			for i := 0; i < len(data); {
				l := 256

				if i+l > len(data) {
					l = len(data) - i
				}

				r.WaitN(context.Background(), l) //nolint:errcheck

				n, e := buf.Write(p[:l])
				req.NoError(e)
				req.Equal(l, n)

				i += l
				p = p[l:]
			}

			req.NoError(eg.Wait())
		})
	}
}

func TestStreamingLateAndIdleReaders(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		expectedLateReadSize int
		expectedIdleReadSize int
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(65536),
				circular.WithSafetyGap(256),
			},

			expectedLateReadSize: 65536 - 256,
			expectedIdleReadSize: 65536,
		},
		{
			name: "chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(16384),
				circular.WithSafetyGap(256),
				circular.WithNumCompressedChunks(4, must.Value(zstd.NewCompressor())(t)),
			},

			expectedLateReadSize: 67232,
			expectedIdleReadSize: 16384,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			idleR := buf.GetStreamingReader()

			size := 100000

			data, err := io.ReadAll(io.LimitReader(cryptorand.Reader, int64(size)))
			req.NoError(err)

			n, err := buf.Write(data)
			req.NoError(err)
			req.Equal(size, n)

			lateR := buf.GetStreamingReader()

			closeCh := make(chan error)

			go func() {
				time.Sleep(50 * time.Millisecond)

				closeCh <- lateR.Close()
			}()

			actual, err := io.ReadAll(lateR)
			req.Equal(circular.ErrClosed, err)
			req.Equal(test.expectedLateReadSize, len(actual))

			req.Equal(data[size-test.expectedLateReadSize:], actual)

			req.NoError(<-closeCh)

			go func() {
				time.Sleep(50 * time.Millisecond)

				closeCh <- idleR.Close()
			}()

			actual, err = io.ReadAll(idleR)
			req.Equal(circular.ErrClosed, err)
			req.Equal(test.expectedIdleReadSize, len(actual))

			req.Equal(data[size-test.expectedIdleReadSize:], actual)

			req.NoError(<-closeCh)
		})
	}
}

func TestStreamingSeek(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		expectedOverflowSeek1 int64
		expectedOverflowSeek2 int64
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(65536),
				circular.WithSafetyGap(256),
			},

			expectedOverflowSeek1: 1024,
			expectedOverflowSeek2: 2048,
		},
		{
			name: "chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(16384),
				circular.WithSafetyGap(256),
				circular.WithNumCompressedChunks(3, must.Value(zstd.NewCompressor())(t)),
			},

			expectedOverflowSeek1: 16384,
			expectedOverflowSeek2: 16384,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)
			asrt := assert.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			_, err = buf.Write(bytes.Repeat([]byte{0xff}, 512))
			req.NoError(err)

			r := buf.GetStreamingReader()

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 512))
			req.NoError(err)

			off, err := r.Seek(0, io.SeekCurrent)
			req.NoError(err)
			asrt.EqualValues(0, off)

			data := make([]byte, 256)

			n, err := r.Read(data)
			req.NoError(err)
			asrt.Equal(256, n)
			asrt.Equal(bytes.Repeat([]byte{0xff}, 256), data)

			off, err = r.Seek(0, io.SeekCurrent)
			req.NoError(err)
			asrt.EqualValues(256, off)

			off, err = r.Seek(-256, io.SeekEnd)
			req.NoError(err)
			asrt.EqualValues(768, off)

			n, err = r.Read(data)
			req.NoError(err)
			asrt.Equal(256, n)
			asrt.Equal(bytes.Repeat([]byte{0xfe}, 256), data)

			off, err = r.Seek(2048, io.SeekStart)
			req.NoError(err)
			asrt.EqualValues(1024, off)

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 65536-256))
			req.NoError(err)

			off, err = r.Seek(0, io.SeekStart)
			req.NoError(err)
			asrt.EqualValues(test.expectedOverflowSeek1, off)

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 1024))
			req.NoError(err)

			off, err = r.Seek(0, io.SeekCurrent)
			req.NoError(err)
			asrt.EqualValues(test.expectedOverflowSeek2, off)

			_, err = r.Seek(-100, io.SeekStart)
			req.ErrorIs(err, circular.ErrSeekBeforeStart)
		})
	}
}

func TestRegularReaderEmpty(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc
	}{
		{
			name: "no chunks",
		},
		{
			name: "chunks",
			options: []circular.OptionFunc{
				circular.WithNumCompressedChunks(5, must.Value(zstd.NewCompressor())(t)),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			n, err := buf.GetReader().Read(nil)
			req.Equal(0, n)
			req.Equal(io.EOF, err)
		})
	}
}

func TestRegularReader(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		size int
	}{
		{
			name: "no chunks",

			size: 512,
		},
		{
			name: "chunks",
			options: []circular.OptionFunc{
				circular.WithMaxCapacity(65536),
				circular.WithNumCompressedChunks(5, must.Value(zstd.NewCompressor())(t)),
			},

			size: 262144,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			data, err := io.ReadAll(io.LimitReader(cryptorand.Reader, int64(test.size)))
			req.NoError(err)

			_, err = buf.Write(data)
			req.NoError(err)

			r := buf.GetReader()

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 512))
			req.NoError(err)

			actual, err := io.ReadAll(r)
			req.NoError(err)
			req.Equal(data, actual)
		})
	}
}

func TestRegularReaderOutOfSync(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		expectOutOfSync bool
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(65536),
				circular.WithSafetyGap(256),
			},

			expectOutOfSync: true,
		},
		{
			name: "chunks, no out of sync",
			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(65536),
				circular.WithSafetyGap(256),
				circular.WithNumCompressedChunks(5, must.Value(zstd.NewCompressor())(t)),
			},
		},
		{
			name: "not enough chunks",
			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(2048),
				circular.WithSafetyGap(256),
				circular.WithNumCompressedChunks(2, must.Value(zstd.NewCompressor())(t)),
			},

			expectOutOfSync: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			data, err := io.ReadAll(io.LimitReader(cryptorand.Reader, 512))
			req.NoError(err)

			_, err = buf.Write(data)
			req.NoError(err)

			r := buf.GetReader()

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 65536-256))
			req.NoError(err)

			actual := make([]byte, 512)
			_, err = r.Read(actual)

			if test.expectOutOfSync {
				req.ErrorIs(err, circular.ErrOutOfSync)
			} else {
				req.NoError(err)
				req.Equal(data, actual)
			}
		})
	}
}

func TestRegularReaderSafetyGap(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		expectedBytesRead int
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(4096),
				circular.WithSafetyGap(256),
			},

			expectedBytesRead: 4096 - 256,
		},
		{
			name: "chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(4096),
				circular.WithSafetyGap(256),
				circular.WithNumCompressedChunks(5, must.Value(zstd.NewCompressor())(t)),
			},

			expectedBytesRead: 6146,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			_, err = buf.Write(bytes.Repeat([]byte{0xff}, 6146))
			req.NoError(err)

			r := buf.GetReader()

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 100))
			req.NoError(err)

			actual, err := io.ReadAll(r)
			req.NoError(err)
			req.Equal(bytes.Repeat([]byte{0xff}, test.expectedBytesRead), actual)

			req.NoError(r.Close())

			_, err = r.Read(nil)
			req.Equal(err, circular.ErrClosed)
		})
	}
}

func TestRegularSeek(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string

		options []circular.OptionFunc

		expectOutOfSync bool
	}{
		{
			name: "no chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(65536),
				circular.WithSafetyGap(256),
			},

			expectOutOfSync: true,
		},
		{
			name: "chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(2048),
				circular.WithMaxCapacity(16384),
				circular.WithSafetyGap(256),
				circular.WithNumCompressedChunks(5, must.Value(zstd.NewCompressor())(t)),
			},
		},
		{
			name: "tiny chunks",

			options: []circular.OptionFunc{
				circular.WithInitialCapacity(256),
				circular.WithMaxCapacity(256),
				circular.WithSafetyGap(64),
				circular.WithNumCompressedChunks(6, must.Value(zstd.NewCompressor())(t)),
			},

			expectOutOfSync: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := require.New(t)
			asrt := assert.New(t)

			buf, err := circular.NewBuffer(test.options...)
			req.NoError(err)

			_, err = buf.Write(bytes.Repeat([]byte{0xff}, 512))
			req.NoError(err)

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 512))
			req.NoError(err)

			r := buf.GetReader()

			_, err = buf.Write(bytes.Repeat([]byte{0xfc}, 512))
			req.NoError(err)

			off, err := r.Seek(0, io.SeekCurrent)
			req.NoError(err)
			asrt.EqualValues(0, off)

			data := make([]byte, 256)

			n, err := r.Read(data)
			req.NoError(err)
			req.Equal(256, n)
			req.Equal(bytes.Repeat([]byte{0xff}, 256), data)

			off, err = r.Seek(0, io.SeekCurrent)
			req.NoError(err)
			asrt.EqualValues(256, off)

			off, err = r.Seek(-256, io.SeekEnd)
			req.NoError(err)
			asrt.EqualValues(768, off)

			n, err = r.Read(data)
			req.NoError(err)
			asrt.Equal(256, n)
			asrt.Equal(bytes.Repeat([]byte{0xfe}, 256), data)

			off, err = r.Seek(2048, io.SeekStart)
			req.NoError(err)
			asrt.EqualValues(1024, off)

			_, err = buf.Write(bytes.Repeat([]byte{0xfe}, 65536-256))
			req.NoError(err)

			off, err = r.Seek(0, io.SeekStart)
			req.NoError(err)
			asrt.EqualValues(0, off)

			_, err = r.Seek(-100, io.SeekStart)
			req.ErrorIs(err, circular.ErrSeekBeforeStart)

			singleByte := make([]byte, 1)
			_, err = r.Read(singleByte)

			if test.expectOutOfSync {
				req.ErrorIs(err, circular.ErrOutOfSync)
			} else {
				req.NoError(err)
				req.EqualValues(0xff, singleByte[0])
			}
		})
	}
}
