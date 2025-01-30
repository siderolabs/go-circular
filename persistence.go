// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package circular

import (
	"cmp"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/siderolabs/gen/xslices"
	"go.uber.org/zap"
)

func (buf *Buffer) load() error {
	if buf.opt.PersistenceOptions.ChunkPath == "" {
		// persistence is disabled
		return nil
	}

	chunkPaths, err := filepath.Glob(buf.opt.PersistenceOptions.ChunkPath + ".*")
	if err != nil {
		return err
	}

	type parsedChunkPath struct {
		path string
		id   int64
	}

	parsedChunkPaths := make([]parsedChunkPath, 0, len(chunkPaths))

	for _, chunkPath := range chunkPaths {
		idx := strings.LastIndexByte(chunkPath, '.')
		if idx == -1 {
			continue
		}

		idStr := chunkPath[idx+1:]

		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id < 0 {
			continue
		}

		parsedChunkPaths = append(parsedChunkPaths, parsedChunkPath{
			id:   id,
			path: chunkPath,
		})
	}

	// sort chunks by ID, from smallest to biggest
	slices.SortFunc(parsedChunkPaths, func(a, b parsedChunkPath) int {
		return cmp.Compare(a.id, b.id)
	})

	idx := 0

	if len(parsedChunkPaths) > 1 && parsedChunkPaths[0].id == 0 {
		idx = 1
	}

	if len(parsedChunkPaths)-idx > buf.opt.NumCompressedChunks {
		for j := idx; j < len(parsedChunkPaths)-buf.opt.NumCompressedChunks; j++ {
			buf.opt.Logger.Warn("dropping chunk, as it is beyond the limit of available chunks", zap.String("path", parsedChunkPaths[j].path))

			if err := os.Remove(parsedChunkPaths[j].path); err != nil {
				buf.opt.Logger.Error("failed to remove chunk", zap.String("path", parsedChunkPaths[j].path), zap.Error(err))

				continue
			}
		}

		parsedChunkPaths = slices.Delete(parsedChunkPaths, idx, len(parsedChunkPaths)-buf.opt.NumCompressedChunks)
	}

	chunks := make([]chunk, 0, len(parsedChunkPaths))

	for _, chunkPath := range parsedChunkPaths {
		data, err := os.ReadFile(chunkPath.path)
		if err != nil {
			buf.opt.Logger.Error("failed to read chunk, skipping", zap.String("path", chunkPath.path), zap.Error(err))

			continue
		}

		if chunkPath.id == 0 {
			buf.data, err = buf.opt.Compressor.Decompress(data, buf.data[:0])
			if err != nil {
				buf.opt.Logger.Error("failed to decompress zero chunk, skipping", zap.String("path", chunkPath.path), zap.Error(err))

				buf.data = buf.data[:min(cap(buf.data), buf.opt.MaxCapacity)]

				continue
			}

			buf.off = int64(len(buf.data))

			buf.data = buf.data[:min(cap(buf.data), buf.opt.MaxCapacity)]
		} else {
			decompressedSize, err := buf.opt.Compressor.DecompressedSize(data)
			if err != nil {
				buf.opt.Logger.Error("failed to get size of compressed chunk, skipping", zap.String("path", chunkPath.path), zap.Error(err))

				continue
			}

			chunks = append(chunks,
				chunk{
					compressed: data,
					id:         chunkPath.id,
					size:       decompressedSize,
				})
		}
	}

	// re-calculate all offsets
	var sizeDecompressed int64

	for i := range chunks {
		sizeDecompressed += chunks[i].size
	}

	buf.opt.Logger.Debug("loaded buffer from disk",
		zap.Int("num_compressed_chunks", len(chunks)),
		zap.Int64("current_chunk_bytes", buf.off),
		zap.Int64("overall_decompressed_bytes", sizeDecompressed+buf.off),
		zap.Strings("chunk_paths", xslices.Map(parsedChunkPaths, func(c parsedChunkPath) string {
			return c.path
		})),
	)

	// if chunk sizes are [10, 30, 20], the offsets will be [-60, -50, -20].
	// the current buffer starts at 0 and goes to b.off (size of the buffer).
	for i := range chunks {
		chunks[i].startOffset = -sizeDecompressed
		sizeDecompressed -= chunks[i].size
	}

	buf.chunks = chunks

	return nil
}

func (buf *Buffer) run() {
	if buf.opt.PersistenceOptions.ChunkPath == "" {
		// persistence is disabled
		return
	}

	buf.commandCh = make(chan persistenceCommand, 8)
	startOffset := buf.off

	buf.wg.Add(1)

	go func() {
		defer buf.wg.Done()
		buf.runPersistence(buf.commandCh, startOffset)
	}()
}

type persistenceCommand struct {
	data []byte

	chunkID int64
	drop    bool
}

func (buf *Buffer) chunkPath(chunkID int64) string {
	return buf.opt.PersistenceOptions.ChunkPath + "." + strconv.FormatInt(chunkID, 10)
}

//nolint:gocognit
func (buf *Buffer) runPersistence(ch <-chan persistenceCommand, lastPersistedOffset int64) {
	var (
		timerC <-chan time.Time
		timer  *time.Timer
	)

	defer func() {
		if timer == nil {
			return
		}

		if !timer.Stop() {
			<-timer.C
		}
	}()

	setTimer := func() {
		interval := buf.opt.PersistenceOptions.NextInterval()

		if timer == nil {
			timer = time.NewTimer(interval)
			timerC = timer.C
		} else {
			timer.Reset(interval)
		}
	}

	if buf.opt.PersistenceOptions.FlushInterval > 0 {
		setTimer()
	}

	chunkPath0 := buf.chunkPath(0)

persistLoop:
	for {
		select {
		case command, ok := <-ch:
			if !ok {
				break persistLoop
			}

			chunkPath := buf.chunkPath(command.chunkID)

			if command.drop {
				if err := os.Remove(chunkPath); err != nil {
					buf.opt.Logger.Error("failed to remove chunk", zap.String("path", chunkPath), zap.Error(err))
				} else {
					buf.opt.Logger.Debug("dropped old chunk", zap.String("path", chunkPath))
				}
			} else {
				if err := atomicWriteFile(chunkPath, command.data, 0o644); err != nil {
					buf.opt.Logger.Error("failed to write compressed chunk", zap.String("path", chunkPath), zap.Error(err))
				} else {
					buf.opt.Logger.Debug("persisted rotated chunk", zap.String("path", chunkPath))
				}

				// as the new chunk was just created, the .0 chunk should be removed now, as its contents were compressed away
				if err := os.Remove(chunkPath0); err != nil {
					buf.opt.Logger.Error("failed to remove chunk", zap.String("path", chunkPath0), zap.Error(err))
				} else {
					buf.opt.Logger.Debug("dropped current chunk, as it was rotated", zap.String("path", chunkPath0))
				}
			}
		case <-timerC:
			// persist current chunk if changed
			currentOffset := buf.Offset()

			if persisted, err := buf.persistCurrentChunk(currentOffset, lastPersistedOffset, chunkPath0); err != nil {
				buf.opt.Logger.Error("failed to persist current chunk on timer", zap.Error(err), zap.String("path", chunkPath0))
			} else if persisted {
				lastPersistedOffset = currentOffset

				buf.opt.Logger.Debug("persisted current chunk on timer", zap.Int64("offset", currentOffset), zap.String("path", chunkPath0))
			}

			setTimer()
		}
	}

	// command channel is closed, persist the current chunk
	if persisted, err := buf.persistCurrentChunk(buf.Offset(), lastPersistedOffset, chunkPath0); err != nil {
		buf.opt.Logger.Error("failed to persist current chunk on close", zap.Error(err), zap.String("path", chunkPath0))
	} else if persisted {
		buf.opt.Logger.Debug("persisted current chunk on close", zap.Int64("offset", buf.Offset()), zap.String("path", chunkPath0))
	}
}

func (buf *Buffer) persistCurrentChunk(currentOffset, lastPersistedOffset int64, chunkPath0 string) (persisted bool, err error) {
	if currentOffset == lastPersistedOffset {
		return false, nil
	}

	buf.mu.Lock()
	data := slices.Clone(buf.data[:currentOffset%int64(buf.capacity())])
	buf.mu.Unlock()

	compressed, err := buf.opt.Compressor.Compress(data, nil)
	if err != nil {
		return false, fmt.Errorf("failed to compress current chunk: %w", err)
	}

	if err = atomicWriteFile(chunkPath0, compressed, 0o644); err != nil {
		return false, fmt.Errorf("failed to write compressed chunk: %w", err)
	}

	return true, nil
}

func atomicWriteFile(path string, data []byte, mode fs.FileMode) error {
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, mode); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath) //nolint:errcheck

		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}
