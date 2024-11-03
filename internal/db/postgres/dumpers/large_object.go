// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dumpers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/utils/ioutils"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/storages"
)

const loBufSize = 1024 * 1024

type BlobsDumper struct {
	Blobs          *entries.Blobs
	OriginalSize   int64
	CompressedSize int64
	usePgzip       bool
}

func NewLargeObjectDumper(blobs *entries.Blobs, usePgzip bool) *BlobsDumper {
	return &BlobsDumper{
		Blobs:    blobs,
		usePgzip: usePgzip,
	}
}

func (lod *BlobsDumper) Execute(ctx context.Context, tx pgx.Tx, st storages.Storager) error {

	for _, lo := range lod.Blobs.LargeObjects {
		eg, gtx := errgroup.WithContext(ctx)
		log.Debug().
			Uint32("oid", uint32(lo.Oid)).
			Msg("dumping large object")

		w, r := ioutils.NewGzipPipe(lod.usePgzip)

		// Writing goroutine
		eg.Go(largeObjectWriter(gtx, st, lo, r))

		// Dumping goroutine
		eg.Go(largeObjectDumper(gtx, lo, w, tx))

		if err := eg.Wait(); err != nil {
			return err
		}

		lod.OriginalSize += w.GetCount()
		lod.CompressedSize += r.GetCount()

		log.Debug().
			Uint32("oid", uint32(lo.Oid)).
			Msg("dumping large object completed")
	}

	// Writing blobs.toc
	if err := lod.generateBlobsToc(ctx, st); err != nil {
		return fmt.Errorf("cannot write large object blobs.toc: %w", err)
	}

	return nil
}

func (lod *BlobsDumper) generateBlobsToc(ctx context.Context, st storages.Storager) error {
	log.Debug().Msg("writing blobs.toc")
	// Writing blobs.toc
	blobsTocBuf := bytes.NewBuffer(nil)

	for _, lo := range lod.Blobs.LargeObjects {
		blobsTocBuf.Write([]byte(fmt.Sprintf("%d blob_%d.dat\n", lo.Oid, lo.Oid)))
	}

	err := st.PutObject(ctx, "blobs.toc", blobsTocBuf)
	if err != nil {
		return fmt.Errorf("cannot write large object blobs.toc: %w", err)
	}
	return nil
}

func largeObjectWriter(ctx context.Context, st storages.Storager, lo *entries.LargeObject, r ioutils.CountReadCloser) func() error {
	return func() error {
		defer func() {
			log.Debug().
				Uint32("oid", uint32(lo.Oid)).
				Msg("closing reader")
			if err := r.Close(); err != nil {
				log.Warn().
					Err(err).
					Uint32("oid", uint32(lo.Oid)).
					Msg("error closing LargeObject reader")
			}
		}()
		err := st.PutObject(ctx, fmt.Sprintf("blob_%d.dat.gz", lo.Oid), r)
		if err != nil {
			return fmt.Errorf("cannot write large object %d object: %w", lo.Oid, err)
		}
		return nil
	}
}

func largeObjectDumper(ctx context.Context, lo *entries.LargeObject, w ioutils.CountWriteCloser, tx pgx.Tx) func() error {
	return func() error {
		defer func() {
			log.Debug().
				Uint32("oid", uint32(lo.Oid)).
				Msg("closing writer")
			if err := w.Close(); err != nil {
				log.Warn().Err(err).Msg("error closing blobs writer")
			}
		}()
		buf := make([]byte, loBufSize)
		los := tx.LargeObjects()
		loObj, err := los.Open(ctx, uint32(lo.Oid), pgx.LargeObjectModeRead)
		if err != nil {
			return fmt.Errorf("error opening large object %d: %w", lo.Oid, err)
		}
		defer func() {
			if err := loObj.Close(); err != nil {
				log.Warn().Err(err).Msg("error closing large object")
			}
		}()
		if err != nil {
			return fmt.Errorf("error dumping large object %d: %w", lo.Oid, err)
		}
		var done bool
		for !done {
			size, err := loObj.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					buf = buf[:size]
					done = true
				} else {
					return fmt.Errorf("error reading large object %d: %w", lo.Oid, err)
				}
			}
			if _, err = w.Write(buf); err != nil {
				return fmt.Errorf("error writing large object %d into storage: %w", lo.Oid, err)
			}
		}
		return nil
	}
}

func (lod *BlobsDumper) DebugInfo() string {
	var largeObjects []string
	for _, lo := range lod.Blobs.LargeObjects {
		largeObjects = append(largeObjects, fmt.Sprintf("%d", lo.Oid))
	}
	return fmt.Sprintf("large objects dumping %s", strings.Join(largeObjects, ", "))
}
