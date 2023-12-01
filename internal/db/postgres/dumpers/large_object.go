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

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/countwriter"
)

const loBufSize = 1024 * 1024

type BlobsDumper struct {
	Blobs          *dump.Blobs
	OriginalSize   int64
	CompressedSize int64
}

func NewLargeObjectDumper(blobs *dump.Blobs) *BlobsDumper {
	return &BlobsDumper{
		Blobs: blobs,
	}
}

func (lod *BlobsDumper) Execute(ctx context.Context, tx pgx.Tx, st storages.Storager) (dump.Entry, error) {

	for _, lo := range lod.Blobs.LargeObjects {
		eg, gtx := errgroup.WithContext(ctx)
		log.Debug().
			Uint32("oid", uint32(lo.Oid)).
			Msg("dumping large object")

		w, r := countwriter.NewGzipPipe()

		// Writing goroutine
		eg.Go(
			func() error {
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
				err := st.PutObject(gtx, fmt.Sprintf("blob_%d.dat.gz", lo.Oid), r)
				if err != nil {
					return fmt.Errorf("cannot write large object %d object: %w", lo.Oid, err)
				}
				return nil
			},
		)

		// Dumping goroutine
		eg.Go(
			func() error {
				defer func() {
					log.Debug().
						Uint32("oid", uint32(lo.Oid)).
						Msg("closing writer")
					if err := w.Close(); err != nil {
						log.Warn().Err(err).Msg("error closing Blobs writer")
					}
				}()
				buf := make([]byte, loBufSize)
				los := tx.LargeObjects()
				lo, err := los.Open(ctx, uint32(lo.Oid), pgx.LargeObjectModeRead)
				defer func() {
					if err := lo.Close(); err != nil {
						log.Warn().Err(err).Msg("error closing large object")
					}
				}()
				if err != nil {
					return fmt.Errorf("error dumping large object %d: %w", lo, err)
				}
				var done bool
				for !done {
					size, err := lo.Read(buf)
					if err != nil {
						if errors.Is(err, io.EOF) {
							buf = buf[:size]
							done = true
						} else {
							return fmt.Errorf("error reading large object %d: %w", lo, err)
						}
					}
					if _, err = w.Write(buf); err != nil {
						return fmt.Errorf("error writing large object %d into storage: %w", lo, err)
					}
				}
				return nil
			},
		)

		if err := eg.Wait(); err != nil {
			return nil, err
		}

		lod.OriginalSize += w.GetCount()
		lod.CompressedSize += r.GetCount()

		log.Debug().
			Uint32("oid", uint32(lo.Oid)).
			Msg("dumping large object completed")
	}

	// Writing blobs.toc
	blobsTocBuf := bytes.NewBuffer(nil)

	for _, lo := range lod.Blobs.LargeObjects {
		blobsTocBuf.Write([]byte(fmt.Sprintf("%d blob_%d.dat\n", lo.Oid, lo.Oid)))
	}

	err := st.PutObject(ctx, "blobs.toc", blobsTocBuf)
	if err != nil {
		return nil, fmt.Errorf("cannot write large object blobs.toc: %w", err)
	}

	return lod.Blobs, nil
}

func (lod *BlobsDumper) DebugInfo() string {
	var largeObjects []string
	for _, lo := range lod.Blobs.LargeObjects {
		largeObjects = append(largeObjects, fmt.Sprintf("%d", lo.Oid))
	}
	return fmt.Sprintf("large objects dumping %s", strings.Join(largeObjects, ", "))
}
