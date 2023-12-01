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

package restorers

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
)

type BlobsRestorer struct {
	Entry            *toc.Entry
	St               storages.Storager
	largeObjectsOids []uint32
}

func NewBlobsRestorer(entry *toc.Entry, st storages.Storager) *BlobsRestorer {
	return &BlobsRestorer{
		Entry: entry,
		St:    st,
	}
}

func (td *BlobsRestorer) Execute(ctx context.Context, tx pgx.Tx) error {

	// Getting blobs.toc
	reader, err := td.St.GetObject(ctx, "blobs.toc")
	if err != nil {
		return fmt.Errorf("error getting blobs.toc: %w", err)
	}
	defer reader.Close()
	r := bufio.NewReader(reader)
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error readling line from blobs.toc: %w", err)
		}
		loOid := strings.Split(string(line), " ")[0]
		oid, err := strconv.ParseInt(loOid, 10, 32)
		if err != nil {
			return fmt.Errorf("unable to parse oid %s from blobs.toc: %w", loOid, err)
		}
		td.largeObjectsOids = append(td.largeObjectsOids, uint32(oid))
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	loApi := tx.LargeObjects()
	// restoring large objects one by one
	buf := make([]byte, DefaultBufferSize)
	for _, loOid := range td.largeObjectsOids {
		log.Debug().Uint32("oid", loOid).Msg("large object restoration is started")
		err = func() error {
			loReader, err := td.St.GetObject(ctx, fmt.Sprintf("blob_%d.dat.gz", loOid))
			if err != nil {
				return fmt.Errorf("error getting object %s: %w", fmt.Sprintf("blob_%d.dat.gz", loOid), err)
			}
			gz, err := gzip.NewReader(loReader)
			if err != nil {
				return fmt.Errorf("cannot create gzip reader: %w", err)
			}
			defer gz.Close()
			lo, err := loApi.Open(ctx, loOid, pgx.LargeObjectModeWrite)
			if err != nil {
				return fmt.Errorf("unable to open large object %d: %w", loOid, err)
			}
			defer func() {
				if err := lo.Close(); err != nil {
					log.Warn().
						Err(err).
						Uint32("oid", loOid).
						Msg("error closing large object")
				}
			}()

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			for {
				n, err := gz.Read(buf)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					return fmt.Errorf("error readimg from table dump: %w", err)
				}
				_, err = lo.Write(buf[:n])
				if err != nil {
					return fmt.Errorf("error writing large object %d: %w", loOid, err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			return nil
		}()
		if err != nil {
			return err
		}
		log.Debug().Uint32("oid", loOid).Msg("large object restoration is complete")
	}

	return nil
}

func (td *BlobsRestorer) DebugInfo() string {
	return fmt.Sprintf("blobs %s", *td.Entry.Tag)
}
