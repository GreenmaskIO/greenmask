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
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/utils"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
)

type BlobsRestorer struct {
	Entry    *toc.Entry
	St       storages.Storager
	usePgzip bool
	buf      []byte
}

func NewBlobsRestorer(entry *toc.Entry, st storages.Storager, usePgzip bool) *BlobsRestorer {
	return &BlobsRestorer{
		Entry:    entry,
		St:       st,
		usePgzip: usePgzip,
		buf:      make([]byte, defaultBufferSize),
	}
}

// Execute - restore large objects from the storage
func (br *BlobsRestorer) Execute(ctx context.Context, conn utils.PGConnector) error {
	// Getting blobs.toc
	loOids, err := br.getBlobsOIds(ctx)
	if err != nil {
		return fmt.Errorf("get blobs oids: %w", err)
	}

	for _, loOid := range loOids {
		log.Debug().
			Uint32("oid", loOid).
			Msg("large object restoration is started")

		if err := br.restoreLargeObject(ctx, conn, loOid); err != nil {
			return fmt.Errorf("restore large object %d: %w", loOid, err)
		}

		log.Debug().
			Uint32("oid", loOid).
			Msg("large object restoration is complete")
	}

	return nil
}

// getBlobsOIds - get all large objects oids from blobs.toc from the storage
func (br *BlobsRestorer) getBlobsOIds(ctx context.Context) ([]uint32, error) {
	reader, err := br.St.GetObject(ctx, "blobs.toc")
	if err != nil {
		return nil, fmt.Errorf("getting blobs.toc: %w", err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Warn().Err(err).Msg("error closing blobs.toc")
		}
	}()
	var loOids []uint32
	r := bufio.NewReader(reader)
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("readling line from blobs.toc: %w", err)
		}
		loOid := strings.Split(string(line), " ")[0]
		oid, err := strconv.ParseInt(loOid, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse oid %s from blobs.toc: %w", loOid, err)
		}
		loOids = append(loOids, uint32(oid))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	return loOids, nil
}

// restoreLargeObject - restore large object by oid in single transaction
// Each large object is restored in a separate transaction.
func (br *BlobsRestorer) restoreLargeObject(ctx context.Context, conn utils.PGConnector, oid uint32) error {
	loObj, err := br.getLargeObjectDataReader(ctx, oid)
	if err != nil {
		return fmt.Errorf("get large object reader: %w", err)
	}
	defer func() {
		if err := loObj.Close(); err != nil {
			log.Warn().
				Uint32("oid", oid).
				Err(err).
				Msg("error closing large object reader")
		}
	}()
	err = conn.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := br.restoreLargeObjectData(ctx, tx, loObj, oid); err != nil {
			return fmt.Errorf("restore large object %d: %w", oid, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("restore large object %d: %w", oid, err)
	}
	return nil
}

// getLargeObjectDataReader - get reader for large object by oid
func (br *BlobsRestorer) getLargeObjectDataReader(ctx context.Context, oid uint32) (io.ReadCloser, error) {
	loReader, err := br.St.GetObject(ctx, fmt.Sprintf("blob_%d.dat.gz", oid))
	if err != nil {
		return nil, fmt.Errorf("error getting object %s: %w", fmt.Sprintf("blob_%d.dat.gz", oid), err)
	}
	gz, err := ioutils.GetGzipReadCloser(loReader, br.usePgzip)
	if err != nil {
		_ = loReader.Close()
		return nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}
	return gz, nil
}

// restoreLargeObjectData - restore large object data by oid by given reader withing transaction
func (br *BlobsRestorer) restoreLargeObjectData(ctx context.Context, tx pgx.Tx, loObj io.Reader, oid uint32) error {
	loAPI := tx.LargeObjects()
	lo, err := loAPI.Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		return fmt.Errorf("unable to open large object %d: %w", oid, err)
	}
	defer func() {
		if err := lo.Close(); err != nil {
			log.Warn().
				Err(err).
				Uint32("oid", oid).
				Msg("error closing large object")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := loObj.Read(br.buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error readimg from table dump: %w", err)
		}
		_, err = lo.Write(br.buf[:n])
		if err != nil {
			return fmt.Errorf("error writing large object %d: %w", oid, err)
		}
	}

	return nil
}

func (br *BlobsRestorer) GetEntry() *toc.Entry {
	return br.Entry
}

func (br *BlobsRestorer) DebugInfo() string {
	return fmt.Sprintf("blobs %s", *br.Entry.Tag)
}
