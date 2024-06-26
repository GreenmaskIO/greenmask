package dumpers

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/rs/zerolog/log"
)

func storeCycleResolutionOps(r *toolkit.Record, storeOps []*entries.CycleResolutionOp, files []io.ReadWriteCloser) error {
	for idx := 0; idx < len(storeOps); idx++ {
		storeOp := storeOps[idx]
		file := files[idx]
		row := pgcopy.NewRow(len(storeOp.Columns))
		var hasNull bool
		for storeColIdx, col := range storeOp.Columns {
			columnIdx, _, ok := r.Driver.GetColumnByName(col)
			if !ok {
				return fmt.Errorf("column %s not found in record", col)
			}
			rawValue, err := r.Row.GetColumn(columnIdx)
			if err != nil {
				return fmt.Errorf("error getting column value: %w", err)
			}
			if rawValue.IsNull {
				hasNull = true
			}
			if err = row.SetColumn(storeColIdx, rawValue); err != nil {
				return fmt.Errorf("error setting column value: %w", err)
			}
		}

		if hasNull {
			continue
		}

		res, err := row.Encode()
		if err != nil {
			return fmt.Errorf("error encoding row: %w", err)
		}
		if _, err = file.Write(res); err != nil {
			return fmt.Errorf("error writing row: %w", err)
		}
		if _, err = file.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("error writing row: %w", err)
		}
	}
	return nil
}

func closeAllOpenFiles(files []io.ReadWriteCloser, cycleResolutionOps []*entries.CycleResolutionOp, remove bool) {
	for cleanIdx, cleanOp := range cycleResolutionOps {
		f := files[cleanIdx]
		if f != nil {
			log.Debug().Str("file", cleanOp.FileName).Msg("closing cycle resolution store file")
			if err := f.Close(); err != nil {
				log.Warn().Err(err).Msg("error closing cycle resolution store file")
			}
			if remove {
				if err := os.Remove(path.Join(tmpFilePath, cleanOp.FileName)); err != nil {
					log.Warn().Err(err).Msg("error removing cycle resolution store file")
				}
			}
		}
	}
}
