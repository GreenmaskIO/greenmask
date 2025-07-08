package dumper

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/tableruntime"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const defaultDialerTimeout = 15 * time.Second

type MySQLConnectionConfig struct {
	Address  string
	User     string
	Password string
	DbName   string
	Timeout  time.Duration
}

type TableDumper struct {
	tableRuntime *tableruntime.TableRuntime
	config       *MySQLConnectionConfig
	usePgzip     bool
}

func NewTableDumper(table *tableruntime.TableRuntime, config *MySQLConnectionConfig, usePgzip bool) *TableDumper {
	return &TableDumper{
		tableRuntime: table,
		config:       config,
		usePgzip:     usePgzip,
	}
}

func getFileName(table *commonmodels.Table) string {
	return fmt.Sprintf("%s.%s.csv.gz", table.Schema, table.Name)
}

func (t *TableDumper) Dump(ctx context.Context, st storages.Storager) (commonmodels.DumpStat, error) {
	// Open transaction
	dialerTimout := t.config.Timeout
	if dialerTimout <= 0 {
		dialerTimout = defaultDialerTimeout
	}
	conn, err := client.ConnectWithContext(
		ctx, t.config.Address, t.config.User, t.config.Password, t.config.DbName, dialerTimout,
	)
	if err != nil {
		return commonmodels.DumpStat{}, err
	}
	defer func() { _ = conn.Close() }()

	cw, cr := utils.NewGzipPipe(t.usePgzip)

	var result mysql.Result
	err = conn.ExecuteSelectStreaming(t.tableRuntime.Query, &result, func(row []mysql.FieldValue) error {
		for idx, val := range row {
			col := result.Fields[idx].Name
		}
		return nil
	}, nil)
	if err != nil {
		return commonmodels.DumpStat{}, fmt.Errorf("execute select streaming: %w", err)
	}
	// lod.OriginalSize += w.GetCount()
	//		lod.CompressedSize += r.GetCount()
	return commonmodels.DumpStat{
		Size:           cw.GetCount(),
		CompressedSize: cw.GetCount(),
	}, nil
}

func (t *TableDumper) storageWriter(ctx context.Context, st storages.Storager, r io.ReadCloser) func() error {
	return func() error {
		defer func() {
			if err := r.Close(); err != nil {
				log.Ctx(ctx).
					Err(err).
					Msg("error on closing TableDumper reader")
			}
		}()
		fileName := getFileName(t.tableRuntime.Table)
		if err := st.PutObject(ctx, fileName, r); err != nil {
			return fmt.Errorf("cannot write object: %w", err)
		}
		return nil
	}
}

// dumper - dumps the data from the table and transform it if needed
func (td *TableDumper) dataDumper(
	ctx context.Context, eg *errgroup.Group, w io.WriteCloser, conn *client.Conn,
) func() error {
	return func() error {
		var pipeline Pipeliner
		var err error
		if len(td.table.TransformersContext) > 0 {
			if td.validate {
				pipeline, err = NewValidationPipeline(ctx, eg, td.table, w)
				if err != nil {
					return fmt.Errorf("cannot initialize validation pipeline: %w", err)
				}
			} else {
				pipeline, err = NewTransformationPipeline(ctx, eg, td.table, w)
				if err != nil {
					return fmt.Errorf("cannot initialize transformation pipeline: %w", err)
				}
			}

		} else {
			pipeline = NewPlainDumpPipeline(td.table, w)
		}
		if err := pipeline.Init(ctx); err != nil {
			return fmt.Errorf("error initializing transformation pipeline: %w", err)
		}
		if err := td.process(ctx, tx, w, pipeline); err != nil {
			doneErr := pipeline.Done(ctx)
			if doneErr != nil {
				log.Warn().Err(err).Msg("error terminating transformation pipeline")
			}
			return fmt.Errorf("error processing table dump %s.%s: %w", td.table.Schema, td.table.Name, err)
		}
		log.Debug().Msg("transformation pipeline executed successfully")
		return pipeline.Done(ctx)
	}
}

func (t *TableDumper) DebugInfo() string {
	//TODO implement me
	panic("implement me")
}
