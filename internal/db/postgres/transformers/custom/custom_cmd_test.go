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

package custom

//func TestCustomTransformer_Transform(t *testing.T) {
//	setting := utils.NewTransformerSettings().
//		SetTransformationType(domains.TupleTransformation).
//		SetName("test")
//
//	table := &data_objects.TableMeta{
//		Oid: 123,
//		Columns: []*domains2.Column{
//			{
//				Name: "test",
//				ColumnMeta: domains2.ColumnMeta{
//					TypeOid: pgtype.JSONBOID,
//				},
//			},
//		},
//	}
//
//	base, err := utils.NewTransformerBase(table, setting, nil, nil, nil)
//	require.NoError(t, err)
//
//	customTransformer := NewCustomTransformer(base, "/usr/bin/cat")
//
//	ctx, ctxCancel := context.WithCancel(context.Background())
//	defer ctxCancel()
//	tCancel, err := customTransformer.InitTransformation(ctx)
//	require.NoError(t, err)
//	defer tCancel()
//
//	res, err := customTransformer.Transform([]byte("test1\ttest2\n"))
//	log.Debug().Str("data", string(res)).Msg("received result")
//	require.NoError(t, err)
//	require.Equal(t, []byte("test1\ttest2"), res)
//
//	res, err = customTransformer.Transform([]byte("test3\ttest4\n"))
//	log.Debug().Str("data", string(res)).Msg("received result")
//	require.NoError(t, err)
//	require.Equal(t, []byte("test3\ttest4"), res)
//}
//
//func TestCustomTransformer_Validate(t *testing.T) {
//	setting := utils.NewTransformerSettings().
//		SetTransformationType(domains.TupleTransformation).
//		SetName("test")
//
//	table := &data_objects.TableMeta{
//		Oid: 123,
//		Columns: []*domains2.Column{
//			{
//				Name: "test",
//				ColumnMeta: domains2.ColumnMeta{
//					TypeOid: pgtype.JSONBOID,
//				},
//			},
//		},
//	}
//
//	base, err := utils.NewTransformerBase(table, setting, nil, nil, nil)
//	require.NoError(t, err)
//
//	customTransformer, _ := NewCustomTransformer(context.Background(), base, "/usr/bin/bash", "-c", "echo 1")
//
//	ctx, ctxCancel := context.WithCancel(context.Background())
//	defer ctxCancel()
//	warnings, err := customTransformer.Validate(ctx)
//	assert.NoError(t, err)
//	assert.Empty(t, warnings)
//}

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func newTestCmdTransformer(t *testing.T, ctd *TransformerDefinition) *CmdTransformer {
	table := &toolkit.Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*toolkit.Column{
			{Name: "id", TypeName: "int4", TypeOid: pgtype.Int4OID, Num: 1, Length: -1},
		},
	}
	driver, _, err := toolkit.NewDriver(table, nil)
	require.NoError(t, err)

	columnDef := toolkit.MustNewParameterDefinition("column", "column name").
		SetIsColumn(toolkit.NewColumnProperties().SetAffected(true))
	columnParam := toolkit.NewStaticParameter(columnDef, driver, false)
	warnings, err := columnParam.Init(nil, toolkit.ParamsValue("id"))
	require.NoError(t, err)
	require.Empty(t, warnings)

	ct, warnings, err := NewCustomCmdTransformer(
		context.Background(), driver, map[string]toolkit.Parameterizer{"column": columnParam}, ctd,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)
	return ct
}

func TestCmdTransformer_Done_expected_non_zero_exit_code(t *testing.T) {
	ct := newTestCmdTransformer(t, &TransformerDefinition{
		Name:       "test",
		Executable: "sh",
		// ignore SIGTERM, report readiness and exit with the expected code once stdin is closed
		Args:                     []string{"-c", "trap '' TERM; read meta; echo ready; cat >/dev/null; exit 3"},
		ExpectedExitCode:         3,
		RowTransformationTimeout: time.Second,
		Driver:                   &toolkit.DriverParams{Name: toolkit.CsvModeName},
	})
	require.NoError(t, ct.Init(context.Background()))
	line, err := ct.ReceiveStdoutLine(context.Background())
	require.NoError(t, err)
	require.Equal(t, "ready", string(line))
	require.NoError(t, ct.Done(context.Background()))
}

func TestCmdTransformer_Done_reaps_process_after_failed_Init(t *testing.T) {
	ct := newTestCmdTransformer(t, &TransformerDefinition{
		Name:                     "test",
		Executable:               "sh",
		RowTransformationTimeout: time.Second,
		Driver:                   &toolkit.DriverParams{Name: toolkit.CsvModeName},
	})
	// the process is started but Init fails before the wait group is created, e.g. on sending metadata
	require.NoError(t, ct.BaseInit("sh", []string{"-c", "cat >/dev/null"}))
	require.NoError(t, ct.Done(context.Background()))
	require.NotNil(t, ct.Cmd.ProcessState)
}
