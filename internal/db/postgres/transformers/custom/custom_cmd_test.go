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
