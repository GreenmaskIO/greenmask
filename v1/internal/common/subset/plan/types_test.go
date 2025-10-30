// Copyright 2025 Greenmask
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

package plan

import (
	"testing"

	"github.com/huandu/go-sqlbuilder"
)

func TestTest(t *testing.T) {
	//
	//     (
	//        "sales"."customer"."storeid" IS NULL 				-- if left table is null ok - we proceed
	//        OR "sales"."store"."businessentityid" IS NOT NULL 	-- if left table is not null then the right table must be not null too.
	//    )

	sb := sqlbuilder.SQLServer.NewSelectBuilder().Select("*").
		From("t")

	//whereClause := sqlbuilder.NewWhereClause()
	//sb.WhereClause = whereClause
	sb.Where(sb.And(sb.IsNull("sales.customer.storeid"), sb.IsNull("sales.customer.storeid1")))

	sq := sqlbuilder.Select("col1, col2").From("test")

	sb.Where(sb.In(sqlbuilder.TupleNames("fr_col1", "fk_col2"), sq))

	print(sb.String())

	//test := "my_column" // column name as string
	//cond := sqlbuilder.NewCond()
	//cond.IsNull(test)
	//print(cond.Var("my_column"))
	//
	//sb := sqlbuilder.NewSelectBuilder()
	//sb.Select("*").From("my_table").Where("")
	//
	//query, args := sb.Build()
	//fmt.Println("Query:", query)
	//fmt.Println("Args:", args)
	//
	//sb = sqlbuilder.Select("name", "level").
	//	From("users")
	//
	//group1 := []string{"id = 1", "id = 2"}
	//group2 := []string{"id = 3", "id = 4"}
	//orGroup := []string{"id is null", "id2 is not null"}

	//sb.Where(
	//	sqlbuilder.NewCond().And(group1...),
	//	sqlbuilder.NewCond().And(group2...),
	//	sqlbuilder.NewCond().Or(orGroup...),
	//)
	//
	//sb.Where()
	//print(sb.String())
}
