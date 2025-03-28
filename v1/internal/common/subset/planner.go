package subset

// The query planned based on the condesation sub graph
//
// Plan query
// There are at least three possible cases to plan a query
//  1. Query do not have any JOIN's (edges)
//  2. Query with simple JOIN's
//  3. One of the vertexes has SCC with cycle
//

// A few questions here:
// 1. How to determine the required planning algorithm?
// 2. Should I implement each planning algorithm separately (dedicated planning algorithm)?
// 3. Should I mix and implement planning algorithm in the same place?
//
//
// Possible options:
// * One vertex
// * Simple JOIN queries
// * Simple JOIN queries with ambiguous vertexes
// * SCC with cycle
//
// The algorithm types:
// 1. Simple query, tables aren't ambiguous
// 2. Simple query, tables are ambiguous
//		- Should we add an alias or implement it via WHERE IN query?
//			- I suspect it's easier to rewrite the where clause.
//			  Otherwise we will have to make a sub-graph.
//
//
// Looks like the best way to generate this is a AST-like structure
// But I still don't know how to dump the generated structure into sql statements
// Need to check how it's done in go parser

type node struct {
}
