package plan

type Node interface {
	Node()
}

type Column string

type Select struct {
	From    string
	Columns []string
	Join    []Join
}

func (s Select) Node() {}

type JoinTable struct {
	TableName string
	Columns   []Column
}

type Join struct {
	Lhs JoinTable
	Rhs JoinTable
}

func (j Join) Node() {}

type Condition struct {
	Lhs Node
	Rhs Node
}

type Where struct {
	Conditions []Condition
}

func (w Where) Node() {}

type SubQuery struct {
}

func (s SubQuery) Node() {}
