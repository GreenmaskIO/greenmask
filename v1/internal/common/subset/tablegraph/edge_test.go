package tablegraph

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func TestNewEdge(t *testing.T) {
	edgeID := 2
	isNullable := true
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	actual := NewEdge(edgeID, isNullable, leftLink, rightLink)
	expected := Edge{
		id:         edgeID,
		isNullable: true,
		from:       leftLink,
		to:         rightLink,
	}
	require.Equal(t, expected, actual)
}

func TestEdge_From(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.From()
	require.Equal(t, leftLink, actual)
}

func TestEdge_To(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.To()
	require.Equal(t, rightLink, actual)
}

func TestEdge_IsNullable(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.IsNullable()
	require.Equal(t, true, actual)
}

func TestEdge_ID(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.ID()
	require.Equal(t, 2, actual)
}
