package condensationgraph

// Link - represents a link to the SCC in the condensation Graph.
//
// It uses to represent the left and right parts of the edge in the condensed Graph.
type Link struct {
	// tableID - index of the SCC in. Using this index the SCC can be identified
	// in the condensation Graph or SCC list. Meaning this ID point to the left or right vertex in the edge.
	tableID int
	scc     SCC
}

// NewLink - creates a new Link instance.
func NewLink(tableID int, c SCC) Link {
	return Link{
		tableID: tableID,
		scc:     c,
	}
}

// SCCID - returns the ID of SCC.
func (l Link) SCCID() int {
	return l.scc.ID()
}

// TableID - return the table ID.
func (l Link) TableID() int {
	return l.tableID
}
