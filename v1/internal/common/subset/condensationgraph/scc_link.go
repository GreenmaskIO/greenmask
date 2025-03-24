package condensationgraph

// Link - represents a link to the SCC in the condensation graph.
//
// It uses to represent the left and right parts of the edge in the condensed graph.
type Link struct {
	// sscID - index of the SSC in. Using this index the SCC can be identified
	// in the condensation graph or SSC list. Meaning this ID point to the left or right vertex in the edge.
	sscID int
	ssc   SCC
}

// NewLink - creates a new Link instance.
func NewLink(id int, c SCC) Link {
	return Link{
		sscID: id,
		ssc:   c,
	}
}
