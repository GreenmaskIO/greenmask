package domains

import "time"

type ObjectStat struct {
	Name         string
	LastModified time.Time
	Exist        bool
}
