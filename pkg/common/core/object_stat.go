package core

import "time"

type StorageObjectStat struct {
	Name         string
	LastModified time.Time
	Exist        bool
}
