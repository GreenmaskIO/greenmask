package mysql

import "context"

type Restore struct {
}

func NewRestore() *Restore {
	return &Restore{}
}

func (r *Restore) Run(ctx context.Context) error {
	return nil
}
