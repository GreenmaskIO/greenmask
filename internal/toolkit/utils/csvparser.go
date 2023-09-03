package utils

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

// TODO: It's not a production solution. Real copy parser must be backported.
// 	We have only two to solve it:
//		1. Fully backport PostgreSQL COPY TEXT format
//		2. Implement COPY using CSV format. I suspect it may cause escaping problems, but it is easier

type StreamDriver struct {
	r      *csv.Reader
	w      *csv.Writer
	driver *transformers.Driver
}

func NewStreamDriver(r io.Reader, w io.Writer, driver *transformers.Driver) *StreamDriver {
	if driver == nil {
		panic("received nil Driver pointer")
	}
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	return &StreamDriver{
		r:      cr,
		w:      csv.NewWriter(w),
		driver: driver,
	}
}

func (c *StreamDriver) Read() (*transformers.Record, error) {
	values, err := c.r.Read()
	if err != nil {
		return nil, fmt.Errorf("cannot read dump line: %w", err)
	}
	// TODO: You should not create always a new object instead you must re-use old buffer
	return transformers.NewRecord(c.driver, values), nil
}

func (c *StreamDriver) Write(r *transformers.Record) error {
	res, err := r.Encode()
	if err != nil {
		return fmt.Errorf("cannot encode record: %w", err)
	}
	if err := c.w.Write(res); err != nil {
		return fmt.Errorf("unnable to write line: %w", err)
	}
	c.w.Flush()
	return nil
}
