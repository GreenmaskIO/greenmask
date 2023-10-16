package utils

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type TextApi struct {
	attributeName    string
	attributeIdx     int
	readCh           chan struct{}
	writeCh          chan struct{}
	w                io.Writer
	r                io.Reader
	lineReader       *bufio.Reader
	skipOriginalData SkipAttrFunc
	timeout          time.Duration
	t                *time.Ticker
}

func NewTextApi(
	driver *toolkit.Driver,
	skipOriginalData SkipAttrFunc, attributes ...string) (*TextApi, error) {
	attributeIdxs, attributeNames, err := GetAffectedAttributes(driver, attributes...)
	if err != nil {
		return nil, err
	}
	if len(attributeIdxs) > 1 {
		return nil, fmt.Errorf("use another interaction format (json or csv): text intearaction formats supports only 1 attribute peer record got %d", len(attributeIdxs))
	}

	return &TextApi{
		attributeName:    attributeNames[0],
		attributeIdx:     attributeIdxs[0],
		readCh:           make(chan struct{}, 1),
		writeCh:          make(chan struct{}, 1),
		skipOriginalData: skipOriginalData,
	}, nil
}

func (j *TextApi) SetWriter(w io.Writer) {
	j.w = w
}

func (j *TextApi) SetReader(r io.Reader) {
	j.r = r
	j.lineReader = bufio.NewReader(r)
}

func (j *TextApi) GetRowDriverFromRecord(r *toolkit.Record) (toolkit.RowDriver, error) {
	rd := toolkit.NewRawRecordText()
	if j.skipOriginalData(j.attributeIdx) {
		return rd, nil
	}

	v, err := r.GetRawAttributeValueByIdx(j.attributeIdx)
	if err != nil {
		return nil, fmt.Errorf("error getting raw attribute: %w", err)
	}
	if v.IsNull {
		_ = rd.Decode(pgcopy.DefaultNullSeq)
	} else {
		_ = rd.Decode(v.Data)
	}
	return rd, nil
}

func (j *TextApi) SetRowDriverToRecord(rd toolkit.RowDriver, r *toolkit.Record) error {
	v, err := rd.GetColumn(j.attributeIdx)
	if err != nil {
		return fmt.Errorf(`error getting column "%s" value: %w`, j.attributeName, err)
	}
	err = r.SetRawAttributeValueByIdx(j.attributeIdx, v)
	if err != nil {
		return fmt.Errorf(`error setting column "%s" value to Record: %w`, j.attributeName, err)
	}
	return nil
}

func (j *TextApi) Encode(ctx context.Context, row toolkit.RowDriver) (err error) {
	data, err := row.Encode()
	if err != nil {
		return fmt.Errorf("error encodig row data via text interaction API: %w", err)
	}
	data = append(data, '\n')
	_, err = j.w.Write(data)

	if err != nil {
		return err
	}

	return nil
}

func (j *TextApi) Decode(ctx context.Context) (toolkit.RowDriver, error) {
	line, _, err := j.lineReader.ReadLine()
	if err != nil {
		return nil, err
	}
	row := toolkit.NewRawRecordText()

	if err = row.Decode(line); err != nil {
		return nil, fmt.Errorf("error decoding via text interaction API: %w", err)
	}

	return row, nil
}
