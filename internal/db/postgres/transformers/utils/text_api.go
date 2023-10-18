package utils

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type TextApi struct {
	attributeName    string
	columnIdx        int
	w                io.Writer
	r                io.Reader
	lineReader       *bufio.Reader
	skipOriginalData bool
	// record - allocated record for Encode - Decode operations
	record *toolkit.RawRecordText
	// emptyRecord - allocated static empty object in case wi just send \n
	emptyRecord *toolkit.RawRecordText
}

func NewTextApi(columnIdx int, skipOriginalData bool) (*TextApi, error) {
	return &TextApi{
		columnIdx:        columnIdx,
		skipOriginalData: skipOriginalData,
		record:           toolkit.NewRawRecordText(),
		emptyRecord:      toolkit.NewRawRecordText(),
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
	if j.skipOriginalData {
		return j.emptyRecord, nil
	}

	v, err := r.GetRawAttributeValueByIdx(j.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("error getting raw attribute: %w", err)
	}
	_ = j.record.Decode(v.Data)
	return j.record, nil
}

func (j *TextApi) SetRowDriverToRecord(rd toolkit.RowDriver, r *toolkit.Record) error {
	v, err := rd.GetColumn(j.columnIdx)
	if err != nil {
		return fmt.Errorf(`error getting column "%s" value: %w`, j.attributeName, err)
	}
	err = r.SetRawAttributeValueByIdx(j.columnIdx, v)
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

	if err = j.record.Decode(line); err != nil {
		return nil, fmt.Errorf("error decoding via text interaction API: %w", err)
	}

	return j.record, nil
}
