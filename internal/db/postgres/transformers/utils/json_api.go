package utils

import (
	"context"
	"fmt"
	"io"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type SkipTransformationFunc func(r *toolkit.Record) bool
type SkipAttrFunc func(idx int) bool

type JsonApi struct {
	transferringColumns []int
	affectedColumns     []int
	tupleLength         int
	w                   io.Writer
	r                   io.Reader
	encoder             *jsoniter.Encoder
	decoder             *jsoniter.Decoder
	skipOriginalData    SkipAttrFunc
	record              toolkit.RawRecord
}

func NewJsonApi(
	transferringColumns []int, affectedColumns []int,
) (*JsonApi, error) {

	return &JsonApi{
		transferringColumns: transferringColumns,
		affectedColumns:     affectedColumns,
		tupleLength:         len(transferringColumns),
	}, nil
}

func (j *JsonApi) SetWriter(w io.Writer) {
	j.w = w
	j.encoder = json.NewEncoder(w)
}

func (j *JsonApi) SetReader(r io.Reader) {
	j.r = r
	j.decoder = json.NewDecoder(r)
}

func (j *JsonApi) GetRowDriverFromRecord(r *toolkit.Record) (toolkit.RowDriver, error) {
	j.record.Clean()
	for _, columnIdx := range j.transferringColumns {

		v, err := r.GetRawAttributeValueByIdx(columnIdx)
		if err != nil {
			return nil, fmt.Errorf("error getting raw atribute value: %w", err)
		}
		j.record[columnIdx] = v
	}
	return &j.record, nil
}

func (j *JsonApi) SetRowDriverToRecord(rd toolkit.RowDriver, r *toolkit.Record) error {
	for _, columnIdx := range j.affectedColumns {
		v, err := rd.GetColumn(columnIdx)
		if err != nil {
			return fmt.Errorf(`error getting column %d value: %w`, columnIdx, err)
		}
		err = r.SetRawAttributeValueByIdx(columnIdx, v)
		if err != nil {
			return fmt.Errorf(`error setting column %d value to Record: %w`, columnIdx, err)
		}
	}
	return nil
}

func (j *JsonApi) Encode(ctx context.Context, row toolkit.RowDriver) (err error) {
	if row.Length() == 0 {
		_, err = j.w.Write([]byte{'\n'})
	} else {
		err = j.encoder.Encode(row)
	}

	if err != nil {
		return err
	}

	return nil
}

func (j *JsonApi) Decode(ctx context.Context) (toolkit.RowDriver, error) {
	var err error
	j.record.Clean()
	err = j.decoder.Decode(&j.record)

	if err != nil {
		return nil, err
	}

	return &j.record, nil
}
