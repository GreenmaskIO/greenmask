package utils

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type SkipTransformationFunc func(r *toolkit.Record) bool
type SkipAttrFunc func(idx int) bool

type JsonApi struct {
	attributeNames   map[int]string
	attributeIdxs    []int
	tupleLength      int
	readCh           chan struct{}
	writeCh          chan struct{}
	w                io.Writer
	r                io.Reader
	encoder          *jsoniter.Encoder
	decoder          *jsoniter.Decoder
	skipOriginalData SkipAttrFunc
	timeout          time.Duration
	t                *time.Ticker
}

func NewJsonApi(
	timeout time.Duration, driver *toolkit.Driver,
	skipOriginalData SkipAttrFunc, attributes ...string) (*JsonApi, error) {
	attributeIdxs, attributeNames, err := GetAffectedAttributes(driver, attributes...)
	if err != nil {
		return nil, err
	}

	return &JsonApi{
		attributeNames:   attributeNames,
		attributeIdxs:    attributeIdxs,
		tupleLength:      len(attributeIdxs),
		readCh:           make(chan struct{}, 1),
		writeCh:          make(chan struct{}, 1),
		timeout:          timeout,
		t:                time.NewTicker(timeout),
		skipOriginalData: skipOriginalData,
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
	res := make(toolkit.RawRecord, j.tupleLength)
	for _, columnIdx := range j.attributeIdxs {
		if !j.skipOriginalData(columnIdx) {
			v, err := r.GetRawAttributeValueByIdx(columnIdx)
			if err != nil {
				return nil, fmt.Errorf("error getting raw atribute value: %w", err)
			}
			res[columnIdx] = v
		}
	}
	return &res, nil
}

func (j *JsonApi) SetRowDriverToRecord(rd toolkit.RowDriver, r *toolkit.Record) error {
	for _, columnIdx := range j.attributeIdxs {
		v, err := rd.GetColumn(columnIdx)
		if err != nil {
			return fmt.Errorf(`error getting column "%s" value: %w`, j.attributeNames[columnIdx], err)
		}
		err = r.SetRawAttributeValueByIdx(columnIdx, v)
		if err != nil {
			return fmt.Errorf(`error setting column "%s" value to Record: %w`, j.attributeNames[columnIdx], err)
		}
	}
	return nil
}

func (j *JsonApi) Encode(ctx context.Context, row toolkit.RowDriver) (err error) {
	j.t.Reset(j.timeout)
	go func() {
		if row.Length() == 0 {
			_, err = j.w.Write([]byte{'\n'})
		} else {
			err = j.encoder.Encode(row)
		}
		j.writeCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-j.t.C:
		return ErrInteractionTimeout
	case <-j.writeCh:
	}

	if err != nil {
		return err
	}

	return nil
}

func (j *JsonApi) Decode(ctx context.Context) (toolkit.RowDriver, error) {
	var err error
	row := make(toolkit.RawRecord, j.tupleLength)
	go func() {
		err = j.decoder.Decode(&row)

		j.writeCh <- struct{}{}
	}()
	j.t.Reset(j.timeout)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-j.t.C:
		return nil, ErrInteractionTimeout
	case <-j.writeCh:
	}

	if err != nil {
		return nil, err
	}

	return &row, nil
}
