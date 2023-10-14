package utils

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type SkipTransformationFunc func(r *toolkit.Record) bool
type SkipAttrFunc func(idx int) bool

type JsonApi struct {
	attributeNames     map[int]string
	attributeIdxs      []int
	tupleLength        int
	readCh             chan struct{}
	writeCh            chan struct{}
	encoder            *jsoniter.Encoder
	decoder            *jsoniter.Decoder
	skipTransformation SkipTransformationFunc
	timeout            time.Duration
	t                  *time.Ticker
}

func NewJsonInteractionApi(
	timeout time.Duration, driver *toolkit.Driver,
	skipTransformation SkipTransformationFunc, skipAttr SkipAttrFunc,
	attributes ...string) (*JsonApi, error) {
	attributeIdxs, attributeNames, err := GetAffectedAttributes(driver, skipAttr, attributes...)
	if err != nil {
		return nil, err
	}
	if skipTransformation == nil {
		panic("skipTransformation is nil")
	}

	return &JsonApi{
		attributeNames:     attributeNames,
		attributeIdxs:      attributeIdxs,
		tupleLength:        len(attributeIdxs),
		readCh:             make(chan struct{}, 1),
		writeCh:            make(chan struct{}, 1),
		skipTransformation: skipTransformation,
		timeout:            timeout,
		t:                  time.NewTicker(timeout),
	}, nil
}

func (j *JsonApi) SetWriter(w io.Writer) {
	j.encoder = json.NewEncoder(w)
}

func (j *JsonApi) SetReader(r io.Reader) {
	j.decoder = json.NewDecoder(r)
}

func (j *JsonApi) SkipTransformation(r *toolkit.Record) bool {
	return j.skipTransformation(r)
}

func (j *JsonApi) GetRowDriverFromRecord(r *toolkit.Record) (toolkit.RowDriver, error) {
	res := make(toolkit.RawRecord, j.tupleLength)
	for _, columnIdx := range j.attributeIdxs {
		v, err := r.GetRawAttributeValueByIdx(columnIdx)
		if err != nil {
			return nil, fmt.Errorf("error getting raw atribute value: %w", err)
		}
		res[columnIdx] = v
	}
	return res, nil
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
		err = j.encoder.Encode(row)
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
		buf := j.decoder.Buffered()
		res, _ := io.ReadAll(buf)
		log.Debug().Msg(string(res))
		return nil, err
	}

	return row, nil
}
