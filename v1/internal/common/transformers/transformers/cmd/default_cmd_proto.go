package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

var (
	errCommonInteractorAlreadyInit = fmt.Errorf("common interactor already initialized")
	errCommonInteractorReaderIsNil = fmt.Errorf("common interactor reader is nil")
	errCommonInteractorWriterIsNil = fmt.Errorf("common interactor writer is nil")
)

type DefaultCMDProto struct {
	rowDriver           CMDRowDriver
	r                   utils.ContextReader
	w                   utils.ContextWriter
	scanner             *bufio.Scanner
	transferringColumns []*commonmodels.Column
	affectedColumns     []*commonmodels.Column
}

func NewDefaultCMDProto(
	rowDriver CMDRowDriver,
	transferringColumns []*commonmodels.Column,
	affectedColumns []*commonmodels.Column,
) *DefaultCMDProto {
	return &DefaultCMDProto{
		rowDriver:           rowDriver,
		transferringColumns: transferringColumns,
		affectedColumns:     affectedColumns,
	}
}

func (i *DefaultCMDProto) Init(w io.Writer, r io.Reader) error {
	if i.w != nil || i.r != nil {
		return errCommonInteractorAlreadyInit
	}
	if r == nil {
		return errCommonInteractorReaderIsNil
	}
	if w == nil {
		return errCommonInteractorWriterIsNil
	}
	i.w = utils.NewDefaultContextWriter(w)
	i.r = utils.NewDefaultContextReader(r)
	i.scanner = bufio.NewScanner(i.r)
	return nil
}

func (i *DefaultCMDProto) createDTO(r commonininterfaces.Recorder) (CMDRowDriver, error) {
	for _, c := range i.transferringColumns {
		v, err := r.GetRawColumnValueByIdx(c.Idx)
		if err != nil {
			return nil, fmt.Errorf("error getting raw atribute value: %w", err)
		}
		if err = i.rowDriver.SetColumn(c, v); err != nil {
			return nil, fmt.Errorf("unable to set new value: %w", err)
		}
	}
	return i.rowDriver, nil
}

func (i *DefaultCMDProto) applyReceivedDTO(rd CMDRowDriver, r commonininterfaces.Recorder) error {
	for _, c := range i.affectedColumns {
		v, err := rd.GetColumn(c)
		if err != nil {
			return fmt.Errorf(`get column %d value: %w`, c.Idx, err)
		}
		err = r.SetRawColumnValueByIdx(c.Idx, v)
		if err != nil {
			return fmt.Errorf(`set column %d value to record: %w`, c.Idx, err)
		}
	}
	return nil
}

func (i *DefaultCMDProto) Send(ctx context.Context, r commonininterfaces.Recorder) error {
	obj, err := i.createDTO(r)
	if err != nil {
		return fmt.Errorf("create DTO: %w", err)
	}
	data, err := obj.Encode()
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}
	data = append(data, '\n')
	_, err = i.w.WithContext(ctx).Write(data)
	if err != nil {
		return fmt.Errorf("write transferRecord: %w", err)
	}
	return nil
}

func (i *DefaultCMDProto) ReceiveAndApply(ctx context.Context, r commonininterfaces.Recorder) error {
	// First set the context to the writer that is used in the scanner.
	i.r.WithContext(ctx)
	lineScanned := i.scanner.Scan()
	if !lineScanned {
		if err := i.scanner.Err(); err != nil {
			return fmt.Errorf("scan line: %w", err)
		}
		return io.EOF
	}
	line := i.scanner.Bytes()
	if err := i.rowDriver.Decode(line); err != nil {
		return fmt.Errorf("decode line: %w", err)
	}
	if err := i.applyReceivedDTO(i.rowDriver, r); err != nil {
		return fmt.Errorf("apply received DTO: %w", err)
	}
	return nil
}
