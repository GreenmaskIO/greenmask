package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDumpOptions_Validate(t *testing.T) {
	t.Run("cap max-insert-statement-size by max-allowed-packet", func(t *testing.T) {
		opts := DumpOptions{
			InsertBatchSize: 100,
		}
		opts.MaxAllowedPacket = 1024
		opts.MaxInsertStatementSize = 4096

		err := opts.Validate()
		assert.NoError(t, err)
		assert.Equal(t, 1024, opts.MaxInsertStatementSize)
	})

	t.Run("do not cap if max-allowed-packet is larger", func(t *testing.T) {
		opts := DumpOptions{
			InsertBatchSize: 100,
		}
		opts.MaxAllowedPacket = 8192
		opts.MaxInsertStatementSize = 4096

		err := opts.Validate()
		assert.NoError(t, err)
		assert.Equal(t, 4096, opts.MaxInsertStatementSize)
	})

	t.Run("invalid batch size", func(t *testing.T) {
		opts := DumpOptions{
			InsertBatchSize: 0,
		}
		err := opts.Validate()
		assert.Error(t, err)
	})
}
