package storages

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestS3Storage(t *testing.T) {
	suite.Run(t, new(S3StorageSuite))
}
