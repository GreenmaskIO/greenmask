package toc

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestTocLibrary(t *testing.T) {
	suite.Run(t, new(TocReadWriterSuite))
}

func TestGreenmaskBackwardCompatibility(t *testing.T) {
	suite.Run(t, new(BackwardCompatibilitySuite))
}
