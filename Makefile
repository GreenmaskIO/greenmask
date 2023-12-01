MAIN_PATH := ./cmd/greenmask/
CMD_NAME := greenmask
CMD_FILES = $(wildcard *.go)
TEST_FILES = $(wildcard *.go)
COVERAGE_FILE := coverage.out
VERSION ?= $(shell git tag --points-at HEAD)
LDFLAGS ?= -X main.version=$(VERSION)

.PHONY: build

tests: unittest

unittest:
	go list ./... | xargs go vet
	go test $(TEST_FILES)

coverage:
	go list ./... | grep -E 'internal|pkg' | xargs go test -v -coverprofile=$(COVERAGE_FILE) | grep -v 'no test files'
	go tool cover -html=$(COVERAGE_FILE)

install:
	mv $(MAIN_PATH)/$(CMD_NAME) $(GOBIN)/$(CMD_NAME)

build: $(CMD_FILES)
	go build -ldflags="$(LDFLAGS)" -v -o $(CMD_NAME) $(MAIN_PATH)
