MAIN_PATH := ./cmd/greenmask/
CMD_NAME := greenmask
CMD_FILES = $(wildcard *.go)
TEST_FILES = $(wildcard *.go)
COVERAGE_FILE := coverage.out
VERSION ?= $(shell git tag --points-at HEAD)
LDFLAGS ?= -X github.com/greenmaskio/greenmask/cmd/greenmask/cmd.Version=$(VERSION)

.PHONY: build

tests: unittest

unittest:
	go list ./... | grep -E 'internal|pkg' | xargs go test -v

coverage:
	go list ./... | grep -E 'internal|pkg' | xargs go test -v -coverprofile=$(COVERAGE_FILE) | grep -v 'no test files'
	go tool cover -html=$(COVERAGE_FILE)

install:
	mv $(MAIN_PATH)/$(CMD_NAME) $(GOBIN)/$(CMD_NAME)

# The build flag -tags=viper_bind_struct has been added to avoid the need to bind each of the environment variables
build: $(CMD_FILES)	
	CGO_ENABLED=0 go build -tags=viper_bind_struct -ldflags="$(LDFLAGS)" -v -o $(CMD_NAME) $(MAIN_PATH)

lint:
	golangci-lint run ./...

up:
	docker-compose up playground-dbs-filler
