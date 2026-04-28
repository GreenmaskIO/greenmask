MAIN_PATH := ./cmd/
CMD_NAME := greenmask
CMD_FILES = $(wildcard *.go)
TEST_FILES = $(wildcard *.go)
COVERAGE_FILE := coverage.out
VERSION ?= $(shell git tag --points-at HEAD)
LDFLAGS ?= -X main.Version=$(VERSION)

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

integration:
	go test -v -timeout 15m ./tests/integration/features/...

local-build:
	DOCKER_BUILDKIT=1 \
		docker build \
			-f docker/greenmask/mysql/main/Dockerfile \
			. \
			-t greenmask-from-source:latest \
			--platform linux/amd64 \
			--target main

local-stop:
	docker compose  -f ./docker-compose-mysql.yml down

local-cleanup: local-stop
	docker container prune -f
	docker network prune -f
	docker volume prune -f
	docker image prune -f

greenmask-latest:
	docker compose -f docker-compose-mysql.yml run greenmask

greenmask-from-source: local-build
	docker compose -f docker-compose-mysql.yml run greenmask-from-source
