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
	go list ./... | grep -E 'internal|pkg' | xargs go test -v -coverprofile=$(COVERAGE_FILE)

coverage-view:
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

# Select the storage backend for the playground, e.g.:
#   make greenmask-from-source STORAGE_BACKEND=azure
# Supported values: s3 (default, Minio), azure (Azurite/Azure Blob) and ssh
# (atmoz/sftp SFTP server). The azure and ssh values layer their respective
# override file over the base file, swapping the `playground-storage` service.
STORAGE_BACKEND ?= s3
ifeq ($(STORAGE_BACKEND),azure)
GREENMASK_COMPOSE := -f docker-compose.yml -f docker-compose-azure.yml
else ifeq ($(STORAGE_BACKEND),ssh)
GREENMASK_COMPOSE := -f docker-compose.yml -f docker-compose-ssh.yml
else ifeq ($(STORAGE_BACKEND),s3)
GREENMASK_COMPOSE := -f docker-compose.yml
else
$(error Unsupported STORAGE_BACKEND "$(STORAGE_BACKEND)"; use "s3", "azure" or "ssh")
endif

greenmask-latest:
	docker compose $(GREENMASK_COMPOSE) run greenmask

# Build the `greenmask-from-source` compose service image and then run it. The
# explicit build step is required because `docker compose run` does not rebuild
# on its own, so without it the container keeps running a stale, cached image.
# DOCKER_BUILD_FLAGS (e.g. --no-cache) is forwarded to this build so a clean
# rebuild actually reaches the image the container runs.
greenmask-from-source:
	docker compose $(GREENMASK_COMPOSE) build $(DOCKER_BUILD_FLAGS) greenmask-from-source
	docker compose $(GREENMASK_COMPOSE) run --rm greenmask-from-source

integration:
	docker buildx build --load -t greenmask-test-dbs-filler:latest -f docker/integration/filldb/Dockerfile docker/integration/filldb
	docker buildx build --load -t greenmask-integration:latest -f docker/integration/tests/Dockerfile .
	docker compose -f docker-compose-integration.yml -p greenmask up \
                --renew-anon-volumes --force-recreate \
                --exit-code-from greenmask --abort-on-container-exit greenmask \
				--profile all

integration-local:
	docker buildx build --load -t greenmask-test-dbs-filler:latest -f docker/integration/filldb/Dockerfile docker/integration/filldb
	docker buildx build --load -t greenmask-integration:latest -f docker/integration/tests/Dockerfile .
	COMPOSE_PROFILES=all docker compose -f docker-compose-integration.yml -p greenmask up \
                --renew-anon-volumes --force-recreate \
                --exit-code-from greenmask --abort-on-container-exit greenmask

