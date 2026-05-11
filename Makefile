THIS_FILE := $(lastword $(MAKEFILE_LIST))

# Go configuration
GO_CMD ?= go
CGO_ENABLED ?= 0
BENCH ?= .
BENCH_COUNT ?= 10
BENCH_TIME ?= 1s
BENCH_BASE ?= main
BENCH_TARGET ?= HEAD
BENCH_KIND ?= all

# Main packages to test/build
MAIN_PACKAGES := $(shell $(GO_CMD) list ./...)

# Default target
default: help
all: generate format tidy test

# Build targets
build:
	@echo "Building all packages..."
	@$(GO_CMD) build ./...

# Dependency management
deps:
	@echo "Downloading dependencies..."
	@$(GO_CMD) mod download

tidy:
	@echo "Tidying go modules..."
	@$(GO_CMD) mod tidy

# Code quality
lint:
	@echo "Running linter..."
	@$(GO_CMD) vet ./...

format:
	@echo "Formatting code..."
	@find ./ -name '*.go' -print0 | xargs -P 12 -0 -I '{}' goimports -w '{}'

# Test targets
test:
	@echo "Running tests..."
	@$(GO_CMD) test -race -cover -count=1 -parallel=10 $(MAIN_PACKAGES)

test_all: test test_integration

test_integration:
	@echo "Running all integration tests..."
	@$(GO_CMD) test -tags 'manual_integration integration' -race -cover -count=1 -p=1 -parallel=1 $(MAIN_PACKAGES)

test_bench:
	@echo "Running benchmarks..."
	@$(GO_CMD) test -run '^$$' -bench '$(BENCH)' -benchmem -count=$(BENCH_COUNT) -benchtime=$(BENCH_TIME) $(MAIN_PACKAGES)

bench_diff:
	@echo "Running benchmark diff..."
	@$(GO_CMD) run ./cmd/benchdiff --base '$(BENCH_BASE)' --target '$(BENCH_TARGET)' --kind '$(BENCH_KIND)' --bench '$(BENCH)' --bench-count '$(BENCH_COUNT)' --benchtime '$(BENCH_TIME)' $(BENCHDIFF_ARGS)

test_neo4j:
	@echo "Running Neo4j integration tests..."
	@$(GO_CMD) test -tags integration -race -cover -count=1 -p=1 -parallel=1 $(MAIN_PACKAGES)

test_pg:
	@echo "Running PostgreSQL integration tests..."
	@$(GO_CMD) test -tags manual_integration -race -cover -count=1 -p=1 -parallel=1 $(MAIN_PACKAGES)

test_update:
	@echo "Updating test cases..."
	@CYSQL_UPDATE_CASES=true $(GO_CMD) test -parallel=10 $(MAIN_PACKAGES)

	@cp -fv cypher/analyzer/updated_cases/* cypher/test/cases
	@rm -rf cypher/analyzer/updated_cases/
	@cp -fv cypher/models/pgsql/test/updated_cases/* cypher/models/pgsql/test/translation_cases
	@rm -rf cypher/models/pgsql/test/updated_cases

# Utility targets
generate:
	@echo "Running code generation..."
	@$(GO_CMD) generate ./...

clean:
	@echo "Cleaning build artifacts..."
	@$(GO_CMD) clean ./...

	@rm -rf cypher/analyzer/updated_cases/
	@rm -rf cypher/models/pgsql/test/updated_cases

help:
	@echo "Available targets:"
	@echo "  default     - Show this help message"
	@echo "  all         - Runs all prep steps for prepare a changeset for review"
	@echo ""
	@echo "Build:"
	@echo "  build       - Build all packages"
	@echo ""
	@echo "Dependencies:"
	@echo "  deps        - Download dependencies"
	@echo "  tidy        - Tidy go modules"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint        - Run go vet"
	@echo "  format      - Format all Go files"
	@echo "  generate    - Run code generation"
	@echo ""
	@echo "Testing:"
	@echo "  test        - Run all unit tests with coverage"
	@echo "  test_all    - Run all tests including integration tests"
	@echo "  test_integration - Run all integration tests"
	@echo "  test_bench  - Run benchmark test"
	@echo "  bench_diff  - Compare benchmarks between commits"
	@echo "  test_neo4j  - Run Neo4j integration tests"
	@echo "  test_pg     - Run PostgreSQL integration tests"
	@echo "  test_update - Update test cases"
	@echo ""
	@echo "Utility:"
	@echo "  clean       - Clean build artifacts"
	@echo "  help        - Show this help message"
