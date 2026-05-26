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
COVERPKG := $(shell $(GO_CMD) list ./... | grep -v '/cypher/parser$$' | tr '\n' ',' | sed 's/,$$//')

# Metric configuration
METRICS_DIR ?= .coverage
COVERAGE_PROFILE ?= $(METRICS_DIR)/unit.out
COVERAGE_FUNC_REPORT ?= $(METRICS_DIR)/coverage.txt
CYCLO_REPORT ?= $(METRICS_DIR)/cyclomatic.txt
CRAP_TEXT_REPORT ?= $(METRICS_DIR)/crap.txt
CRAP_JSON_REPORT ?= $(METRICS_DIR)/crap.json
QUALITY_TEXT_REPORT ?= $(METRICS_DIR)/quality.txt
QUALITY_JSON_REPORT ?= $(METRICS_DIR)/quality.json
METRICS_HTML_REPORT ?= $(METRICS_DIR)/metrics.html
METRICS_IGNORE ?= (^|/)(testdata|vendor)/|_test\.go$$|^cypher/parser/
CYCLO_TOP ?= 20
CYCLO_OVER ?= 25
CRAP_TOP ?= 20
CRAP_OVER ?= 30
METRICS_ENFORCE ?= 0
BENCHMARK_REPORT ?=
BENCHMARK_BASELINE ?=
BENCHMARK_REGRESSION ?= 0.20
FUZZ_REPORT ?=
MUTATION_REPORT ?=
BACKEND_RESULT_ARGS ?=
BACKEND_PG_REPORT ?= $(METRICS_DIR)/integration-pg.json
BACKEND_NEO4J_REPORT ?= $(METRICS_DIR)/integration-neo4j.json
QUALITY_BENCHMARK_REPORT ?= $(METRICS_DIR)/benchmark.json
QUALITY_BENCHMARK_MARKDOWN ?= $(METRICS_DIR)/benchmark.md

QUALITY_INPUTS := $(BACKEND_RESULT_ARGS)
ifneq ($(strip $(BENCHMARK_REPORT)),)
QUALITY_INPUTS += -benchmark-report $(BENCHMARK_REPORT)
endif
ifneq ($(strip $(BENCHMARK_BASELINE)),)
QUALITY_INPUTS += -benchmark-baseline $(BENCHMARK_BASELINE)
endif
ifneq ($(strip $(FUZZ_REPORT)),)
QUALITY_INPUTS += -fuzz-report $(FUZZ_REPORT)
endif
ifneq ($(strip $(MUTATION_REPORT)),)
QUALITY_INPUTS += -mutation-report $(MUTATION_REPORT)
endif
QUALITY_INPUTS += -benchmark-regression $(BENCHMARK_REGRESSION)

.PHONY: default all build deps tidy lint format test test_all test_integration test_neo4j test_pg test_update plan_corpus complexity complexity_check crap crap_check quality quality_check quality_backend quality_bench metrics metrics_check generate clean help

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
test: $(METRICS_DIR)
	@echo "Running tests..."
	@$(GO_CMD) test -race -covermode=atomic -coverprofile=$(COVERAGE_PROFILE) -coverpkg=$(COVERPKG) -count=1 -parallel=10 $(MAIN_PACKAGES)
	@$(GO_CMD) tool cover -func=$(COVERAGE_PROFILE) > $(COVERAGE_FUNC_REPORT)
	@echo "Coverage report written to $(COVERAGE_FUNC_REPORT)"

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

plan_corpus: $(METRICS_DIR)
	@echo "Capturing Cypher plan corpus..."
	@$(GO_CMD) run ./cmd/plancorpus

# Metric targets
$(METRICS_DIR):
	@mkdir -p $(METRICS_DIR)

complexity: $(METRICS_DIR)
	@echo "Measuring cyclomatic complexity..."
	@$(GO_CMD) tool gocyclo -top $(CYCLO_TOP) -ignore '$(METRICS_IGNORE)' . | tee $(CYCLO_REPORT)
	@echo "Cyclomatic complexity report written to $(CYCLO_REPORT)"

complexity_check: $(METRICS_DIR)
	@echo "Checking cyclomatic complexity..."
	@if [ "$(METRICS_ENFORCE)" = "1" ]; then \
		$(GO_CMD) tool gocyclo -over $(CYCLO_OVER) -ignore '$(METRICS_IGNORE)' . | tee $(CYCLO_REPORT); \
	else \
		$(GO_CMD) tool gocyclo -top $(CYCLO_TOP) -ignore '$(METRICS_IGNORE)' . | tee $(CYCLO_REPORT); \
		echo "METRICS_ENFORCE=0; cyclomatic complexity threshold $(CYCLO_OVER) is report-only."; \
	fi

crap: test
	@echo "Calculating CRAP metrics..."
	@$(GO_CMD) tool dawgs-metrics -source-root . -coverprofile $(COVERAGE_PROFILE) -ignore '$(METRICS_IGNORE)' -top $(CRAP_TOP) -over $(CRAP_OVER) -cyclo-over $(CYCLO_OVER) $(QUALITY_INPUTS) -text $(CRAP_TEXT_REPORT) -json $(CRAP_JSON_REPORT) -quality-text $(QUALITY_TEXT_REPORT) -quality-json $(QUALITY_JSON_REPORT) -html $(METRICS_HTML_REPORT)
	@echo "CRAP and quality reports written to $(CRAP_TEXT_REPORT), $(CRAP_JSON_REPORT), $(QUALITY_TEXT_REPORT), $(QUALITY_JSON_REPORT), and $(METRICS_HTML_REPORT)"

crap_check: test
	@echo "Checking CRAP metrics..."
	@if [ "$(METRICS_ENFORCE)" = "1" ]; then \
		$(GO_CMD) tool dawgs-metrics -source-root . -coverprofile $(COVERAGE_PROFILE) -ignore '$(METRICS_IGNORE)' -top $(CRAP_TOP) -over $(CRAP_OVER) -cyclo-over $(CYCLO_OVER) $(QUALITY_INPUTS) -text $(CRAP_TEXT_REPORT) -json $(CRAP_JSON_REPORT) -quality-text $(QUALITY_TEXT_REPORT) -quality-json $(QUALITY_JSON_REPORT) -html $(METRICS_HTML_REPORT) -fail-over $(CRAP_OVER) -fail-quality; \
	else \
		$(GO_CMD) tool dawgs-metrics -source-root . -coverprofile $(COVERAGE_PROFILE) -ignore '$(METRICS_IGNORE)' -top $(CRAP_TOP) -over $(CRAP_OVER) -cyclo-over $(CYCLO_OVER) $(QUALITY_INPUTS) -text $(CRAP_TEXT_REPORT) -json $(CRAP_JSON_REPORT) -quality-text $(QUALITY_TEXT_REPORT) -quality-json $(QUALITY_JSON_REPORT) -html $(METRICS_HTML_REPORT); \
		echo "METRICS_ENFORCE=0; CRAP threshold $(CRAP_OVER) is report-only."; \
	fi

quality: test
	@echo "Calculating quality metrics..."
	@$(GO_CMD) tool dawgs-metrics -source-root . -coverprofile $(COVERAGE_PROFILE) -ignore '$(METRICS_IGNORE)' -top $(CRAP_TOP) -over $(CRAP_OVER) -cyclo-over $(CYCLO_OVER) $(QUALITY_INPUTS) -quality-text $(QUALITY_TEXT_REPORT) -quality-json $(QUALITY_JSON_REPORT) -html $(METRICS_HTML_REPORT) -stdout=false
	@echo "Quality reports written to $(QUALITY_TEXT_REPORT), $(QUALITY_JSON_REPORT), and $(METRICS_HTML_REPORT)"

quality_check: test
	@echo "Checking quality metrics..."
	@if [ "$(METRICS_ENFORCE)" = "1" ]; then \
		$(GO_CMD) tool dawgs-metrics -source-root . -coverprofile $(COVERAGE_PROFILE) -ignore '$(METRICS_IGNORE)' -top $(CRAP_TOP) -over $(CRAP_OVER) -cyclo-over $(CYCLO_OVER) $(QUALITY_INPUTS) -quality-text $(QUALITY_TEXT_REPORT) -quality-json $(QUALITY_JSON_REPORT) -html $(METRICS_HTML_REPORT) -stdout=false -fail-quality; \
	else \
		$(GO_CMD) tool dawgs-metrics -source-root . -coverprofile $(COVERAGE_PROFILE) -ignore '$(METRICS_IGNORE)' -top $(CRAP_TOP) -over $(CRAP_OVER) -cyclo-over $(CYCLO_OVER) $(QUALITY_INPUTS) -quality-text $(QUALITY_TEXT_REPORT) -quality-json $(QUALITY_JSON_REPORT) -html $(METRICS_HTML_REPORT) -stdout=false; \
		echo "METRICS_ENFORCE=0; quality watch signals are report-only."; \
	fi

quality_backend: test
	@echo "Running backend equivalence test captures..."
	@if [ -z "$(PG_CONNECTION_STRING)" ] || [ -z "$(NEO4J_CONNECTION_STRING)" ]; then \
		echo "PG_CONNECTION_STRING and NEO4J_CONNECTION_STRING are required."; \
		exit 1; \
	fi
	@set +e; \
	CONNECTION_STRING="$(PG_CONNECTION_STRING)" $(GO_CMD) test -json -tags 'manual_integration integration' -race -cover -count=1 -p=1 -parallel=1 $(MAIN_PACKAGES) > $(BACKEND_PG_REPORT); \
	pg_status=$$?; \
	CONNECTION_STRING="$(NEO4J_CONNECTION_STRING)" $(GO_CMD) test -json -tags 'manual_integration integration' -race -cover -count=1 -p=1 -parallel=1 $(MAIN_PACKAGES) > $(BACKEND_NEO4J_REPORT); \
	neo4j_status=$$?; \
	set -e; \
	$(GO_CMD) tool dawgs-metrics -source-root . -coverprofile $(COVERAGE_PROFILE) -ignore '$(METRICS_IGNORE)' -top $(CRAP_TOP) -over $(CRAP_OVER) -cyclo-over $(CYCLO_OVER) -backend-result pg=$(BACKEND_PG_REPORT) -backend-result neo4j=$(BACKEND_NEO4J_REPORT) $(QUALITY_INPUTS) -quality-text $(QUALITY_TEXT_REPORT) -quality-json $(QUALITY_JSON_REPORT) -html $(METRICS_HTML_REPORT) -stdout=false; \
	if [ $$pg_status -ne 0 ]; then exit $$pg_status; fi; \
	if [ $$neo4j_status -ne 0 ]; then exit $$neo4j_status; fi

quality_bench: $(METRICS_DIR)
	@echo "Running benchmark capture..."
	@$(GO_CMD) run ./cmd/benchmark -output $(QUALITY_BENCHMARK_MARKDOWN) -json-output $(QUALITY_BENCHMARK_REPORT)
	@echo "Benchmark reports written to $(QUALITY_BENCHMARK_MARKDOWN) and $(QUALITY_BENCHMARK_REPORT)"

metrics: complexity crap

metrics_check: METRICS_ENFORCE = 1
metrics_check: complexity_check crap_check

# Utility targets
generate:
	@echo "Running code generation..."
	@$(GO_CMD) generate ./...

clean:
	@echo "Cleaning build artifacts..."
	@$(GO_CMD) clean ./...

	@rm -rf cypher/analyzer/updated_cases/
	@rm -rf cypher/models/pgsql/test/updated_cases
	@rm -rf $(METRICS_DIR)

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
	@echo "  plan_corpus - Capture shared corpus query plans for configured backends"
	@echo "  test_update - Update test cases"
	@echo "  complexity  - Report cyclomatic complexity"
	@echo "  crap        - Report CRAP scores from unit test coverage"
	@echo "  quality     - Report drift, equivalence, invariant, fuzz, mutation, and benchmark signals"
	@echo "  quality_backend - Capture backend equivalence test results"
	@echo "  quality_bench - Capture benchmark markdown and JSON reports"
	@echo "  metrics     - Run cyclomatic complexity, CRAP, and quality reports"
	@echo "  metrics_check - Enforce cyclomatic complexity, CRAP, and quality thresholds"
	@echo ""
	@echo "Utility:"
	@echo "  clean       - Clean build artifacts"
	@echo "  help        - Show this help message"
