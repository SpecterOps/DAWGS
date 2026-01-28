THIS_FILE := $(lastword $(MAKEFILE_LIST))

GO_CMD?=go
CGO_ENABLED?=0

MAIN_PACKAGES=$$($(GO_CMD) list ./...)

default: format test
all: generate format test
update: mod_update mod_tidy

generate:
	@$(GO_CMD) generate ./...

format:
	@find ./ -name '*.go' -print0 | xargs -P 12 -0 -I '{}' goimports -w '{}'

test:
	@for pkg in $(MAIN_PACKAGES) ; do \
		$(GO_CMD) test -cover $$pkg -parallel=20 ; \
	done

mod_update:
	@$(GO_CMD) get -u ./...

mod_tidy:
	@$(GO_CMD) mod tidy
