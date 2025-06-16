THIS_FILE := $(lastword $(MAKEFILE_LIST))

GO_CMD?=go
CGO_ENABLED?=0

MAIN_PACKAGES=$$($(GO_CMD) list ./...)

default: format test

format:
	@find ./ -name '*.go' -print0 | xargs -P 12 -0 -I '{}' goimports -w '{}'

test:
	@for pkg in $(MAIN_PACKAGES) ; do \
		$(GO_CMD) test -cover $$pkg -parallel=20 ; \
	done