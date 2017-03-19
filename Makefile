GO ?= go
GOFLAGS ?=

TAGS :=
SUFFIX :=

GIT_DIR := $(shell git rev-parse --git-dir 2> /dev/null)

.DELETE_ON_ERROR:

.PHONY: all
all:

.PHONY: example
example:
	$(GO) run -v $(GOFLAGS) -tags '$(TAGS)' $(@).go
