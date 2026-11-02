GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

.PHONY: build test lint coverage

build:
	go build ./...

test:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run

coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
