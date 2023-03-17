all: lint test

test:
	go test ./...

fmt:
	go install golang.org/x/tools/cmd/goimports@latest
	goimports -w .
	find . -name go.mod -execdir go mod tidy \;

lint:
	go vet
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.50.1
	golangci-lint run

check: fmt lint test

.PHONY: all test lint check
