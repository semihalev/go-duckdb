.PHONY: build native test bench examples clean

GO := go
GOFLAGS :=
GOTESTFLAGS := -v
GOBENCHFLAGS := -v -benchmem -bench=.

all: build test

build:
	$(GO) build $(GOFLAGS) ./...

test:
	$(GO) test $(GOFLAGS) $(GOTESTFLAGS) ./...

bench:
	$(GO) test $(GOFLAGS) $(GOBENCHFLAGS) ./...

clean:
	@echo "Cleaning Go artifacts..."
	$(GO) clean
	rm -rf bin/
	rm -f *.db

lint:
	gofmt -w -s .
	~/go/bin/golangci-lint run

staticcheck:
	~/go/bin/staticcheck ./...
	
vet:
	$(GO) vet ./...

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all           - Build Go package, run tests, and compile examples"
	@echo "  build         - Build the package"
	@echo "  test          - Run tests"
	@echo "  bench         - Run benchmarks"
	@echo "  clean         - Clean up build Go artifacts"
	@echo "  lint          - Run code linters"
	@echo "  vet           - Run Go vet"
	@echo "  help          - Show this help message"