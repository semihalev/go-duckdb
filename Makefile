.PHONY: build test bench examples clean

GO := go
GOFLAGS :=
GOTESTFLAGS := -v
GOBENCHFLAGS := -v -benchmem -bench=.

all: build test examples

build:
	$(GO) build $(GOFLAGS) ./...

test:
	$(GO) test $(GOFLAGS) $(GOTESTFLAGS) ./...

bench:
	$(GO) test $(GOFLAGS) $(GOBENCHFLAGS) ./...

examples:
	$(GO) build $(GOFLAGS) -o bin/minimal ./example/minimal.go
	$(GO) build $(GOFLAGS) -o bin/complex ./example/complex.go
	$(GO) build $(GOFLAGS) -o bin/prepared ./example/prepared.go

run-examples: examples
	@echo "Running minimal example..."
	./bin/minimal
	@echo "\nRunning prepared example..."
	./bin/prepared
	@echo "\nRunning complex example..."
	./bin/complex

clean:
	$(GO) clean
	rm -rf bin/
	rm -f *.db

lint:
	$(GO) fmt ./...
	golangci-lint run

vet:
	$(GO) vet ./...

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all        - Build, test, and compile examples"
	@echo "  build      - Build the package"
	@echo "  test       - Run tests"
	@echo "  bench      - Run benchmarks"
	@echo "  examples   - Build example programs"
	@echo "  run-examples - Build and run example programs"
	@echo "  clean      - Clean up build artifacts"
	@echo "  lint       - Run code linters"
	@echo "  vet        - Run Go vet"
	@echo "  help       - Show this help message"