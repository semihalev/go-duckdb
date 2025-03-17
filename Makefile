.PHONY: build native test bench examples clean

GO := go
GOFLAGS :=
GOTESTFLAGS := -v
GOBENCHFLAGS := -v -benchmem -bench=.

all: native build test examples

native:
	@echo "Building native optimizations..."
	cd src && chmod +x build.sh && ./build.sh
	@echo "Building native shim library..."
	cd native && $(MAKE) && $(MAKE) install

native-dynamic:
	@echo "Building dynamic native libraries for all platforms..."
	cd src && chmod +x build.sh && ./build.sh --platform darwin
	cd src && chmod +x build.sh && ./build.sh --platform linux
	cd src && chmod +x build.sh && ./build.sh --platform windows
	@echo "Building native shim libraries for all platforms..."
	cd native && $(MAKE) darwin-shim
	cd native && $(MAKE) linux-shim
	cd native && $(MAKE) windows-shim

native-current:
	@echo "Building dynamic native library for current platform..."
	cd src && chmod +x build.sh && ./build.sh
	@echo "Building native shim library for current platform..."
	cd native && $(MAKE) && $(MAKE) install

build: native
	$(GO) build $(GOFLAGS) ./...

test:
	$(GO) test $(GOFLAGS) $(GOTESTFLAGS) ./...

bench:
	$(GO) test $(GOFLAGS) $(GOBENCHFLAGS) ./...

examples:
	mkdir -p bin
	$(GO) build $(GOFLAGS) -o bin/minimal ./example/minimal.go
	$(GO) build $(GOFLAGS) -o bin/complex ./example/complex.go
	$(GO) build $(GOFLAGS) -o bin/prepared ./example/prepared.go
	$(GO) build $(GOFLAGS) -o bin/native_info ./example/native_info.go

run-examples: examples
	@echo "Running minimal example..."
	./bin/minimal
	@echo "\nRunning prepared example..."
	./bin/prepared
	@echo "\nRunning complex example..."
	./bin/complex
	@echo "\nRunning native info example..."
	./bin/native_info

clean:
	@echo "Cleaning Go artifacts..."
	$(GO) clean
	rm -rf bin/
	rm -f *.db
	@echo "Cleaning native artifacts..."
	cd src && $(MAKE) clean
	@echo "Cleaning native shim artifacts..."
	cd native && $(MAKE) clean

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
	@echo "  all           - Build native code, Go package, run tests, and compile examples"
	@echo "  native        - Build native optimizations library (static)"
	@echo "  native-dynamic - Build dynamic native libraries for all platforms"
	@echo "  native-current - Build dynamic native library for current platform only"
	@echo "  build         - Build the package (depends on native)"
	@echo "  test          - Run tests"
	@echo "  bench         - Run benchmarks"
	@echo "  examples      - Build example programs"
	@echo "  run-examples  - Build and run example programs"
	@echo "  clean         - Clean up build artifacts (Go and native)"
	@echo "  lint          - Run code linters"
	@echo "  vet           - Run Go vet"
	@echo "  help          - Show this help message"