CC=cc
CFLAGS=-I../include -fPIC

# Detect platform
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
    PLATFORM=darwin
    EXT=.dylib
    LDFLAGS=-dynamiclib
else ifeq ($(UNAME_S),Linux)
    PLATFORM=linux
    EXT=.so
    LDFLAGS=-shared
else ifeq ($(OS),Windows_NT)
    PLATFORM=windows
    EXT=.dll
    LDFLAGS=-shared
endif

ifeq ($(UNAME_M),x86_64)
    ARCH=amd64
else ifeq ($(UNAME_M),i386)
    ARCH=386
else ifeq ($(UNAME_M),i686)
    ARCH=386
else ifeq ($(UNAME_M),arm64)
    ARCH=arm64
else ifeq ($(UNAME_M),aarch64)
    ARCH=arm64
endif

LIB_NAME=libduckdbnative_shim$(EXT)
LIB_DIR=../lib/$(PLATFORM)/$(ARCH)

all: $(LIB_NAME)

$(LIB_NAME): native_shim.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

install: $(LIB_NAME)
	mkdir -p $(LIB_DIR)
	cp $(LIB_NAME) $(LIB_DIR)/

darwin-shim:
	# Build for macOS (AMD64)
	$(CC) $(CFLAGS) -arch x86_64 -dynamiclib -o libduckdbnative-darwin-amd64.dylib native_shim.c
	mkdir -p ../lib/darwin/amd64
	cp libduckdbnative-darwin-amd64.dylib ../lib/darwin/amd64/libduckdbnative.dylib
	
	# Build for macOS (ARM64)
	$(CC) $(CFLAGS) -arch arm64 -dynamiclib -o libduckdbnative-darwin-arm64.dylib native_shim.c
	mkdir -p ../lib/darwin/arm64
	cp libduckdbnative-darwin-arm64.dylib ../lib/darwin/arm64/libduckdbnative.dylib

linux-shim:
	# Linux builds would need cross-compilation toolchain or to be run on Linux
	@echo "Linux builds should be performed on a Linux machine or with a cross-compiler"

windows-shim:
	# Windows builds would need cross-compilation toolchain or to be run on Windows
	@echo "Windows builds should be performed on a Windows machine or with a cross-compiler"

clean:
	rm -f *.o *.dylib *.so *.dll

.PHONY: all install clean darwin-shim linux-shim windows-shim