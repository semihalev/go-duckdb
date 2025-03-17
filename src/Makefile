# Makefile for DuckDB native optimizations with SIMD support for multiple architectures

CC = gcc
CXX = g++
COMMON_FLAGS = -O3 -fPIC -Wall -I.. -I../include

# Detect OS and set up platform-specific variables
UNAME := $(shell uname)
ARCH := $(shell uname -m)

# Architecture-specific flags
ifeq ($(ARCH), x86_64)
    # x86_64 architecture
    ARCH_FLAGS = -mavx2 -mfma
    MARCH = -march=x86-64-v3
endif

ifeq ($(ARCH), arm64)
    # ARM64 architecture
    ARCH_FLAGS = -march=armv8-a+simd+fp
    CFLAGS += -D__ARM_NEON
endif

ifeq ($(ARCH), aarch64)
    # ARM64 on Linux
    ARCH_FLAGS = -march=armv8-a+simd+fp
    CFLAGS += -D__ARM_NEON
endif

# OS-specific settings
ifeq ($(UNAME), Darwin)
    # macOS
    EXT = .dylib
    LDFLAGS = -shared -undefined dynamic_lookup
else ifeq ($(UNAME), Linux)
    # Linux
    EXT = .so
    LDFLAGS = -shared
else
    # Windows/other - assuming Windows
    EXT = .dll
    LDFLAGS = -shared -Wl,--out-implib,$@.a
endif

# Combine flags
CFLAGS = $(COMMON_FLAGS) $(ARCH_FLAGS) $(MARCH)

TARGET = ../libduckdb_native$(EXT)

SRC = duckdb_native.c
OBJ = $(SRC:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJ)
	@echo "Building for architecture: $(ARCH)"
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ -lstdc++

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)

.PHONY: all clean