#!/bin/bash
# Script to build native libraries for multiple platforms

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CMAKE_OPTIONS=""

# Function to build for a specific platform/architecture
build_for() {
    local platform=$1
    local arch=$2
    local build_dir="${SCRIPT_DIR}/build-${platform}-${arch}"
    
    echo "Building for ${platform}/${arch}..."
    
    # Create build directory
    mkdir -p "$build_dir"
    cd "$build_dir"
    
    # Configure and build
    cmake ${CMAKE_OPTIONS} "${SCRIPT_DIR}"
    cmake --build . --config Release
    
    # Create output directory
    mkdir -p "${SCRIPT_DIR}/../lib/${platform}/${arch}"
    
    # Copy the library to the correct location
    if [[ "$platform" == "darwin" ]]; then
        cp "${build_dir}/lib/darwin/${arch}/libduckdbnative.dylib" "${SCRIPT_DIR}/../lib/darwin/${arch}/"
    elif [[ "$platform" == "linux" ]]; then
        cp "${build_dir}/lib/linux/${arch}/libduckdbnative.so" "${SCRIPT_DIR}/../lib/linux/${arch}/"
    elif [[ "$platform" == "windows" ]]; then
        cp "${build_dir}/lib/windows/${arch}/duckdbnative.dll" "${SCRIPT_DIR}/../lib/windows/${arch}/"
    fi
    
    echo "Build for ${platform}/${arch} completed"
}

# Detect host system
HOST_OS=$(uname -s)
HOST_ARCH=$(uname -m)

case "$HOST_OS" in
    Darwin*)
        HOST_PLATFORM="darwin"
        ;;
    Linux*)
        HOST_PLATFORM="linux"
        ;;
    MINGW*|MSYS*|CYGWIN*)
        HOST_PLATFORM="windows"
        ;;
    *)
        echo "Unsupported host OS: $HOST_OS"
        exit 1
        ;;
esac

# Setup cross-compilation if needed
if [ "$1" = "--cross" ]; then
    echo "Setting up for cross-compilation..."
    
    # This would set up appropriate cross-compilation toolchains
    # Specific options would be added to CMAKE_OPTIONS
    
    # For example, to cross-compile for Windows from Linux:
    # CMAKE_OPTIONS="-DCMAKE_TOOLCHAIN_FILE=path/to/windows-toolchain.cmake"
fi

# Check for specific platform to build
if [ "$1" = "--platform" ] && [ -n "$2" ]; then
    case "$2" in
        darwin)
            build_for darwin arm64
            build_for darwin x86_64
            ;;
        linux)
            build_for linux amd64
            build_for linux arm64
            ;;
        windows)
            build_for windows amd64
            build_for windows arm64
            ;;
        *)
            echo "Unknown platform: $2"
            exit 1
            ;;
    esac
    exit 0
fi

# Build for the host platform/architecture by default
echo "Building for host: ${HOST_PLATFORM}/${HOST_ARCH}"
case "$HOST_ARCH" in
    x86_64|amd64)
        build_for "$HOST_PLATFORM" "amd64"
        ;;
    arm64|aarch64)
        build_for "$HOST_PLATFORM" "arm64"
        ;;
    *)
        echo "Unsupported architecture: $HOST_ARCH"
        exit 1
        ;;
esac

echo "Build completed successfully"