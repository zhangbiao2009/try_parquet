#!/bin/bash

# Build script for C++ OLAP Analysis implementations
set -e

echo "Building C++ OLAP Analysis implementations..."

# Create build directory
mkdir -p build
cd build

# Check if required dependencies are available
echo "Checking dependencies..."

# Check for CMake
if ! command -v cmake &> /dev/null; then
    echo "❌ CMake not found. Please install CMake."
    exit 1
fi

# Check for pkg-config
if ! command -v pkg-config &> /dev/null; then
    echo "❌ pkg-config not found. Please install pkg-config."
    exit 1
fi

echo "✅ Basic build tools found"

# Configure with CMake
echo "Configuring build..."
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build the projects
echo "Building projects..."
make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
    echo ""
    echo "Executables created:"
    ls -la bin/ 2>/dev/null || echo "Check build/src/ for executables"
    echo ""
    echo "To run:"
    echo "  DuckDB version: ./bin/duckdb_olap_analysis (or ./src/duckdb_olap_analysis)"
    echo "  Arrow version:  ./bin/arrow_olap_analysis (or ./src/arrow_olap_analysis)"
else
    echo "❌ Build failed!"
    exit 1
fi
