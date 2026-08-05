#!/bin/bash

set -e

echo "Building Lambda Packages..."

rm -rf build
mkdir -p build

for FUNCTION in rds_to_bronze external_files_to_bronze
do

    BUILD_DIR="build/$FUNCTION"

    mkdir -p "$BUILD_DIR"

    # Copy Lambda code only
    cp deployment/$FUNCTION/lambda_function.py "$BUILD_DIR/"

    cp -r deployment/$FUNCTION/common "$BUILD_DIR/"
    cp -r deployment/$FUNCTION/config "$BUILD_DIR/"
    cp -r deployment/$FUNCTION/logs "$BUILD_DIR/"

    cd "$BUILD_DIR"

    zip -r "../../${FUNCTION}.zip" .

    cd ../../

done

echo "Lambda packages created successfully."