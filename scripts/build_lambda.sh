#!/bin/bash

set -e

echo "Building Lambda Packages..."

for FUNCTION in rds_to_bronze external_files_to_bronze
do

    BUILD_DIR="build/$FUNCTION"

    rm -rf "$BUILD_DIR"

    mkdir -p "$BUILD_DIR"

    pip install \
        -r deployment/$FUNCTION/requirements.txt \
        -t "$BUILD_DIR"

    cp -r deployment/$FUNCTION/common "$BUILD_DIR/"

    cp -r deployment/$FUNCTION/config "$BUILD_DIR/"

    cp -r deployment/$FUNCTION/logs "$BUILD_DIR/"

    cp deployment/$FUNCTION/lambda_function.py "$BUILD_DIR/"

    cd "$BUILD_DIR"

    zip -r "../../${FUNCTION}.zip" .

    cd ../../

done

echo "Lambda packages created successfully."