#!/bin/bash

set -e

echo "Building Lambda Layer..."

rm -rf layer

mkdir -p layer/python

pip3 install \
    -r layers/requirements.txt \
    -t layer/python \
    --no-cache-dir

# Remove unnecessary files to reduce size
find layer/python -type d -name "__pycache__" -exec rm -rf {} +
find layer/python -type d -name "tests" -exec rm -rf {} +
find layer/python -type d -name "test" -exec rm -rf {} +
find layer/python -type d -name "*.dist-info" -exec rm -rf {} +
find layer/python -type d -name "*.egg-info" -exec rm -rf {} +

cd layer

zip -r layer.zip python

echo "Layer built successfully."