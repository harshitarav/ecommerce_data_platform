#!/bin/bash

set -e

echo "Building Lambda Layer..."

rm -rf layer

mkdir -p layer/python

pip3 install \
    -r layers/requirements.txt \
    -t layer/python

cd layer

zip -r layer.zip python

cd ..

echo "Layer built successfully."