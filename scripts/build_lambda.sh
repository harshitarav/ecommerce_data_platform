#!/bin/bash

set -e

echo "Building Lambda Packages..."

for FUNCTION in rds_to_bronze external_files_to_bronze
do

    PACKAGE_DIR="deployment/$FUNCTION"

    rm -rf $PACKAGE_DIR

    mkdir -p $PACKAGE_DIR

    pip install \
        -r deployment/$FUNCTION/requirements.txt \
        -t $PACKAGE_DIR

    cp -r common $PACKAGE_DIR/

    cp -r config $PACKAGE_DIR/

    cp -r logs $PACKAGE_DIR/

    cp deployment/$FUNCTION/lambda_function.py $PACKAGE_DIR/

    cd $PACKAGE_DIR

    zip -r ../${FUNCTION}.zip .

    cd ../../

done

echo "Lambda packages created successfully."