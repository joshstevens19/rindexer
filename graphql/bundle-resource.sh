#!/bin/bash

ROOT_DIR=$(pwd)

echo "Bundling resources..."

TARGET_DIR="${ROOT_DIR}/../documentation/docs/public/releases"

# Zip the resources directory and copy it to the target directory
(cd ../core/resources && zip -r "${TARGET_DIR}/resources.zip" .)

# Check if the copy was successful
if [ $? -eq 0 ]; then
    echo "Resources successfully copied to ${TARGET_DIR}/resources.zip"
else
    echo "Failed to resources."
fi