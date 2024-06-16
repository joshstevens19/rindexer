#!/bin/bash

make prod_build

if [ $? -ne 0 ]; then
    echo "Build failed."
    exit 1
fi

# Extract project name from Cargo.toml 
# This uses awk to find the line with 'name =', strips quotes and equals sign, then prints the project name
PROJECT_NAME=$(awk -F'=' '/^name/ {gsub(/"/, "", $2); print $2}' Cargo.toml | tr -d ' ')

TARGET_DIR="binary_builds"
mkdir -p ${TARGET_DIR}

DATETIME=$(date "+%Y%m%d-%H%M%S")

cp "target/production/${PROJECT_NAME}" "${TARGET_DIR}/${PROJECT_NAME}_${DATETIME}"

if [ $? -eq 0 ]; then
    echo "Binary successfully copied to ${TARGET_DIR}/${PROJECT_NAME}_${DATETIME}"
else
    echo "Failed to copy binary."
fi
