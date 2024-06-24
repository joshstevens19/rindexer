#!/bin/bash

# Change to the cli directory
cd cli || { echo "Failed to change directory to cli"; exit 1; }

# Extract project name and version from Cargo.toml in the cli directory
PROJECT_NAME=$(awk -F'=' '/^name/ {gsub(/"/, "", $2); print $2}' Cargo.toml | tr -d ' ')
VERSION=$(awk -F'=' '/^version/ {gsub(/"/, "", $2); print $2}' Cargo.toml | tr -d ' ')

# Replace dots with hyphens in the version string
VERSION=$(echo $VERSION | tr '.' '-')

TARGET_DIR="../documentation/docs/public/releases"

# Check if the file with the current version already exists
if [ -e "${TARGET_DIR}/${PROJECT_NAME}_v${VERSION}" ]; then
    echo "Error: The file ${TARGET_DIR}/${PROJECT_NAME}_v${VERSION} already exists. You must bump the version."
    exit 1
fi

# Run the build command
make prod_build

# Check if the build was successful
if [ $? -ne 0 ]; then
    echo "Build failed."
    exit 1
fi

# Change back to the original directory
cd - || { echo "Failed to change back to original directory"; exit 1; }

mkdir -p ${TARGET_DIR}

# Copy the binary to the target directory with the version in the filename
cp "target/release/${PROJECT_NAME}" "${TARGET_DIR}/${PROJECT_NAME}_latest"
cp "target/release/${PROJECT_NAME}" "${TARGET_DIR}/${PROJECT_NAME}_v${VERSION}"

# Check if the copy was successful
if [ $? -eq 0 ]; then
    echo "Binary successfully copied to ${TARGET_DIR}/${PROJECT_NAME}_v${VERSION}"
else
    echo "Failed to copy binary."
fi
