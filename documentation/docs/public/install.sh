#!/usr/bin/env bash
set -eo pipefail

BASE_DIR="${XDG_CONFIG_HOME:-$HOME}"
RINDEXER_DIR="${RINDEXER_DIR:-"$BASE_DIR/.rindexer"}"
RINDEXER_BIN_DIR="$RINDEXER_DIR/bin"
RINDEXERUP_PATH="$RINDEXER_BIN_DIR/rindexerup"
RINDEXERDOWN_PATH="$RINDEXER_BIN_DIR/rindexerdown"
OS_TYPE=$(uname)
ARCH_TYPE=$(uname -m)

# Parse command line arguments
VERSION=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --local)
            LOCAL_INSTALL=true
            shift
            ;;
        --uninstall)
            UNINSTALL=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

if [[ "$OS_TYPE" == "Linux" ]]; then
    BIN_PATH="$RINDEXER_BIN_DIR/rindexer"
    PLATFORM="linux"
    ARCH_TYPE="amd64"
    EXT="tar.gz"
    if ! command -v unzip &> /dev/null; then
        sudo apt-get update && sudo apt-get install -y unzip
    fi
elif [[ "$OS_TYPE" == "Darwin" ]]; then
    BIN_PATH="$RINDEXER_BIN_DIR/rindexer"
    PLATFORM="darwin"
    # For Darwin, you're using `uname -m` for ARCH_TYPE.
    # If the binary is specifically for 'arm64' or 'amd64', ensure ARCH_TYPE is set correctly.
    # From your output: Downloading binary archive from ...rindexer_darwin-arm64.tar.gz
    # So, if uname -m gives 'arm64', this is fine. If it gives 'x86_64' for Intel Macs, it will be 'amd64'.
    # This seems to be handled correctly in the original script.
    if [[ "$ARCH_TYPE" == "x86_64" ]]; then
        ARCH_TYPE="amd64"
    fi
    EXT="tar.gz"
elif [[ "$OS_TYPE" == "MINGW"* ]] || [[ "$OS_TYPE" == "MSYS"* ]] || [[ "$OS_TYPE" == "CYGWIN"* ]]; then
    PLATFORM="win32"
    EXT="zip"
    BIN_PATH="$RINDEXER_BIN_DIR/rindexer.exe"
else
    echo "Unsupported OS: $OS_TYPE"
    exit 1
fi

# Function to get the latest version from GitHub API
get_latest_version() {
    local latest_version
    latest_version=$(curl -s "https://api.github.com/repos/joshstevens19/rindexer/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/' | sed 's/^v//')
    echo "$latest_version"
}

# Set download URLs
if [[ -n "$VERSION" ]]; then
    BIN_URL="https://github.com/joshstevens19/rindexer/releases/download/v${VERSION}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
else
    # Get latest version if no version specified
    VERSION=$(get_latest_version)
    BIN_URL="https://github.com/joshstevens19/rindexer/releases/download/v${VERSION}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
fi

# --- THE CRUCIAL CHANGE FOR LFS FILES ---
RESOURCES_URL="https://media.githubusercontent.com/media/joshstevens19/rindexer/master/documentation/docs/public/releases/resources.zip"
# --- END OF CRUCIAL CHANGE ---

log() {
   echo -e "\033[1;32m$1\033[0m"
}

error_log() {
    echo -e "\033[1;31m$1\033[0m"
}

spinner() {
    local text="$1"
    local pid=$!
    local delay=0.1
    local spinstr='|/-\'
    log "$text"
    # This spinner logic is still a bit problematic if not carefully managed with background processes
    # For now, it's bypassed in the critical download section by making curl foreground.
    while ps -p "$pid" &>/dev/null; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    echo ""
}

# Install or uninstall based on the command line option
if [[ "$LOCAL_INSTALL" == true ]]; then
    log "Using local binary from $LOCAL_BIN_PATH and resources from $LOCAL_RESOURCES_PATH..."
    cp "$LOCAL_BIN_PATH" "$BIN_PATH"
    unzip -o "$LOCAL_RESOURCES_PATH" -d "$RINDEXER_DIR/resources"
elif [[ "$UNINSTALL" == true ]]; then
    log "Uninstalling rindexer..."
    rm -f "$BIN_PATH" "$RINDEXERUP_PATH"
    rm -rf "$RINDEXER_DIR/resources"
    rmdir "$RINDEXER_BIN_DIR" "$RINDEXER_DIR" 2> /dev/null
    if [[ "$OS_TYPE" == "Darwin" ]]; then
        sed -i '' '/rindexerup/d' "$PROFILE"
        sed -i '' '/rindexer/d' "$PROFILE"
    else
        sed -i '/rindexerup/d' "$PROFILE"
        sed -i '/rindexer/d' "$PROFILE"
    fi
    log "Uninstallation complete! Please restart your shell or source your profile to complete the process."
    exit 0
else
    if [[ -n "$VERSION" ]]; then
        log "Installing rindexer version $VERSION..."
    else
        log "Installing latest rindexer version ($VERSION)..."
    fi
    log "Preparing the installation..."
    mkdir -p "$RINDEXER_BIN_DIR"
    log "Downloading binary archive from $BIN_URL..."

    if ! curl -sSf -L "$BIN_URL" -o "$RINDEXER_DIR/rindexer.${EXT}"; then
        error_log "Failed to download rindexer version $VERSION. Please check if the version exists."
        error_log "Available releases: https://github.com/joshstevens19/rindexer/releases"
        exit 1
    fi

    log "Downloaded binary archive to $RINDEXER_DIR/rindexer.${EXT}"

    log "Extracting binary..."
    if [[ "$EXT" == "tar.gz" ]]; then
        tar -xzvf "$RINDEXER_DIR/rindexer.${EXT}" -C "$RINDEXER_BIN_DIR"
        mv "$RINDEXER_BIN_DIR/rindexer_cli" "$BIN_PATH" || mv "$RINDEXER_BIN_DIR/rindexer" "$BIN_PATH"
    else
        unzip -o "$RINDEXER_DIR/rindexer.${EXT}" -d "$RINDEXER_BIN_DIR"
        mv "$RINDEXER_BIN_DIR/rindexer_cli.exe" "$BIN_PATH" || mv "$RINDEXER_BIN_DIR/rindexer.exe" "$BIN_PATH"
    fi

    log "Extracted binary to $RINDEXER_BIN_DIR"

    mkdir -p "$RINDEXER_DIR/resources"

    log "Downloading resources..."
    # Using the corrected RESOURCES_URL here
    if ! curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR/resources.zip"; then
        error_log "Failed to download resources.zip from $RESOURCES_URL"
        exit 1
    fi
    log "Resources downloaded to $RINDEXER_DIR/resources.zip"

    log "Extracting resources..."
    if ! unzip -o "$RINDEXER_DIR/resources.zip" -d "$RINDEXER_DIR/resources" > /dev/null; then
        error_log "Failed to extract resources.zip. The file might be corrupted or not a valid zip."
        exit 1
    fi
    log "Resources extracted to $RINDEXER_DIR/resources"

    rm "$RINDEXER_DIR/resources.zip"
fi

# Ensure the binary exists before setting permissions
if [ -f "$BIN_PATH" ]; then
    chmod +x "$BIN_PATH"
    log "Binary found and permissions set at $BIN_PATH"
else
    error_log "Error: Binary not found at $BIN_PATH"
    exit 1
fi

# Update PATH in user's profile
PROFILE="${HOME}/.profile"  # Default to .profile
case $SHELL in
    */zsh) PROFILE="${ZDOTDIR:-"$HOME"}/.zshenv" ;;
    */bash) PROFILE="$HOME/.bashrc" ;;
    */fish) PROFILE="$HOME/.config/fish/config.fish" ;;
esac

if [[ ":$PATH:" != *":${RINDEXER_BIN_DIR}:"* ]]; then
    echo "export PATH=\"\$PATH:$RINDEXER_BIN_DIR\"" >> "$PROFILE"
    log "PATH updated in $PROFILE. Please log out and back in or source the profile file."
fi

# Add the rindexerup and rindexerdown commands to the profile
if ! grep -q "alias rindexerup" "$PROFILE"; then
    echo "" >> "$PROFILE"
    echo "# Adding rindexerup and rindexerdown commands" >> "$PROFILE"
    if [[ "$SHELL" == */fish ]]; then
        echo "alias rindexerup 'bash $RINDEXERUP_PATH \$argv'" >> "$PROFILE"
        echo "alias rindexerdown 'bash $RINDEXERDOWN_PATH'" >> "$PROFILE"
    else
        echo "alias rindexerup='bash $RINDEXERUP_PATH \$@'" >> "$PROFILE"
        echo "alias rindexerdown='bash $RINDEXERDOWN_PATH'" >> "$PROFILE"
    fi
fi

# Create or update the rindexerup script to check for updates
cat <<EOF > "$RINDEXERUP_PATH"
#!/usr/bin/env bash
set -eo pipefail

# Parse command line arguments for rindexerup
UPDATE_VERSION=""
while [[ \$# -gt 0 ]]; do
    case \$1 in
        --version)
            UPDATE_VERSION="\$2"
            shift 2
            ;;
        --local)
            LOCAL_UPDATE=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

echo "Updating rindexer..."
if [[ "\$LOCAL_UPDATE" == true ]]; then
    echo "Using local binary for update..."
    cp "$LOCAL_BIN_PATH" "$BIN_PATH"
    unzip -o "$LOCAL_RESOURCES_PATH" -d "$RINDEXER_DIR/resources" > /dev/null
else
    # Function to get the latest version from GitHub API
    get_latest_version() {
        local latest_version
        latest_version=\$(curl -s "https://api.github.com/repos/joshstevens19/rindexer/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/' | sed 's/^v//')
        echo "\$latest_version"
    }

    # Set version for update
    if [[ -n "\$UPDATE_VERSION" ]]; then
        echo "Updating to rindexer version \$UPDATE_VERSION..."
        DOWNLOAD_URL="https://github.com/joshstevens19/rindexer/releases/download/v\${UPDATE_VERSION}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
    else
        UPDATE_VERSION=\$(get_latest_version)
        echo "Updating to latest rindexer version (\$UPDATE_VERSION)..."
        DOWNLOAD_URL="https://github.com/joshstevens19/rindexer/releases/download/v\${UPDATE_VERSION}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
    fi

    echo "Downloading the binary from \$DOWNLOAD_URL..."
    if ! curl -sSf -L "\$DOWNLOAD_URL" -o "$RINDEXER_DIR/rindexer.${EXT}"; then
        echo "Failed to download rindexer version \$UPDATE_VERSION. Please check if the version exists."
        echo "Available releases: https://github.com/joshstevens19/rindexer/releases"
        exit 1
    fi

    if [[ "$EXT" == "tar.gz" ]]; then
        tar -xzvf "$RINDEXER_DIR/rindexer.${EXT}" -C "$RINDEXER_BIN_DIR"
        mv "$RINDEXER_BIN_DIR/rindexer_cli" "$BIN_PATH" || mv "$RINDEXER_BIN_DIR/rindexer" "$BIN_PATH"
    else
        unzip -o "$RINDEXER_DIR/rindexer.${EXT}" -d "$RINDEXER_BIN_DIR"
        mv "$RINDEXER_BIN_DIR/rindexer_cli.exe" "$BIN_PATH" || mv "$RINDEXER_BIN_DIR/rindexer.exe" "$BIN_PATH"
    fi
    mkdir -p "$RINDEXER_DIR/resources"

    echo "Downloading resources..."
    # Using the corrected RESOURCES_URL here within rindexerup
    if ! curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR/resources.zip"; then
        echo "Failed to download resources.zip from $RESOURCES_URL"
        exit 1
    fi
    echo "Resources downloaded to $RINDEXER_DIR/resources.zip"

    echo "Extracting resources..."
    if ! unzip -o "$RINDEXER_DIR/resources.zip" -d "$RINDEXER_DIR/resources" > /dev/null; then
        echo "Failed to extract resources.zip. The file might be corrupted or not a valid zip."
        exit 1
    fi
    echo "Resources extracted to $RINDEXER_DIR/resources"

    rm "$RINDEXER_DIR/resources.zip"
fi
chmod +x "$BIN_PATH"
echo "rindexer has been updated successfully."
EOF

chmod +x "$RINDEXERUP_PATH"

# rindexerdown
cat <<EOF > "$RINDEXERDOWN_PATH"
#!/usr/bin/env bash
set -eo pipefail

echo "Uninstalling rindexer..."
rm -f "$BIN_PATH" "$RINDEXERUP_PATH"
rm -rf "$RINDEXER_DIR/resources"
rmdir "$RINDEXER_BIN_DIR" "$RINDEXER_DIR" 2> /dev/null
if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' '/rindexerup/d' "$PROFILE"
    sed -i '' '/rindexer/d' "$PROFILE"
else
    sed -i '/rindexerup/d' "$PROFILE"
    sed -i '/rindexer/d' "$PROFILE"
fi
echo "rindexer uninstallation complete!"
EOF

chmod +x "$RINDEXERDOWN_PATH"

log ""
log "rindexer has been installed successfully"
log ""
log "To update rindexer run 'rindexerup' (latest) or 'rindexerup --version X.X.X' (specific version)."
log ""
log "To uninstall rindexer run 'rindexerdown'."
log ""
log "Open a new terminal and run 'rindexer' to get started."