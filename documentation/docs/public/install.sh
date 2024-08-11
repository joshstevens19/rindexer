#!/usr/bin/env bash
set -eo pipefail

BASE_DIR="${XDG_CONFIG_HOME:-$HOME}"
RINDEXER_DIR="${RINDEXER_DIR:-"$BASE_DIR/.rindexer"}"
RINDEXER_BIN_DIR="$RINDEXER_DIR/bin"
RINDEXERUP_PATH="$RINDEXER_BIN_DIR/rindexerup"
RINDEXERDOWN_PATH="$RINDEXER_BIN_DIR/rindexerdown"
OS_TYPE=$(uname)
ARCH_TYPE=$(uname -m)

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
    EXT="tar.gz"
    if [[ "$ARCH_TYPE" != "arm64" ]]; then
        ARCH_TYPE="amd64"
    fi
elif [[ "$OS_TYPE" == "MINGW"* ]] || [[ "$OS_TYPE" == "MSYS"* ]] || [[ "$OS_TYPE" == "CYGWIN"* ]]; then
    PLATFORM="win32"
    EXT="zip"
    BIN_PATH="$RINDEXER_BIN_DIR/rindexer.exe"
else
    echo "Unsupported OS: $OS_TYPE"
    exit 1
fi

BIN_URL="https://rindexer.xyz/releases/${PLATFORM}-${ARCH_TYPE}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
RESOURCES_URL="https://rindexer.xyz/releases/resources.zip"

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
    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    echo ""
}

# Install or uninstall based on the command line option
case "$1" in
    --local)
        log "Using local binary from $LOCAL_BIN_PATH and resources from $LOCAL_RESOURCES_PATH..."
        cp "$LOCAL_BIN_PATH" "$BIN_PATH"
        unzip -o "$LOCAL_RESOURCES_PATH" -d "$RINDEXER_DIR/resources"
        ;;
    --uninstall)
        log "Uninstalling rindexer..."
        rm -f "$BIN_PATH" "$RINDEXERUP_PATH"
        rm -rf "$RINDEXER_DIR/resources"
        rmdir "$RINDEXER_BIN_DIR" "$RINDEXER_DIR" 2> /dev/null
        sed -i '' '/rindexerup/d' "$PROFILE"
        sed -i '' '/rindexer/d' "$PROFILE"
        log "Uninstallation complete! Please restart your shell or source your profile to complete the process."
        exit 0
        ;;
    *)
        log "Preparing the installation..."
        mkdir -p "$RINDEXER_BIN_DIR"
        log "Downloading binary archive from $BIN_URL..."
        curl -sSf -L "$BIN_URL" -o "$RINDEXER_DIR/rindexer.${EXT}"
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
        curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR/resources.zip" & spinner "Downloading resources..."
        if [[ "$OS_TYPE" == "Linux" ]]; then
            unzip -o "$RINDEXER_DIR/resources.zip" -d "$RINDEXER_DIR/resources" > /dev/null
        else
            tar -xzf "$RINDEXER_DIR/resources.zip" -C "$RINDEXER_DIR/resources" > /dev/null
        fi
        rm "$RINDEXER_DIR/resources.zip"
        ;;
esac

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
echo "# Adding rindexerup and rindexerdown commands" >> "$PROFILE"
if [[ "$SHELL" == */fish ]]; then
    echo "alias rindexerup 'bash $RINDEXERUP_PATH \$argv'" >> "$PROFILE"
    echo "alias rindexerdown 'bash $RINDEXERDOWN_PATH'" >> "$PROFILE"
else
    echo "alias rindexerup='bash $RINDEXERUP_PATH \$@'" >> "$PROFILE"
    echo "alias rindexerdown='bash $RINDEXERDOWN_PATH'" >> "$PROFILE"
fi

# Create or update the rindexerup script to check for updates
cat <<EOF > "$RINDEXERUP_PATH"
#!/usr/bin/env bash
set -eo pipefail

echo "Updating rindexer..."
if [ "\$1" == "--local" ]; then
    echo "Using local binary for update..."
    cp "$LOCAL_BIN_PATH" "$BIN_PATH"
    unzip -o "$LOCAL_RESOURCES_PATH" -d "$RINDEXER_DIR/resources" > /dev/null
else
    echo "Downloading the latest binary from $BIN_URL..."
    curl -sSf -L "$BIN_URL" -o "$RINDEXER_DIR/rindexer.${EXT}"
    if [[ "$EXT" == "tar.gz" ]]; then
        tar -xzvf "$RINDEXER_DIR/rindexer.${EXT}" -C "$RINDEXER_BIN_DIR"
        mv "$RINDEXER_BIN_DIR/rindexer_cli" "$BIN_PATH" || mv "$RINDEXER_BIN_DIR/rindexer" "$BIN_PATH"
    else
        unzip -o "$RINDEXER_DIR/rindexer.${EXT}" -d "$RINDEXER_BIN_DIR"
        mv "$RINDEXER_BIN_DIR/rindexer_cli.exe" "$BIN_PATH" || mv "$RINDEXER_BIN_DIR/rindexer.exe" "$BIN_PATH"
    fi
    mkdir -p "$RINDEXER_DIR/resources"
    curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR/resources.zip"
    if [[ "$OS_TYPE" == "Linux" ]]; then
        unzip -o "$RINDEXER_DIR/resources.zip" -d "$RINDEXER_DIR/resources" > /dev/null
    else
        tar -xzf "$RINDEXER_DIR/resources.zip" -C "$RINDEXER_DIR/resources" > /dev/null
    fi
    rm "$RINDEXER_DIR/resources.zip"
fi
chmod +x "$BIN_PATH"
echo "rindexer has been updated to the latest version."
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
sed -i '' '/rindexerup/d' "$PROFILE"
sed -i '' '/rindexer/d' "$PROFILE"
echo "rindexer uninstallation complete!"
EOF

chmod +x "$RINDEXERDOWN_PATH"

log ""
log "rindexer has been installed successfully"
log ""
log "To update rindexer run 'rindexerup'."
log ""
log "To uninstall rindexer run 'rindexerdown'."
log ""
log "Open a new terminal and run 'rindexer' to get started."
