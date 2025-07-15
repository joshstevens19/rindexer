#!/usr/bin/env bash
set -eo pipefail

VERSION="latest"
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --local|--uninstall)
            COMMAND="$1"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--version VERSION] [--local|--uninstall]"
            exit 1
            ;;
    esac
done

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
    # TODO: once we have arm64 building we can put this line back in
#    if [[ "$ARCH_TYPE" == "aarch64" ]]; then
#        ARCH_TYPE="arm64"
#    else
#        ARCH_TYPE="amd64"
#    fi
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
    ARCH_TYPE="amd64"
    EXT="zip"
    BIN_PATH="$RINDEXER_BIN_DIR/rindexer.exe"
else
    echo "Unsupported OS: $OS_TYPE"
    exit 1
fi

# GitHub releases URLs
GITHUB_REPO="joshstevens19/rindexer"
if [[ "$VERSION" == "latest" ]]; then
    # Get the latest release version from GitHub API
    LATEST_VERSION=$(curl -s https://api.github.com/repos/${GITHUB_REPO}/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    if [[ -z "$LATEST_VERSION" ]]; then
        echo "Error: Could not fetch latest version from GitHub"
        exit 1
    fi
    BIN_URL="https://github.com/${GITHUB_REPO}/releases/download/${LATEST_VERSION}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
    RESOURCES_URL="https://rindexer.xyz/releases/resources.zip"
else
    BIN_URL="https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
    RESOURCES_URL="https://rindexer.xyz/releases/resources.zip"
fi

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
case "$COMMAND" in
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
        if [[ "$VERSION" == "latest" ]]; then
            log "Preparing the installation (latest version: $LATEST_VERSION)..."
        else
            log "Preparing the installation (version $VERSION)..."
        fi
        mkdir -p "$RINDEXER_BIN_DIR"
        log "Downloading binary archive from $BIN_URL..."

        if ! curl -sSf -L --head "$BIN_URL" > /dev/null 2>&1; then
            error_log "Error: Version $VERSION not found or URL not accessible"
            error_log "URL: $BIN_URL"
            exit 1
        fi

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
    */bash)
        PROFILE="$HOME/.bashrc"
        if [ -f "$HOME/.bash_profile" ]; then
            grep -q 'source ~/.bashrc' "$HOME/.bash_profile" || {
                echo -e "# Source .bashrc if it exists\nif [ -f ~/.bashrc ]; then\n    source ~/.bashrc\nfi" >> "$HOME/.bash_profile";
                log "Updated .bash_profile to source .bashrc";
            }
        fi
        ;;
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
    echo "Fetching latest version from GitHub..."
    LATEST_VERSION=\$(curl -s https://api.github.com/repos/${GITHUB_REPO}/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    if [[ -z "\$LATEST_VERSION" ]]; then
        echo "Error: Could not fetch latest version from GitHub"
        exit 1
    fi
    BIN_URL="https://github.com/${GITHUB_REPO}/releases/download/\${LATEST_VERSION}/rindexer_${PLATFORM}-${ARCH_TYPE}.${EXT}"
    echo "Downloading the latest binary (\$LATEST_VERSION) from \$BIN_URL..."
    curl -sSf -L "\$BIN_URL" -o "$RINDEXER_DIR/rindexer.${EXT}"
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
if [[ "$VERSION" == "latest" ]]; then
    log "rindexer has been installed successfully (latest version: $LATEST_VERSION)"
else
    log "rindexer has been installed successfully (version $VERSION)"
fi
log ""
log "To update rindexer run 'rindexerup'."
log ""
log "To uninstall rindexer run 'rindexerdown'."
log ""
log "Open a new terminal and run 'rindexer' to get started."