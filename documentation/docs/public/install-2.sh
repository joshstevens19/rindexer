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
    EXT="tar.gz"
elif [[ "$OS_TYPE" == "Darwin" ]]; then
    BIN_PATH="$RINDEXER_BIN_DIR/rindexer"
    PLATFORM="darwin"
    EXT="tar.gz"
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

spinner() {
    local pid=$!
    local delay=0.1
    local spinstr='|/-\'
    echo -n "Installing rindexer"
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
        echo "Using local binary from $LOCAL_BIN_PATH and resources from $LOCAL_RESOURCES_PATH..."
        cp "$LOCAL_BIN_PATH" "$BIN_PATH"
        unzip -o "$LOCAL_RESOURCES_PATH" -d "$RINDEXER_DIR/resources"
        ;;
    --uninstall)
        echo "


               _           _
              (_)         | |
          _ __ _ _ __   __| | _____  _____ _ __
         | '__| | '_ \ / _` |/ _ \ \/ / _ \ '__|
         | |  | | | | | (_| |  __/>  <  __/ |
         |_|  |_|_| |_|\__,_|\___/_/\_\___|_|



        "
        rm -f "$BIN_PATH" "$RINDEXERUP_PATH"
        rm -rf "$RINDEXER_DIR/resources"
        rmdir "$RINDEXER_BIN_DIR" "$RINDEXER_DIR" 2> /dev/null
        sed -i '' '/rindexerup/d' "$PROFILE"
        sed -i '' '/rindexer/d' "$PROFILE"
        echo "Uninstallation complete! Please restart your shell or source your profile to complete the process."
        exit 0
        ;;
    *)
        echo "


               _           _
              (_)         | |
          _ __ _ _ __   __| | _____  _____ _ __
         | '__| | '_ \ / _` |/ _ \ \/ / _ \ '__|
         | |  | | | | | (_| |  __/>  <  __/ |
         |_|  |_|_| |_|\__,_|\___/_/\_\___|_|



        "
        echo "Preparing the installation..."
        mkdir -p "$RINDEXER_BIN_DIR"
        curl -sSf -L "$BIN_URL" -o "$RINDEXER_DIR/rindexer.${EXT}"
        if [[ "$EXT" == "tar.gz" ]]; then
            tar -xzvf "$RINDEXER_DIR/rindexer.${EXT}" -C "$RINDEXER_BIN_DIR" & spinner
        else
            unzip -o "$RINDEXER_DIR/rindexer.${EXT}" -d "$RINDEXER_BIN_DIR" & spinner
        fi
        mkdir -p "$RINDEXER_DIR/resources"
        curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR/resources.zip"
        unzip -o "$RINDEXER_DIR/resources.zip" -d "$RINDEXER_DIR/resources" > /dev/null
        rm "$RINDEXER_DIR/resources.zip"
        ;;
esac

chmod +x "$BIN_PATH"

# Update PATH in user's profile
PROFILE="${HOME}/.profile"  # Default to .profile
case $SHELL in
    */zsh) PROFILE="${ZDOTDIR:-"$HOME"}/.zshenv" ;;
    */bash) PROFILE="$HOME/.bashrc" ;;
    */fish) PROFILE="$HOME/.config/fish/config.fish" ;;
esac

if [[ ":$PATH:" != *":${RINDEXER_BIN_DIR}:"* ]]; then
    echo "export PATH=\"\$PATH:$RINDEXER_BIN_DIR\"" >> "$PROFILE"
    echo "PATH updated in $PROFILE. Please log out and back in or source the profile file."
fi

# Add the rindexerup and rindexerdown commands to the profile
echo "# Adding rindexerup and rindexerdown commands" >> "$PROFILE"
echo "alias rindexerup='bash $RINDEXERUP_PATH \$@'" >> "$PROFILE"
echo "alias rindexerdown='bash $RINDEXERDOWN_PATH'" >> "$PROFILE"

# Create or update the rindexerup script to check for updates
cat <<EOF > "$RINDEXERUP_PATH"
#!/usr/bin/env bash
set -eo pipefail

echo "Updating rindexer..."
echo "


       _           _
      (_)         | |
  _ __ _ _ __   __| | _____  _____ _ __
 | '__| | '_ \ / _` |/ _ \ \/ / _ \ '__|
 | |  | | | | | (_| |  __/>  <  __/ |
 |_|  |_|_| |_|\__,_|\___/_/\_\___|_|



"
if [ "\$1" == "--local" ]; then
    echo "Using local binary for update..."
    cp "$LOCAL_BIN_PATH" "$BIN_PATH"
    unzip -o "$LOCAL_RESOURCES_PATH" -d "$RINDEXER_DIR/resources" > /dev/null
else
    echo "Downloading the latest binary from $BIN_URL..."
    curl -sSf -L "$BIN_URL" -o "$RINDEXER_DIR/rindexer.${EXT}"
    if [[ "$EXT" == "tar.gz" ]]; then
        tar -xzvf "$RINDEXER_DIR/rindexer.${EXT}" -C "$RINDEXER_BIN_DIR"
    else
        unzip -o "$RINDEXER_DIR/rindexer.${EXT}" -d "$RINDEXER_BIN_DIR"
    fi
    mkdir -p "$RINDEXER_DIR/resources"
    curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR/resources.zip"
    unzip -o "$RINDEXER_DIR/resources.zip" -d "$RINDEXER_DIR/resources" > /dev/null
    rm "$RINDEXER_DIR/resources.zip"
fi
chmod +x "$BIN_PATH"
echo ""
echo "rindexer has been updated to the latest version."
EOF

chmod +x "$RINDEXERUP_PATH"

# rindexerdown
cat <<EOF > "$RINDEXERDOWN_PATH"
#!/usr/bin/env bash
set -eo pipefail

echo "Uninstalling rindexer..."
echo "


       _           _
      (_)         | |
  _ __ _ _ __   __| | _____  _____ _ __
 | '__| | '_ \ / _` |/ _ \ \/ / _ \ '__|
 | |  | | | | | (_| |  __/>  <  __/ |
 |_|  |_|_| |_|\__,_|\___/_/\_\___|_|



"
rm -f "$BIN_PATH" "$RINDEXERUP_PATH"
rm -rf "$RINDEXER_DIR/resources"
rmdir "$RINDEXER_BIN_DIR" "$RINDEXER_DIR" 2> /dev/null
sed -i '' '/rindexerup/d' "$PROFILE"
sed -i '' '/rindexer/d' "$PROFILE"
echo "rindexer uninstallation complete!"
EOF

chmod +x "$RINDEXERDOWN_PATH"

echo "rindexer has been installed successfully"
echo ""
echo "To update rindexer run 'rindexerup'."
echo ""
echo "To uninstall rindexer run 'rindexerdown'."
echo ""
echo "Open a new terminal and run rindexer to get started."
