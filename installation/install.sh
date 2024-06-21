#!/usr/bin/env bash
set -eo pipefail

BASE_DIR="${XDG_CONFIG_HOME:-$HOME}"
RINDEXER_DIR="${RINDEXER_DIR:-"$BASE_DIR/.rindexer"}"
RINDEXER_BIN_DIR="$RINDEXER_DIR/bin"
RINDEXERUP_PATH="$RINDEXER_BIN_DIR/rindexerup"
RINDEXERDOWN_PATH="$RINDEXER_BIN_DIR/rindexerdown"
BIN_PATH="$RINDEXER_BIN_DIR/rindexer"
BIN_URL="https://rindexer.io/download/rindexer-latest"
LOCAL_BIN_PATH="/Users/joshstevens/code/rindexer/target/debug/rindexer_cli"

# Install or uninstall based on the command line option
case "$1" in
    --local)
        echo "Using local binary from $LOCAL_BIN_PATH..."
        cp "$LOCAL_BIN_PATH" "$BIN_PATH"
        ;;
    --uninstall)
        echo "Uninstalling rindexer..."
        rm -f "$BIN_PATH" "$RINDEXERUP_PATH"
        rmdir "$RINDEXER_BIN_DIR" "$RINDEXER_DIR" 2> /dev/null
        sed -i '' '/rindexerup/d' "$PROFILE"
        sed -i '' '/rindexer/d' "$PROFILE"
        echo "Uninstallation complete! Please restart your shell or source your profile to complete the process."
        exit 0
        ;;
    *)
        echo "Downloading binary from $BIN_URL..."
        curl -sSf -L "$BIN_URL" -o "$BIN_PATH"
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
if [ "\$1" == "--local" ]; then
    echo "Using local binary for update..."
    cp "$LOCAL_BIN_PATH" "$BIN_PATH"
else
    echo "Downloading the latest binary from $BIN_URL..."
    curl -sSf -L "$BIN_URL" -o "$BIN_PATH"
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
rmdir "$RINDEXER_BIN_DIR" "$RINDEXER_DIR" 2> /dev/null
sed -i '' '/rindexerup/d' "$PROFILE"
sed -i '' '/rindexer/d' "$PROFILE"
echo "Uninstallation complete! Please restart your shell or source your profile to complete the process."
EOF

chmod +x "$RINDEXERDOWN_PATH"

echo "Installation complete! Please run 'source $PROFILE' or start a new terminal session to use rindexer."
echo "You can update rindexer anytime by typing 'rindexerup --local' or just 'rindexerup'."
echo "To uninstall rindexer, type 'rindexerdown'."
