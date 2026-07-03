#!/bin/bash

# setup-client-remote.sh
# Purpose: Installs the tunnel client on macOS and configures it to run as a LaunchAgent.
# Usage: ./setup-client-remote.sh <SERVER_HOST> <TARGET_HOST> <REPO_URL> [CLIENT_NAME] [CERT_DIR] [PARALLEL_SOCKETS]
# Example: ./setup-client-remote.sh t0.arunoda.me:9443 localhost:3000 https://github.com/arunoda/okproxy.git blog ~/.okproxy/certs/blog 4

set -eo pipefail

# Collect positional args
if [ $# -lt 3 ]; then
    echo "Error: SERVER_HOST, TARGET_HOST, and REPO_URL are required."
    echo "Usage: ./setup-client-remote.sh <SERVER_HOST> <TARGET_HOST> <REPO_URL> [CLIENT_NAME] [CERT_DIR] [PARALLEL_SOCKETS]"
    exit 1
fi

SERVER_HOST="$1"
TARGET_HOST="$2"
REPO_URL="$3"
CLIENT_NAME="${4:-default}"
CERT_DIR="${5:-}"
PARALLEL_SOCKETS="${6:-4}"

# Parse server host and port
SERVER_HOSTNAME="${SERVER_HOST%%:*}"
SERVER_PORT="${SERVER_HOST##*:}"
if [ "$SERVER_PORT" = "$SERVER_HOST" ]; then
    SERVER_PORT=9443
fi

# Parse target host and port
TARGET_HOSTNAME="${TARGET_HOST%%:*}"
TARGET_PORT="${TARGET_HOST##*:}"
if [ "$TARGET_PORT" = "$TARGET_HOST" ]; then
    TARGET_PORT=3000
fi

SAFE_CLIENT_NAME=$(printf '%s' "$CLIENT_NAME" | tr -c 'A-Za-z0-9_.-' '-')
if [ -z "$SAFE_CLIENT_NAME" ]; then
    SAFE_CLIENT_NAME="default"
fi

APP_DIR="$HOME/okproxy"
CLIENT_DIR="$APP_DIR/apps/client"
if [ -z "$CERT_DIR" ]; then
    if [ "$SAFE_CLIENT_NAME" = "default" ]; then
        CERT_DIR="$HOME/.okproxy/certs"
    else
        CERT_DIR="$HOME/.okproxy/certs/$SAFE_CLIENT_NAME"
    fi
elif [[ "$CERT_DIR" == ~/* ]]; then
    CERT_DIR="$HOME/${CERT_DIR#~/}"
fi
if [ "$SAFE_CLIENT_NAME" = "default" ]; then
    LOG_DIR="$HOME/.okproxy/logs"
    LAUNCH_LABEL="com.okproxy.client"
else
    LOG_DIR="$HOME/.okproxy/logs/$SAFE_CLIENT_NAME"
    LAUNCH_LABEL="com.okproxy.client.$SAFE_CLIENT_NAME"
fi
PLIST_PATH="$HOME/Library/LaunchAgents/${LAUNCH_LABEL}.plist"

if ! [[ "$PARALLEL_SOCKETS" =~ ^[0-9]+$ ]] || [ "$PARALLEL_SOCKETS" -lt 1 ] || [ "$PARALLEL_SOCKETS" -gt 32 ]; then
    echo "Error: PARALLEL_SOCKETS must be an integer from 1 to 32."
    exit 1
fi

echo "Starting setup for OKProxy Client on macOS..."
echo "App Directory: $APP_DIR"
echo "Server: $SERVER_HOSTNAME:$SERVER_PORT"
echo "Target: $TARGET_HOSTNAME:$TARGET_PORT"
echo "Repository: $REPO_URL"
echo "Client name: $SAFE_CLIENT_NAME"
echo "Parallel sockets per interface: $PARALLEL_SOCKETS"
echo "Cert directory: $CERT_DIR"

# 1. Check for and install Node.js if needed
echo "Checking for Node.js..."

LOCAL_NODE="$HOME/.local/bin/node"

# If OKPROXY_NODE_PATH is set and exists, skip all detection and installation
if [ -n "$OKPROXY_NODE_PATH" ]; then
    if [ -x "$OKPROXY_NODE_PATH" ]; then
        echo "Using custom Node.js path from OKPROXY_NODE_PATH: $OKPROXY_NODE_PATH"
        echo "Skipping Node.js detection and installation."
        NODE_PATH="$OKPROXY_NODE_PATH"
    else
        echo "Error: OKPROXY_NODE_PATH is set but executable not found: $OKPROXY_NODE_PATH"
        exit 1
    fi
else

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    arm64)
        NODE_ARCH="darwin-arm64"
        ;;
    x86_64)
        NODE_ARCH="darwin-x64"
        ;;
    *)
        echo "Error: Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Fetch latest LTS version from official Node.js releases JSON
LTS_DATA=$(curl -fsSL https://nodejs.org/dist/index.json 2>/dev/null | head -c 10000 || echo "")

if [ -n "$LTS_DATA" ]; then
    TARGET_VERSION=$(echo "$LTS_DATA" | grep -oE '\{"version":"v[0-9]+\.[^}]*"lts":"[^"]+"[^}]*\}' | head -1 | grep -oE '"version":"v[0-9]+' | grep -oE 'v[0-9]+')
    TARGET_MAJOR=$(echo "$TARGET_VERSION" | grep -oE '[0-9]+')

    if [ -n "$TARGET_MAJOR" ]; then
        TARGET_NODE_VERSION="v${TARGET_MAJOR}"
        echo "Latest LTS detected: Node.js ${TARGET_NODE_VERSION}.x"
    else
        TARGET_NODE_VERSION="v22"
        TARGET_MAJOR="22"
        echo "Could not detect latest LTS. Using fallback: Node.js ${TARGET_NODE_VERSION}"
    fi
else
    TARGET_NODE_VERSION="v22"
    TARGET_MAJOR="22"
    echo "Could not fetch LTS info. Using fallback: Node.js ${TARGET_NODE_VERSION}"
fi

# Check only our custom Node.js installation at ~/.local/bin/node (ignore system Node.js)
INSTALL_NODE=false

if [ -x "$LOCAL_NODE" ]; then
    CURRENT_NODE_VERSION=$("$LOCAL_NODE" -v)
    echo "Found custom Node.js installation: ${CURRENT_NODE_VERSION}"

    # Check if current major version matches target
    if [[ "${CURRENT_NODE_VERSION}" == ${TARGET_NODE_VERSION}* ]]; then
        echo "Node.js is already at target LTS version ${TARGET_NODE_VERSION}."
        INSTALL_NODE=false
    else
        echo "Node.js version mismatch. Target: ${TARGET_NODE_VERSION}, Current: ${CURRENT_NODE_VERSION}"
        echo "Will update to Node.js ${TARGET_NODE_VERSION}..."
        INSTALL_NODE=true
    fi
else
    echo "Custom Node.js not found at $LOCAL_NODE. Will install Node.js ${TARGET_NODE_VERSION}..."
    INSTALL_NODE=true
fi

if [ "$INSTALL_NODE" = true ]; then
    echo "Installing Node.js ${TARGET_NODE_VERSION}.x from official Node.js distribution..."

    NODE_VERSION_FULL=$(curl -fsSL "https://nodejs.org/dist/latest-${TARGET_NODE_VERSION}.x/" 2>/dev/null | grep -oE "node-${TARGET_NODE_VERSION}\.[0-9]+\.[0-9]+-${NODE_ARCH}\.tar\.gz" | head -1 | sed "s/node-//;s/-${NODE_ARCH}\.tar\.gz//")
    if [ -z "$NODE_VERSION_FULL" ]; then
        NODE_VERSION_FULL="${TARGET_NODE_VERSION}.15.0"
        echo "Could not detect latest ${TARGET_NODE_VERSION}.x version. Using fallback: ${NODE_VERSION_FULL}"
    else
        echo "Installing Node.js ${NODE_VERSION_FULL}..."
    fi

    NODE_TARBALL="node-${NODE_VERSION_FULL}-${NODE_ARCH}.tar.gz"
    # NODE_VERSION_FULL already includes 'v' prefix, don't add another
    NODE_URL="https://nodejs.org/dist/${NODE_VERSION_FULL}/${NODE_TARBALL}"
    SHASUMS_URL="https://nodejs.org/dist/${NODE_VERSION_FULL}/SHASUMS256.txt"

    # Create temp directory for downloads
    TEMP_DIR=$(mktemp -d)
    trap "rm -rf $TEMP_DIR" EXIT

    # Download tarball and checksums
    echo "Downloading Node.js ${NODE_VERSION_FULL} for ${ARCH}..."
    curl -fsSL "$NODE_URL" -o "$TEMP_DIR/$NODE_TARBALL"
    curl -fsSL "$SHASUMS_URL" -o "$TEMP_DIR/SHASUMS256.txt"

    # Verify SHA256 checksum
    echo "Verifying SHA256 checksum..."
    EXPECTED_HASH=$(grep "$NODE_TARBALL" "$TEMP_DIR/SHASUMS256.txt" | awk '{print $1}')
    if [ -z "$EXPECTED_HASH" ]; then
        echo "Error: Could not find checksum for $NODE_TARBALL"
        exit 1
    fi

    ACTUAL_HASH=$(shasum -a 256 "$TEMP_DIR/$NODE_TARBALL" | awk '{print $1}')
    if [ "$EXPECTED_HASH" != "$ACTUAL_HASH" ]; then
        echo "Error: SHA256 checksum verification failed!"
        echo "Expected: $EXPECTED_HASH"
        echo "Actual:   $ACTUAL_HASH"
        exit 1
    fi
    echo "SHA256 checksum verified."

    # Remove any existing local Node.js installation
    if [ -d "$HOME/.local/lib/node" ]; then
        echo "Removing existing Node.js installation..."
        rm -rf "$HOME/.local/lib/node" "$HOME/.local/bin/node" "$HOME/.local/bin/npm" "$HOME/.local/bin/npx" 2>/dev/null || true
    fi

    # Extract tarball to ~/.local
    echo "Extracting Node.js to ~/.local..."
    mkdir -p "$HOME/.local"
    tar -xz -C "$HOME/.local" --strip-components=1 -f "$TEMP_DIR/$NODE_TARBALL"

    # Verify installation
    if [ -x "$LOCAL_NODE" ]; then
        echo "Node.js installed successfully: $($LOCAL_NODE -v)"
        echo "npm version: $("$HOME/.local/bin/npm" -v)"
    else
        echo "Error: Node.js installation failed"
        exit 1
    fi

# End of INSTALL_NODE block
fi
fi

# If custom Node.js path wasn't set, use the default local installation
if [ -z "$NODE_PATH" ]; then
    # Determine the Node.js executable path for the LaunchAgent plist
    NODE_PATH="$LOCAL_NODE"
    if [ ! -x "$NODE_PATH" ]; then
        echo "Error: Node.js executable not found at $NODE_PATH"
        echo "Install Node.js first, or set OKPROXY_NODE_PATH to the correct binary"
        exit 1
    fi
fi
echo "Using Node.js at: $NODE_PATH ($($NODE_PATH -v))"

# 2. Create necessary directories
echo "Creating directories..."
mkdir -p "$CERT_DIR"
mkdir -p "$LOG_DIR"

# 3. Clone or update repository
echo "Setting up repository..."
if [ -d "$APP_DIR/.git" ]; then
    echo "App directory exists. Updating repository..."
    cd "$APP_DIR"
    git fetch origin
    git reset --hard "origin/main"
    echo "Repository updated."
else
    echo "App directory does not exist. Cloning repository..."
    if [ -d "$APP_DIR" ]; then
        rm -rf "$APP_DIR"
    fi
    git clone "$REPO_URL" "$APP_DIR"
    echo "Repository cloned."
fi

# 4. Check/generate certificates (if not uploaded)
CLIENT_CERT="$CERT_DIR/client-cert.pem"
CLIENT_KEY="$CERT_DIR/client-key.pem"
CA_CERT="$CERT_DIR/ca-cert.pem"

if [ ! -f "$CLIENT_CERT" ] || [ ! -f "$CLIENT_KEY" ]; then
    echo "Client certificates not found in $CERT_DIR"
    echo "Please run setup-client.sh with --upload-certs from your server machine"
    echo "Or manually copy the certificates to $CERT_DIR:"
    echo "  - client-cert.pem"
    echo "  - client-key.pem"
    echo "  - ca-cert.pem"
    exit 1
fi

if [ ! -f "$CA_CERT" ]; then
    echo "CA certificate not found in $CERT_DIR"
    echo "Please run setup-client.sh with --upload-certs from your server machine"
    exit 1
fi

echo "Certificates verified at $CERT_DIR"

# 5. Unload existing LaunchAgent if present
echo "Checking for existing LaunchAgent..."
if launchctl list "$LAUNCH_LABEL" &> /dev/null; then
    echo "Unloading existing LaunchAgent..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
fi

# Remove old plist if exists
if [ -f "$PLIST_PATH" ]; then
    rm "$PLIST_PATH"
fi

# 6. Create LaunchAgent plist
echo "Creating LaunchAgent plist..."
cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LAUNCH_LABEL}</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>${NODE_PATH}</string>
        <string>${CLIENT_DIR}/index.js</string>
        <string>--multipath</string>
        <string>--server</string>
        <string>${SERVER_HOSTNAME}:${SERVER_PORT}</string>
        <string>--target</string>
        <string>${TARGET_HOSTNAME}:${TARGET_PORT}</string>
        <string>--parallel-sockets</string>
        <string>${PARALLEL_SOCKETS}</string>
        <string>--cert</string>
        <string>${CLIENT_CERT}</string>
        <string>--key</string>
        <string>${CLIENT_KEY}</string>
        <string>--ca</string>
        <string>${CA_CERT}</string>
    </array>
    
    <key>WorkingDirectory</key>
    <string>${CLIENT_DIR}</string>
    
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/client.log</string>
    
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/client-error.log</string>
    
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
        <key>Crashed</key>
        <true/>
    </dict>
    
    <key>RunAtLoad</key>
    <true/>
    
    <key>ThrottleInterval</key>
    <integer>10</integer>
    
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>NODE_ENV</key>
        <string>production</string>
        <key>MULTIPATH_ENABLED</key>
        <string>true</string>
        <key>OKPROXY_PARALLEL_SOCKETS</key>
        <string>${PARALLEL_SOCKETS}</string>
    </dict>
</dict>
</plist>
EOF

# 7. Load and start the LaunchAgent
echo "Loading LaunchAgent..."
launchctl load "$PLIST_PATH"

# 8. Start the service
echo "Starting tunnel client..."
launchctl start "$LAUNCH_LABEL"

# 9. Wait a moment and check status
sleep 2

echo ""
echo "Running health checks..."

# Check if LaunchAgent is loaded
if launchctl list "$LAUNCH_LABEL" &> /dev/null; then
    echo "✓ LaunchAgent is loaded"
else
    echo "✗ LaunchAgent is NOT loaded"
    exit 1
fi

# Check if process is running
PID=$(launchctl list "$LAUNCH_LABEL" 2>/dev/null | grep '"PID"' | awk '{print $NF}' | tr -d ';')
if [ -n "$PID" ] && [ "$PID" -gt 0 ] 2>/dev/null; then
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "✓ Client process is running (PID: $PID)"
    else
        echo "⚠ Client process not found (PID: $PID may have exited)"
    fi
else
    echo "⚠ Client may not be running (checking logs...)"
fi

# Check recent logs
if [ -f "$LOG_DIR/client.log" ]; then
    echo ""
    echo "Recent log entries:"
    tail -n 5 "$LOG_DIR/client.log"
fi

if [ -f "$LOG_DIR/client-error.log" ]; then
    ERROR_LOG=$(cat "$LOG_DIR/client-error.log")
    if [ -n "$ERROR_LOG" ]; then
        echo ""
        echo "Error log entries:"
        cat "$LOG_DIR/client-error.log"
    fi
fi

echo ""
echo "Setup completed successfully!"
echo ""
echo "The tunnel client is configured to:"
echo "  - Connect to: $SERVER_HOSTNAME:$SERVER_PORT"
echo "  - Forward to: $TARGET_HOSTNAME:$TARGET_PORT"
echo "  - Certificate dir: $CERT_DIR"
echo ""
echo "Management commands:"
echo "  Check status: launchctl list $LAUNCH_LABEL"
echo "  Start:        launchctl start $LAUNCH_LABEL"
echo "  Stop:         launchctl stop $LAUNCH_LABEL"
echo "  View logs:    tail -f $LOG_DIR/client.log"
echo "  View errors:  tail -f $LOG_DIR/client-error.log"
echo ""
echo "The client will automatically start on login and restart on crashes."
