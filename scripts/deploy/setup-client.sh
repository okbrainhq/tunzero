#!/bin/bash

# setup-client.sh
# Purpose: Orchestrates the setup of a tunnel client on a remote MacBook.
# Usage: ./scripts/deploy/setup-client.sh [USER@HOST] [--upload-certs] [--cert-dir <dir>] [--client-name <name>]
# Optional .deploy.client: PARALLEL_SOCKETS=4 (default)

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Read config from .deploy.client file
if [ -f "$PROJECT_ROOT/.deploy.client" ]; then
    source "$PROJECT_ROOT/.deploy.client"
else
    echo "Error: .deploy.client file not found in project root."
    echo "Create one with SERVER_HOST and TARGET_HOST variables."
    exit 1
fi

# Validate required config
if [ -z "$SERVER_HOST" ]; then
    echo "Error: SERVER_HOST not set in .deploy.client"
    exit 1
fi

if [ -z "$TARGET_HOST" ]; then
    echo "Error: TARGET_HOST not set in .deploy.client"
    exit 1
fi

if [ -z "$REPO_URL" ]; then
    echo "Error: REPO_URL not set in .deploy.client"
    exit 1
fi

# Parse command line arguments
HOST=""
UPLOAD_CERTS=false
CLIENT_NAME=${CLIENT_NAME:-default}
CLIENT_CERT_DIR=${CLIENT_CERT_DIR:-$PROJECT_ROOT/.certs}
REMOTE_CERT_DIR=${REMOTE_CERT_DIR:-}
PARALLEL_SOCKETS=${PARALLEL_SOCKETS:-4}

while [ $# -gt 0 ]; do
    arg="$1"
    case "$arg" in
        --upload-certs)
            UPLOAD_CERTS=true
            ;;
        --cert-dir)
            shift
            CLIENT_CERT_DIR="$1"
            ;;
        --client-name)
            shift
            CLIENT_NAME="$1"
            ;;
        --remote-cert-dir)
            shift
            REMOTE_CERT_DIR="$1"
            ;;
        --*)
            # Unknown flag
            ;;
        *)
            # Assume it's the host
            HOST="$arg"
            ;;
    esac
    shift
done

# Resolve relative local cert directories from the project root
if [[ "$CLIENT_CERT_DIR" != /* ]]; then
    CLIENT_CERT_DIR="$PROJECT_ROOT/${CLIENT_CERT_DIR#./}"
fi

# If no host provided, check if DEPLOY_HOST is set in .deploy.client
if [ -z "$HOST" ] && [ -n "$DEPLOY_HOST" ]; then
    HOST="$DEPLOY_HOST"
fi

if [ -z "$HOST" ]; then
    echo "Error: No host specified and DEPLOY_HOST not set in .deploy.client file."
    echo "Usage: ./scripts/deploy/setup-client.sh [USER@HOST] [--upload-certs] [--cert-dir <dir>] [--client-name <name>]"
    echo "Or set DEPLOY_HOST in .deploy.client file."
    exit 1
fi

if ! [[ "$PARALLEL_SOCKETS" =~ ^[0-9]+$ ]] || [ "$PARALLEL_SOCKETS" -lt 1 ] || [ "$PARALLEL_SOCKETS" -gt 32 ]; then
    echo "Error: PARALLEL_SOCKETS must be an integer from 1 to 32."
    exit 1
fi

echo "Setting up OKProxy Client on $HOST..."
echo "Server: $SERVER_HOST"
echo "Target: $TARGET_HOST"
echo "Repository: $REPO_URL"
echo "Client name: $CLIENT_NAME"
echo "Parallel sockets per interface: $PARALLEL_SOCKETS"
echo "Local cert dir: $CLIENT_CERT_DIR"

# Build SSH/SCP port options
SSH_OPTS=""
SCP_OPTS=""
if [ -n "$SSH_PORT" ] && [ "$SSH_PORT" != "22" ]; then
    SSH_OPTS="-p $SSH_PORT"
    SCP_OPTS="-P $SSH_PORT"
    echo "Using custom SSH port: $SSH_PORT"
fi

# 1. Copy setup script to remote MacBook
echo "Copying setup-client-remote.sh..."
scp $SCP_OPTS "$SCRIPT_DIR/setup-client-remote.sh" "$HOST:~/setup-client-remote.sh"

# 2. Upload certificates if requested
# Note: Using ~ for remote home directory (will be expanded on remote side)
if [ -z "$REMOTE_CERT_DIR" ]; then
    if [ "$CLIENT_NAME" = "default" ]; then
        REMOTE_CERT_DIR="~/.okproxy/certs"
    else
        REMOTE_CERT_DIR="~/.okproxy/certs/$CLIENT_NAME"
    fi
fi
if [ "$UPLOAD_CERTS" = true ]; then
    echo "Validating and uploading certificates..."
    
    # Check local cert directories exist
    if [ ! -d "$CLIENT_CERT_DIR" ]; then
        echo "Error: client cert directory not found: $CLIENT_CERT_DIR"
        echo "Generate certificates first: npx ca init"
        exit 1
    fi
    if [ ! -d "$PROJECT_ROOT/.ca" ]; then
        echo "Error: .ca directory not found in project root"
        echo "Generate certificates first: npx ca init"
        exit 1
    fi

    # Check required client certificate files exist
    if [ ! -f "$CLIENT_CERT_DIR/client-cert.pem" ] || [ ! -f "$CLIENT_CERT_DIR/client-key.pem" ]; then
        echo "Error: Client certificates not found in $CLIENT_CERT_DIR"
        echo "Required files: client-cert.pem, client-key.pem"
        echo "Generate with: npx ca issue-client --domain <domain> --output <dir>"
        exit 1
    fi

    # Check CA certificate exists
    if [ ! -f "$PROJECT_ROOT/.ca/ca-cert.pem" ]; then
        echo "Error: CA certificate not found in .ca/"
        echo "Required file: ca-cert.pem"
        echo "Generate with: npx ca init"
        exit 1
    fi
    
    echo "Local certificates validated."
    echo "Uploading certificates to remote MacBook..."
    
    # Create remote cert directory (use ~ for remote expansion)
    ssh $SSH_OPTS "$HOST" "mkdir -p $REMOTE_CERT_DIR"
    
    # Upload certificates using scp
    scp $SCP_OPTS "$CLIENT_CERT_DIR/client-cert.pem" "$HOST:$REMOTE_CERT_DIR/"
    scp $SCP_OPTS "$CLIENT_CERT_DIR/client-key.pem" "$HOST:$REMOTE_CERT_DIR/"
    scp $SCP_OPTS "$PROJECT_ROOT/.ca/ca-cert.pem" "$HOST:$REMOTE_CERT_DIR/"
    
    # Fix permissions (private key should be restricted)
    ssh $SSH_OPTS "$HOST" "chmod 600 $REMOTE_CERT_DIR/client-key.pem && chmod 644 $REMOTE_CERT_DIR/client-cert.pem $REMOTE_CERT_DIR/ca-cert.pem"
    
    echo "Certificates uploaded successfully to $REMOTE_CERT_DIR"
fi

# 3. Execute setup script remotely
echo "Executing setup script on remote MacBook..."
# Use printf %q to properly escape arguments to prevent shell injection
ESCAPED_SERVER_HOST=$(printf '%q' "$SERVER_HOST")
ESCAPED_TARGET_HOST=$(printf '%q' "$TARGET_HOST")
ESCAPED_REPO_URL=$(printf '%q' "$REPO_URL")
ESCAPED_CLIENT_NAME=$(printf '%q' "$CLIENT_NAME")
ESCAPED_REMOTE_CERT_DIR=$(printf '%q' "$REMOTE_CERT_DIR")
ESCAPED_PARALLEL_SOCKETS=$(printf '%q' "$PARALLEL_SOCKETS")
ssh $SSH_OPTS "$HOST" "chmod +x ~/setup-client-remote.sh && ~/setup-client-remote.sh $ESCAPED_SERVER_HOST $ESCAPED_TARGET_HOST $ESCAPED_REPO_URL $ESCAPED_CLIENT_NAME $ESCAPED_REMOTE_CERT_DIR $ESCAPED_PARALLEL_SOCKETS"

if [ "$CLIENT_NAME" = "default" ]; then
    LAUNCH_LABEL="com.okproxy.client"
    CLIENT_LOG_PATH="~/.okproxy/logs/client.log"
else
    LAUNCH_LABEL="com.okproxy.client.$CLIENT_NAME"
    CLIENT_LOG_PATH="~/.okproxy/logs/$CLIENT_NAME/client.log"
fi

echo ""
echo "Client setup completed successfully on $HOST!"
echo "The tunnel client will start automatically on login and restart on crashes."
echo ""
if [ -n "$SSH_OPTS" ]; then
    echo "To check status: ssh $SSH_OPTS $HOST 'launchctl list $LAUNCH_LABEL'"
    echo "To view logs: ssh $SSH_OPTS $HOST 'tail -f $CLIENT_LOG_PATH'"
else
    echo "To check status: ssh $HOST 'launchctl list $LAUNCH_LABEL'"
    echo "To view logs: ssh $HOST 'tail -f $CLIENT_LOG_PATH'"
fi
