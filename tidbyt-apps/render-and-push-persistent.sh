#!/usr/bin/env bash
# Render a .star applet and push it to your Tidbyt device with an installation ID
# so it stays in the device's app rotation.
# Usage: ./render-and-push-persistent.sh <applet.star> [installation-id]

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <applet.star> [installation-id]"
    echo "  installation-id defaults to the filename without extension"
    exit 1
fi

STAR_FILE="$1"
BASENAME="$(basename "${STAR_FILE%.star}")"
WEBP_FILE="${STAR_FILE%.star}.webp"
INSTALLATION_ID="${2:-$BASENAME}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

if [ -z "${TIDBYT_DEVICE_ID:-}" ] || [ "$TIDBYT_DEVICE_ID" = "YOUR_DEVICE_ID_HERE" ]; then
    echo "Error: Set TIDBYT_DEVICE_ID in $SCRIPT_DIR/.env"
    exit 1
fi

if [ -z "${TIDBYT_API_TOKEN:-}" ] || [ "$TIDBYT_API_TOKEN" = "YOUR_API_TOKEN_HERE" ]; then
    echo "Error: Set TIDBYT_API_TOKEN in $SCRIPT_DIR/.env"
    exit 1
fi

echo "Rendering $STAR_FILE..."
pixlet render "$STAR_FILE"

echo "Pushing $WEBP_FILE to device $TIDBYT_DEVICE_ID (installation: $INSTALLATION_ID)..."
pixlet push "$TIDBYT_DEVICE_ID" "$WEBP_FILE" -t "$TIDBYT_API_TOKEN" --installation-id "$INSTALLATION_ID"

echo "Done! Applet '$INSTALLATION_ID' is now in your Tidbyt rotation."
