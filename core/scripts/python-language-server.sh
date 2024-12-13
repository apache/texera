#!/bin/bash

set -e

DEFAULT_PROVIDER="pylsp"
DEFAULT_PORT=3000

PROVIDER=""
PORT=""

BASE_DIR=$(dirname "$0")
PYRIGHT_DIR="$BASE_DIR/../pyright-language-server"

while [ $# -gt 0 ]; do
  case "$1" in
    --server=*|--provider=*)
      PROVIDER="${1#*=}"
      ;;
    --port=*)
      PORT="${1#*=}"
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--server=<pyright|pylsp>] [--port=<port_number>]"
      exit 1
      ;;
  esac
  shift
done

PROVIDER="${PROVIDER:-$DEFAULT_PROVIDER}"
PORT="${PORT:-$DEFAULT_PORT}"

# Validate port value
if ! [[ "$PORT" =~ ^[0-9]+$ ]]; then
  echo "Invalid port: $PORT. Must be a number."
  exit 1
fi

start_pyright() {
  echo "Starting Pyright Language Server on port $PORT..."
  cd "$PYRIGHT_DIR"
  yarn install --silent
  yarn start --port="$PORT"
}

start_pylsp() {
  echo "Starting Pylsp Language Server on port $PORT..."
  if ! command -v pylsp &>/dev/null; then
    echo "Error: pylsp is not installed. Install it with 'pip install python-lsp-server'."
    exit 1
  fi
  pylsp --port "$PORT" --ws
}

case $PROVIDER in
  pyright)
    start_pyright
    ;;
  pylsp)
    start_pylsp
    ;;
  *)
    echo "Invalid provider: $PROVIDER. Valid options are: pyright, pylsp."
    exit 1
    ;;
esac
