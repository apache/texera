#!/bin/bash

set -e

DEFAULT_PROVIDER="pylsp"
DEFAULT_PORT=3000

PROVIDER=${1:-$DEFAULT_PROVIDER}
PORT=${2:-$DEFAULT_PORT}

BASE_DIR=$(dirname "$0")
PYRIGHT_DIR="$BASE_DIR/../pyright-language-server"

start_pyright() {
  echo "Starting Pyright Language Server..."
  cd "$PYRIGHT_DIR"
  yarn install --silent
  yarn start
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
