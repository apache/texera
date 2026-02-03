#!/usr/bin/env python3
"""
Parse storage.conf HOCON file with environment variable resolution.
This script properly handles HOCON syntax including environment variable substitution.

Usage:
    python3 bin/parse-storage-config.py [key_path]
    
Examples:
    python3 bin/parse-storage-config.py storage.iceberg.catalog.rest.uri
    python3 bin/parse-storage-config.py storage.s3.endpoint
"""

import os
import sys
from pathlib import Path

try:
    from pyhocon import ConfigFactory
except ImportError:
    print("Error: pyhocon is not installed. Install it with: pip install pyhocon", file=sys.stderr)
    sys.exit(1)


def find_storage_conf():
    """Find storage.conf path."""
    texera_home = os.environ.get("TEXERA_HOME")
    if texera_home:
        conf_path = Path(texera_home) / "common" / "config" / "src" / "main" / "resources" / "storage.conf"
    else:
        # Assume we're in the project root
        script_dir = Path(__file__).parent
        conf_path = script_dir.parent / "common" / "config" / "src" / "main" / "resources" / "storage.conf"
    
    if not conf_path.exists():
        print(f"Error: storage.conf not found at {conf_path}", file=sys.stderr)
        sys.exit(1)
    
    return conf_path


def parse_storage_config():
    """Parse storage.conf with environment variable resolution."""
    conf_path = find_storage_conf()
    
    # pyhocon automatically resolves environment variables
    # Environment variables are available in os.environ
    config = ConfigFactory.parse_file(str(conf_path))
    
    return config


def get_value(config, key_path):
    """Get value from config by key path (e.g., 'storage.iceberg.catalog.rest.uri')."""
    keys = key_path.split(".")
    value = config
    for key in keys:
        if hasattr(value, key):
            value = getattr(value, key)
        elif key in value:
            value = value[key]
        else:
            return None
    return value


def main():
    if len(sys.argv) > 1:
        key_path = sys.argv[1]
        config = parse_storage_config()
        value = get_value(config, key_path)
        if value is None:
            print(f"Key '{key_path}' not found", file=sys.stderr)
            sys.exit(1)
        print(value)
    else:
        # Print all storage config
        config = parse_storage_config()
        print(config.get("storage", {}))


if __name__ == "__main__":
    main()
