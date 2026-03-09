#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Parse storage.conf HOCON file with environment variable resolution.
This script properly handles HOCON syntax including environment variable substitution.

Usage:
    # Single key mode (backward compatible):
    python3 bin/parse-storage-config.py storage.iceberg.catalog.rest.uri

    # Batch mode (outputs VAR_NAME=value lines):
    python3 bin/parse-storage-config.py --batch VAR1=key.path1 VAR2=key.path2 ...

Examples:
    python3 bin/parse-storage-config.py storage.s3.endpoint
    python3 bin/parse-storage-config.py --batch \
        REST_URI=storage.iceberg.catalog.rest.uri \
        WAREHOUSE_NAME=storage.iceberg.catalog.rest.warehouse-name \
        S3_ENDPOINT=storage.s3.endpoint
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
        script_dir = Path(__file__).parent
        conf_path = script_dir.parent / "common" / "config" / "src" / "main" / "resources" / "storage.conf"

    if not conf_path.exists():
        print(f"Error: storage.conf not found at {conf_path}", file=sys.stderr)
        sys.exit(1)

    return conf_path


def parse_storage_config():
    """Parse storage.conf with environment variable resolution."""
    conf_path = find_storage_conf()
    config = ConfigFactory.parse_file(str(conf_path))
    return config


def get_value(config, key_path):
    """Get value from config by dot-separated key path."""
    try:
        return config.get_string(key_path)
    except Exception:
        return None


def main():
    if len(sys.argv) < 2:
        config = parse_storage_config()
        print(config.get("storage", {}))
        return

    if sys.argv[1] == "--batch":
        config = parse_storage_config()
        for arg in sys.argv[2:]:
            if "=" not in arg:
                print(f"Error: batch argument must be VAR_NAME=key.path, got '{arg}'", file=sys.stderr)
                sys.exit(1)
            var_name, key_path = arg.split("=", 1)
            value = get_value(config, key_path)
            if value is None:
                value = ""
            print(f"{var_name}={value}")
    else:
        key_path = sys.argv[1]
        config = parse_storage_config()
        value = get_value(config, key_path)
        if value is None:
            print(f"Key '{key_path}' not found", file=sys.stderr)
            sys.exit(1)
        print(value)


if __name__ == "__main__":
    main()
