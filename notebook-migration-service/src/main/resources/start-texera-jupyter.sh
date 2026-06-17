#!/bin/bash
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

set -euo pipefail

# Texera app origin used by custom.js (postMessage targetOrigin + inbound origin
# check) and by the iframe CSP frame-ancestors. Override TEXERA_ORIGIN for
# deployments under a real hostname; defaults to the local dev origin.
TEXERA_ORIGIN="${TEXERA_ORIGIN:-http://localhost:4200}"

# Weak default token so the server is not fully open to anyone reachable on the
# published port. The Texera-side iframe URL must pass this through ?token=<value>.
JUPYTER_TOKEN="${JUPYTER_TOKEN:-texera}"

# Substitute the origin placeholder in custom.js before the server starts serving it.
sed -i "s|__TEXERA_ORIGIN__|${TEXERA_ORIGIN}|g" /home/jovyan/.jupyter/custom/custom.js

exec start-notebook.sh \
  --NotebookApp.token="${JUPYTER_TOKEN}" \
  --NotebookApp.password='' \
  --NotebookApp.disable_check_xsrf=True \
  --NotebookApp.tornado_settings="{'headers': {'Content-Security-Policy': 'frame-ancestors ${TEXERA_ORIGIN}'}}" \
  --NotebookApp.default_url=/tree
