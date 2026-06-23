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

FROM jupyter/base-notebook:notebook-6.5.4

# Copy custom JavaScript for Jupyter and the startup script
COPY custom.js /home/jovyan/.jupyter/custom/custom.js
COPY start-texera-jupyter.sh /usr/local/bin/start-texera-jupyter.sh

# Ensure correct permissions. custom.js must stay writable by jovyan so the
# startup script can substitute the origin placeholder at runtime.
USER root
RUN mkdir -p /home/jovyan/.jupyter/custom && \
    chown -R jovyan:users /home/jovyan/.jupyter && \
    chmod +x /usr/local/bin/start-texera-jupyter.sh

USER jovyan
