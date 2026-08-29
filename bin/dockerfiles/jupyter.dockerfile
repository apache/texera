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

# Apache Texera is an effort undergoing incubation at The Apache Software
# Foundation (ASF), sponsored by the Apache Incubator PMC. Incubation is
# required of all newly accepted projects until a further review indicates
# that the infrastructure, communications, and decision-making process have
# stabilized in a manner consistent with other successful ASF projects.
# While incubation status is not necessarily a reflection of the
# completeness or stability of the code, it does indicate that the project
# has yet to be fully endorsed by the ASF.

FROM jupyter/base-notebook:notebook-6.5.4

# The customizations live with notebook-migration-service, which owns the Jupyter
# integration. Paths are repo-root relative: the build context is the repo root.
COPY notebook-migration-service/src/main/resources/custom.js /home/jovyan/.jupyter/custom/custom.js
COPY notebook-migration-service/src/main/resources/custom.css /home/jovyan/.jupyter/custom/custom.css
COPY notebook-migration-service/src/main/resources/start-texera-jupyter.sh /usr/local/bin/start-texera-jupyter.sh

# custom.js must stay writable by jovyan: the startup script substitutes the origin
# placeholder into it at runtime.
USER root
RUN chown -R jovyan:users /home/jovyan/.jupyter && \
    chmod +x /usr/local/bin/start-texera-jupyter.sh

USER jovyan

CMD ["start-texera-jupyter.sh"]

EXPOSE 8888
