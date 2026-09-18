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

# The texera-mounter runs as a per-node privileged DaemonSet. It performs the GeeseFS
# FUSE mount that computing-unit pods used to do themselves, so that CU pods (which run
# untrusted user code) can be unprivileged. It needs python3 (the mounter), fuse3 + geesefs
# (to mount), and util-linux (umount) — all part of a minimal Debian base.
FROM debian:bookworm-slim

ARG GEESEFS_VERSION=v0.43.8
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 fuse3 mount ca-certificates curl \
    && curl -fsSL -o /usr/local/bin/geesefs \
       "https://github.com/yandex-cloud/geesefs/releases/download/${GEESEFS_VERSION}/geesefs-linux-$(dpkg --print-architecture)" \
    && chmod 755 /usr/local/bin/geesefs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # allow FUSE mounts to be accessible by other users (the unprivileged CU pod's UID)
    && echo "user_allow_other" >> /etc/fuse.conf

COPY bin/mounter/mounter.py /opt/mounter/mounter.py

EXPOSE 8100
ENTRYPOINT ["python3", "/opt/mounter/mounter.py"]
