<<<<<<< HEAD
# Texera Deployment
=======
<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# `bin/`
>>>>>>> 214402f05 (fix: Add missing license headers to Docs (#6958))

This directory contains Dockerfiles and configuration files for building and deploying Texera's microservices.

## Dockerfiles

This directory includes several Dockerfiles, such as `file-service.dockerfile` and `computing-unit-master.dockerfile`. Each Dockerfile builds a specific Texera microservice. All Dockerfiles must be built from the `texera` project root as the Docker build context.

For example, to build the image using `texera-web-application.dockerfile`, run the following command **from the project root**:

```bash
docker build -f bin/texera-web-application.dockerfile -t your-repo/texera-web-application:test .
```

Two shell scripts, `build-images.sh` and `build-services.sh` are included for building platform-dependent images conveniently. 

You can also find prebuilt images published by the Texera team on the [Texera DockerHub Repository](https://hub.docker.com/repositories/texera).

## Deployment using images

Subdirectories `single-node` and `k8s` contain configuration files for deploying Texera using the above Docker images.