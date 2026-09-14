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

Subject: [VOTE] Release Apache Texera (incubating) ${VERSION} RC${RC_NUM}

Hi Texera Community,

This is a call for a vote on releasing Apache Texera (incubating)
${VERSION} RC${RC_NUM}.

== Release Candidate Artifacts ==

https://dist.apache.org/repos/dist/dev/incubator/texera/${RC_DIR}/

The staging directory contains:
- Source tarball (.tar.gz), with its GPG signature (.asc) and SHA-512 checksum (.sha512)
- Docker Compose convenience bundle, with its GPG signature and SHA-512 checksum

== Container Images ==

The following convenience container images are available:

${IMAGE_REGISTRY}/texera-dashboard-service:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-workflow-execution-coordinator:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-workflow-execution-runner:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-workflow-compiling-service:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-file-service:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-config-service:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-access-control-service:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-agent-service:${IMAGE_TAG}
${IMAGE_REGISTRY}/texera-workflow-computing-unit-managing-service:${IMAGE_TAG}

These images were built from the source tarball included in this release candidate.

== Git Tag and Commit ==

https://github.com/apache/texera/releases/tag/${TAG_NAME}

Commit: ${COMMIT_HASH}

== KEYS ==

https://downloads.apache.org/incubator/texera/KEYS

The release was signed with GPG key [${GPG_KEY_ID}] (${GPG_EMAIL}).

== Major Changes Since <PREVIOUS_RELEASE, e.g. Apache Texera 1.1.0-incubating> ==

Full diff:
https://github.com/apache/texera/compare/<PREVIOUS_TAG>...${TAG_NAME}

Major changes include:
<FILL IN: grouped highlights, verified against the previous release BRANCH,
not its tag>

== Vote ==

The vote will remain open for at least 72 hours.

[ ] +1 Approve the release
[ ]  0 No opinion
[ ] -1 Do not approve the release because...

== Checklist ==

Please include the checklist below in your reply and mark the checks you
performed with an x.

[ ] Checksums and PGP signatures are valid
[ ] LICENSE and NOTICE files are correct
[ ] All files have ASF license headers where appropriate
[ ] Source tarball matches the Git tag
[ ] Docker Compose deploys successfully

Thank you for reviewing and voting on this release candidate.

Best,
<YOUR NAME>
Release Manager for Apache Texera ${VERSION}
