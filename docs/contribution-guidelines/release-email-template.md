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

---
title: "[VOTE] Release Apache Texera (incubating) Email Template"
weight: 80
---

The canonical vote email template lives in the repository at
[`.github/release/vote-email-template.txt`](https://github.com/apache/texera/blob/main/.github/release/vote-email-template.txt).

The release manager does not fill it in by hand. The
`Create and upload release candidate artifacts` workflow fills in the version,
RC number, staging directory, git tag, commit hash, signing key, and the list
of container images pulled by the bundled Docker Compose file, and prints the
ready-to-send email in its run summary. The run log lists any placeholders
(such as the "Major Changes" highlights and the sender name) that remain for
the release manager to fill in before sending.

Keeping the template in one place avoids the two copies drifting apart; edit
the file above rather than this page.
