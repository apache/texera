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

# `sql/updates/`

One file per schema migration. A file is live only when
[`sql/changelog.xml`](../changelog.xml) references it as a Liquibase
changeSet — the directory itself is never globbed for work to do, so an
unreferenced file here is inert.

## The retired scripts, `01.sql`–`22.sql`

They are not in the working tree; they are in git history.

These 22 predate Liquibase. Operators applied them by hand, prompted by the
DDL-change notification e-mail. Liquibase arrived with
[#4401](https://github.com/apache/texera/pull/4401), which started the chain
fresh at `23.sql` and enrolled none of the 22 — so no runner in the repository
has been able to execute them since. They are also why the live files are
unpadded: the retired set is `01.sql`, the current set `23.sql`.

Only an operator whose database was created before changeset 23 would need
them. Any database past that point applied them long ago, and a fresh one is
bootstrapped from [`sql/texera_ddl.sql`](../texera_ddl.sql), which already has
the post-migration shape.

To read one, or to reassemble the whole set, from any commit that precedes the
removal — `363537e` is one:

```bash
git show 363537e:sql/updates/07.sql
for n in $(seq -w 1 22); do git show "363537e:sql/updates/$n.sql"; done > 01-22.sql
```

To locate the removal itself:

```bash
git log --diff-filter=D -- sql/updates/01.sql
```
