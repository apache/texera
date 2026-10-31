<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# texera-support

The helpers a Python script exported from a Texera workflow calls, so the
script gives the answers the Texera engine gives.

An exported script imports the helpers it calls from this package. The export
downloads as a zip that holds this module as `texera_support.py` next to the
script, so the unzipped folder runs with nothing installed. Installing the
package, with `pip install ./texera-support`, makes it importable from anywhere.

`texera_support/__init__.py` is generated from the helper definitions in
`common/workflow-operator`. Do not edit it by hand. After changing a helper, run

```
TEXERA_SUPPORT_WRITE=1 sbt "WorkflowOperator/testOnly org.apache.texera.amber.operator.StandaloneHelpersSpec"
```

Without the variable, the same spec fails when the file is stale.
