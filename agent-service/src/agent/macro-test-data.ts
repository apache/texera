/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import fixture from "../../../common/workflow-compiler/src/test/resources/macros/nested-expansion.json";

export function macroWorkflow(leafProperty = "value") {
  const content = structuredClone(fixture.content);
  macroLeaf(content).formBinding.fields[0].propertyKey = leafProperty;
  return content;
}

export function macroLeaf(content: typeof fixture.content) {
  return content.macroDependencies[0].content.macroDependencies[0].content.macroDependencies[0].content;
}

export function macroExpansionResponse() {
  return structuredClone(fixture.expected);
}
