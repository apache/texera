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

/**
 * Shared fixtures for UserDatasetVersionFiletreeComponent specs.
 *
 * The filetree has two spec files that drive the same component:
 *   - user-dataset-version-filetree.component.spec.ts          (jsdom test target)
 *   - user-dataset-version-filetree.component.browser.spec.ts  (test-browser target,
 *                                                               for the virtual scroll
 *                                                               paths that need real
 *                                                               geometry)
 *
 * Both specs build the same flat file trees. Exporting the factory here
 * keeps them from drifting over time.
 */

import { DatasetFileNode } from "../../../../../../common/type/datasetVersionFileTree";

export const FILE_COUNT = 1000;

export function makeFlatFileNodes(count: number): DatasetFileNode[] {
  return Array.from({ length: count }, (_, i) => ({
    name: `file_${String(i + 1).padStart(4, "0")}.txt`,
    type: "file",
    parentDir: "/owner/dataset/v1",
    size: 1,
  }));
}
