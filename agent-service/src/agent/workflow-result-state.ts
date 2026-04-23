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

import type { OperatorInfo } from "../types/execution";

interface ResultEntry {
  operatorInfo: OperatorInfo;
  stepId: string;
}

/**
 * Versioned per-operator execution results keyed by step id.
 *
 * Each operator can have multiple result snapshots (one per step that
 * executed it). Lookups walk the current ancestor path from HEAD and return
 * the most recent result visible on that branch, so checking out an earlier
 * step exposes the results that were live at that point.
 */
export class WorkflowResultState {
  private results = new Map<string, Map<string, ResultEntry>>();

  constructor(private getAncestorPath: () => string[]) {}

  set(operatorId: string, stepId: string, operatorInfo: OperatorInfo): void {
    let versions = this.results.get(operatorId);
    if (!versions) {
      versions = new Map();
      this.results.set(operatorId, versions);
    }
    versions.set(stepId, { operatorInfo, stepId });
  }

  get(operatorId: string): ResultEntry | undefined {
    const versions = this.results.get(operatorId);
    if (!versions) return undefined;

    const path = this.getAncestorPath();
    for (let i = path.length - 1; i >= 0; i--) {
      const entry = versions.get(path[i]);
      if (entry) return entry;
    }
    return undefined;
  }

  getOperatorInfo(operatorId: string): OperatorInfo | undefined {
    return this.get(operatorId)?.operatorInfo;
  }

  getAllVisible(): Map<string, ResultEntry> {
    const result = new Map<string, ResultEntry>();
    const path = this.getAncestorPath();

    for (const [operatorId, versions] of this.results) {
      for (let i = path.length - 1; i >= 0; i--) {
        if (versions.has(path[i])) {
          result.set(operatorId, versions.get(path[i])!);
          break;
        }
      }
    }
    return result;
  }

  clear(): void {
    this.results.clear();
  }
}
