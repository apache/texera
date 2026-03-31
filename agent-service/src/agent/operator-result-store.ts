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
 * Versioned operator result store.
 *
 * Stores raw OperatorInfo per (operatorId, actionId) pair. Results are
 * looked up using the HEAD ancestor path so that only results from the
 * current branch are visible. Append-only — no invalidation needed.
 */

import type { OperatorInfo } from "../types/execution";
import type { AgentActionManager } from "./agent-action-manager";

/**
 * Unified operator result entry — stores the raw structured data
 * instead of pre-serialized strings.
 */
export interface OperatorResultEntry {
  /** The raw OperatorInfo from the backend execution result. */
  operatorInfo: OperatorInfo;
  /** The action ID under which this result was produced. */
  actionId: string;
}

/**
 * Versioned, HEAD-aware operator result store.
 *
 * Data model: Map<operatorId, Map<actionId, OperatorResultEntry>>
 *
 * Lookup: Given an operatorId and the HEAD ancestor path, return the
 * result whose actionId is the latest on that path.
 */
export class OperatorResultStore {
  /** operatorId → (actionId → entry) */
  private store = new Map<string, Map<string, OperatorResultEntry>>();

  constructor(private agentActionManager: AgentActionManager) {}

  /**
   * Store a result for an operator at a specific action version.
   */
  set(operatorId: string, actionId: string, operatorInfo: OperatorInfo): void {
    let versions = this.store.get(operatorId);
    if (!versions) {
      versions = new Map();
      this.store.set(operatorId, versions);
    }
    versions.set(actionId, { operatorInfo, actionId });
  }

  /**
   * Get the result for an operator visible from the current HEAD.
   *
   * Walks the ancestor path from root to HEAD and returns the result
   * whose actionId is the latest (closest to HEAD) on that path.
   * Returns undefined if no result exists on the current branch.
   */
  get(operatorId: string): OperatorResultEntry | undefined {
    const versions = this.store.get(operatorId);
    if (!versions) return undefined;

    // Walk ancestor path in reverse (HEAD → root) to find the latest match
    const path = this.agentActionManager.getAncestorPath();
    for (let i = path.length - 1; i >= 0; i--) {
      const entry = versions.get(path[i]);
      if (entry) return entry;
    }
    return undefined;
  }

  /**
   * Get the OperatorInfo for an operator visible from the current HEAD.
   * Convenience method — returns just the OperatorInfo or undefined.
   */
  getOperatorInfo(operatorId: string): OperatorInfo | undefined {
    return this.get(operatorId)?.operatorInfo;
  }

  /**
   * Get all operator results visible from the current HEAD.
   * Returns a Map<operatorId, OperatorResultEntry> with one entry per operator.
   */
  getAllVisible(): Map<string, OperatorResultEntry> {
    const result = new Map<string, OperatorResultEntry>();
    const pathSet = new Set(this.agentActionManager.getAncestorPath());

    for (const [operatorId, versions] of this.store) {
      // Walk ancestor path in reverse to find the latest match
      const path = this.agentActionManager.getAncestorPath();
      for (let i = path.length - 1; i >= 0; i--) {
        if (versions.has(path[i])) {
          result.set(operatorId, versions.get(path[i])!);
          break;
        }
      }
    }
    return result;
  }

  /**
   * Clear all stored results.
   */
  clear(): void {
    this.store.clear();
  }
}
