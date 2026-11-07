/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { isEqual } from "lodash";
import type { WorkflowContent } from "../../core/common/type/workflow";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";
export interface VersionDifference {
  added: string[];
  modified: string[];
  deleted: string[];
}
export interface VersionHighlights extends VersionDifference {
  leftBrackets: string[];
  rightBrackets: string[];
  operatorPropertyDiff: Record<string, Map<string, string>>;
}
export const VERSION_PROPERTY_STYLE = "outline: 3px solid rgb(255, 118, 20); transition: 0.3s ease-in-out outline;";
function operators(content: WorkflowContent): Map<string, OperatorPredicate> {
  if (!content || !Array.isArray(content.operators)) throw new Error("Malformed workflow comparison.");
  const map = new Map<string, OperatorPredicate>();
  for (const operator of content.operators) {
    if (
      !operator ||
      typeof operator.operatorID !== "string" ||
      !operator.operatorID ||
      map.has(operator.operatorID) ||
      !operator.operatorProperties ||
      typeof operator.operatorProperties !== "object"
    )
      throw new Error("Malformed workflow comparison operators.");
    map.set(operator.operatorID, operator);
  }
  return map;
}
/** Preserves WorkflowVersionService's operator-only diff and property highlight semantics. */
export class VersionDiff {
  operatorPropertyDiff: Record<string, Map<string, string>> = {};
  getWorkflowsDifference(current: WorkflowContent, historical: WorkflowContent): VersionDifference {
    this.operatorPropertyDiff = {};
    const before = operators(current),
      after = operators(historical);
    const difference: VersionDifference = { added: [], modified: [], deleted: [] };
    for (const [id, operator] of after) {
      const previous = before.get(id);
      if (!previous) difference.added.push(id);
      else if (!isEqual(previous, operator)) {
        difference.modified.push(id);
        this.operatorPropertyDiff[id] = this.getOperatorsDifference(previous, operator);
      }
    }
    for (const id of before.keys()) if (!after.has(id)) difference.deleted.push(id);
    return difference;
  }
  getOperatorsDifference(current: OperatorPredicate, historical: OperatorPredicate): Map<string, string> {
    const difference = new Map<string, string>();
    // Deliberately retain the original loose comparison and arg1 property scope.
    for (const property of Object.keys(current.operatorProperties))
      if (current.operatorProperties[property] != historical.operatorProperties[property])
        difference.set(property, VERSION_PROPERTY_STYLE);
    if (current.operatorVersion != historical.operatorVersion)
      difference.set("operatorVersion", VERSION_PROPERTY_STYLE);
    return difference;
  }
}
export function versionHighlights(
  difference: VersionDifference,
  current: WorkflowContent,
  propertyDiff: Record<string, Map<string, string>> = {}
): VersionHighlights {
  if (!Array.isArray(current.links)) throw new Error("Malformed workflow comparison links.");
  const deleted = new Set(difference.deleted),
    leftBrackets = new Set<string>(),
    rightBrackets = new Set<string>();
  for (const link of current.links) {
    if (
      !link ||
      !link.source ||
      !link.target ||
      typeof link.source.operatorID !== "string" ||
      typeof link.target.operatorID !== "string"
    )
      throw new Error("Malformed workflow comparison links.");
    if (deleted.has(link.source.operatorID) && link.target.operatorID !== undefined)
      leftBrackets.add(link.target.operatorID);
    if (deleted.has(link.target.operatorID) && link.source.operatorID !== undefined)
      rightBrackets.add(link.source.operatorID);
  }
  return {
    ...difference,
    leftBrackets: [...leftBrackets],
    rightBrackets: [...rightBrackets],
    operatorPropertyDiff: propertyDiff,
  };
}
export interface VersionPropertyChange {
  property: string;
  before: unknown;
  after: unknown;
}
export function versionPropertyChanges(
  before: OperatorPredicate | undefined,
  after: OperatorPredicate | undefined
): VersionPropertyChange[] {
  const first = before?.operatorProperties ?? {},
    second = after?.operatorProperties ?? {};
  const changes = [...new Set([...Object.keys(first), ...Object.keys(second)])]
    .filter(key => !isEqual(first[key], second[key]))
    .map(property => ({ property, before: first[property], after: second[property] }));
  if (before?.operatorVersion !== after?.operatorVersion)
    changes.push({ property: "operatorVersion", before: before?.operatorVersion, after: after?.operatorVersion });
  return changes;
}
