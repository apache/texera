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

import {
  DEFAULT_OPERATOR_RECOMMENDATION_LIMIT,
  DEFAULT_SOURCE_PORT_ID,
  MAX_OPERATOR_RECOMMENDATION_LIMIT,
  OUTPUT_PORT_ID_PATTERN,
  type OperatorRecommendation,
  type OperatorRecommendationRequest,
  type OperatorRecommendationResponse,
} from "./operator-recommendation-types";
import {
  candidateSuccessors,
  categoryOf,
  rationaleFor,
  targetPortIdFor,
  type OperatorCatalogEntry,
} from "./operator-recommendation-rules";

/**
 * Minimal read-only view of the operator catalog the recommender needs. The
 * `WorkflowSystemMetadata` singleton satisfies this structurally; tests can pass
 * a stub.
 */
export interface OperatorCatalog {
  isInitialized(): boolean;
  getSchema(operatorType: string): unknown;
  getAdditionalMetadata(operatorType: string): OperatorCatalogEntry | undefined;
}

// Top suggestion scores 0.9 and each lower rank drops one step, floored at 0.3.
// With MAX_OPERATOR_RECOMMENDATION_LIMIT = 5 this yields 0.9 / 0.75 / 0.6 / 0.45 / 0.3.
const TOP_SCORE = 0.9;
const SCORE_STEP = 0.15;
const MIN_SCORE = 0.3;

const OUTPUT_PORT_ID = new RegExp(OUTPUT_PORT_ID_PATTERN);

function scoreForRank(rank: number): number {
  return Math.max(MIN_SCORE, +(TOP_SCORE - rank * SCORE_STEP).toFixed(2));
}

function clampLimit(limit: number | undefined): number {
  if (limit === undefined || !Number.isFinite(limit)) return DEFAULT_OPERATOR_RECOMMENDATION_LIMIT;
  return Math.min(MAX_OPERATOR_RECOMMENDATION_LIMIT, Math.max(1, Math.floor(limit)));
}

/** Whether `portId` names an output port the operator actually has. */
function hasOutputPort(entry: OperatorCatalogEntry, portId: string): boolean {
  const match = OUTPUT_PORT_ID.exec(portId);
  if (!match) return false;
  // With user-added ports, any well-formed port may exist.
  if (entry.dynamicOutputPorts) return true;
  return Number(match[1]) < (entry.outputPorts?.length ?? 0);
}

/** Whether a suggested operator has an input port the source can be wired to. */
function acceptsInput(entry: OperatorCatalogEntry | undefined): boolean {
  return !!entry && ((entry.inputPorts?.length ?? 0) > 0 || !!entry.dynamicInputPorts);
}

/**
 * Version 1 hardcoded recommender: given the operator just added, return a
 * short ranked list of likely successors drawn from {@link candidateSuccessors}.
 *
 * The result is deterministic and requires no LLM call. The operator is
 * classified from its catalog metadata, suggestions are filtered to operator
 * types that exist and have an input port to wire to (so a stale rule never
 * yields an unusable suggestion), and each suggestion is enriched with its
 * display name and the input port to wire to. Since every one of those checks
 * needs the catalog, nothing is suggested while the catalog is not loaded, or
 * for an operator or output port it does not know.
 */
export function recommendOperators(
  req: OperatorRecommendationRequest,
  catalog: OperatorCatalog | undefined
): OperatorRecommendationResponse {
  const operatorType = req.operatorType?.trim();
  const limit = clampLimit(req.limit);
  const empty: OperatorRecommendationResponse = { recommendations: [], strategy: "hardcoded" };

  if (!operatorType || !catalog?.isInitialized()) return empty;

  const source = catalog.getAdditionalMetadata(operatorType);
  if (!source || !hasOutputPort(source, req.sourcePortID ?? DEFAULT_SOURCE_PORT_ID)) return empty;
  const sourceCategory = categoryOf(operatorType, source);

  const seen = new Set<string>();
  const recommendations: OperatorRecommendation[] = [];

  for (const candidate of candidateSuccessors(operatorType, sourceCategory)) {
    if (recommendations.length >= limit) break;
    // Never suggest the operator itself, and never suggest the same type twice.
    if (candidate === operatorType || seen.has(candidate)) continue;
    // Drop suggestions the running system can't actually instantiate (an
    // operator exists in the catalog exactly when its schema was loaded) or
    // can't wire the source into.
    if (catalog.getSchema(candidate) === undefined) continue;
    const target = catalog.getAdditionalMetadata(candidate);
    if (!acceptsInput(target)) continue;
    seen.add(candidate);

    const userFriendlyName = target?.userFriendlyName;

    recommendations.push({
      operatorType: candidate,
      targetPortID: targetPortIdFor(sourceCategory, target),
      score: scoreForRank(recommendations.length),
      reason: rationaleFor(candidate),
      ...(userFriendlyName ? { userFriendlyName } : {}),
    });
  }

  return { recommendations, strategy: "hardcoded" };
}
