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
  DEFAULT_RECOMMENDATION_LIMIT,
  MAX_RECOMMENDATION_LIMIT,
  type OperatorRecommendation,
  type RecommendationRequest,
  type RecommendationResponse,
} from "./recommender-types";
import { candidateSuccessors, rationaleFor } from "./hardcoded-rules";

/**
 * Minimal read-only view of the operator catalog the recommender needs. The
 * `WorkflowSystemMetadata` singleton satisfies this structurally; tests can pass
 * a stub (or omit it entirely to exercise the un-initialized path).
 */
export interface OperatorCatalog {
  isInitialized(): boolean;
  operatorTypeExists(operatorType: string): boolean;
  getAdditionalMetadata(operatorType: string): { userFriendlyName?: string } | undefined;
}

// Top suggestion scores 0.9 and each lower rank drops one step, floored at 0.3.
// With MAX_RECOMMENDATION_LIMIT = 5 this yields 0.9 / 0.75 / 0.6 / 0.45 / 0.3.
const TOP_SCORE = 0.9;
const SCORE_STEP = 0.15;
const MIN_SCORE = 0.3;

function scoreForRank(rank: number): number {
  return Math.max(MIN_SCORE, +(TOP_SCORE - rank * SCORE_STEP).toFixed(2));
}

function clampLimit(limit: number | undefined): number {
  if (limit === undefined || !Number.isFinite(limit)) return DEFAULT_RECOMMENDATION_LIMIT;
  return Math.min(MAX_RECOMMENDATION_LIMIT, Math.max(1, Math.floor(limit)));
}

/**
 * Version 1 hardcoded recommender: given the operator just added, return a
 * short ranked list of likely successors drawn from {@link candidateSuccessors}.
 *
 * The result is deterministic and requires no LLM call. When a `catalog` is
 * supplied and initialized, suggestions are filtered to operator types that
 * actually exist (so a stale rule never yields an unusable suggestion) and enriched
 * with the operator's display name; without an initialized catalog the raw
 * ranked candidates are returned as-is.
 */
export function recommendOperators(req: RecommendationRequest, catalog?: OperatorCatalog): RecommendationResponse {
  const operatorType = req.operatorType?.trim();
  const limit = clampLimit(req.limit);

  if (!operatorType) {
    return { recommendations: [], strategy: "hardcoded" };
  }

  const catalogReady = catalog?.isInitialized() ?? false;

  const seen = new Set<string>();
  const recommendations: OperatorRecommendation[] = [];

  for (const candidate of candidateSuccessors(operatorType)) {
    if (recommendations.length >= limit) break;
    // Never suggest the operator itself, and never suggest the same type twice.
    if (candidate === operatorType || seen.has(candidate)) continue;
    // Drop suggestions the running system can't actually instantiate.
    if (catalogReady && !catalog!.operatorTypeExists(candidate)) continue;
    seen.add(candidate);

    const userFriendlyName = catalogReady ? catalog!.getAdditionalMetadata(candidate)?.userFriendlyName : undefined;

    recommendations.push({
      operatorType: candidate,
      score: scoreForRank(recommendations.length),
      reason: rationaleFor(candidate),
      ...(userFriendlyName ? { userFriendlyName } : {}),
    });
  }

  return { recommendations, strategy: "hardcoded" };
}
