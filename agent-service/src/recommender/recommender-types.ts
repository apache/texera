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
 * Wire types for the ambient operator recommender (discussion apache/texera#5240).
 *
 * The recommender is a stateless endpoint that, given the operator a user just
 * added to the canvas, returns a short ranked list of likely "next" operators
 * to render as suggestion chips on the output port.
 *
 * Version 1 (this file's consumer, `recommendOperators`) produces suggestions
 * from a hardcoded rule table — no LLM call, no persistent state — so it can
 * ship and validate the full canvas pipeline at zero API cost. Version 2 will
 * swap the ranking strategy for a small LLM behind the same request/response
 * shape; the `strategy` discriminator on the response lets clients tell the two
 * apart without a breaking change.
 */

/** Ranking strategy that produced a response. `llm` is reserved for V2. */
export type RecommendationStrategy = "hardcoded" | "llm";

/** Hard ceiling on how many suggestions a single request may ask for. */
export const MAX_RECOMMENDATION_LIMIT = 5;

/** Default number of suggestions when the request does not specify a limit. */
export const DEFAULT_RECOMMENDATION_LIMIT = 3;

export interface RecommendationRequest {
  /**
   * The operator type that was just added and whose output port we are
   * suggesting successors for (e.g. `"CSVFileScan"`). Primary ranking signal.
   */
  operatorType: string;

  /**
   * Optional: operator types already present on the canvas. Unused by the V1
   * hardcoded ranker; reserved so V2 can bias suggestions with graph context
   * without a request-shape change.
   */
  existingOperatorTypes?: string[];

  /**
   * Optional: maximum number of suggestions to return. Defaults to
   * {@link DEFAULT_RECOMMENDATION_LIMIT}, clamped to
   * `[1, {@link MAX_RECOMMENDATION_LIMIT}]`.
   */
  limit?: number;
}

export interface OperatorRecommendation {
  /** Recommended operator type; always a real, catalog-known type. */
  operatorType: string;
  /** Confidence in `[0, 1]`, monotonically non-increasing down the list. */
  score: number;
  /** Short, human-readable rationale shown alongside the suggested operator. */
  reason: string;
  /** Display name from operator metadata, when the catalog is available. */
  userFriendlyName?: string;
}

export interface RecommendationResponse {
  recommendations: OperatorRecommendation[];
  /** Which ranking strategy produced these results. */
  strategy: RecommendationStrategy;
}
