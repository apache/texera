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
 * Wire types for the operator recommender (discussion apache/texera#5240).
 *
 * The recommender is a stateless endpoint that, given the operator a user just
 * added to the canvas, returns a short ranked list of likely "next" operators
 * to render as suggestions on one of its output ports.
 *
 * Version 1 (this file's consumer, `recommendOperators`) produces suggestions
 * from a hardcoded rule table (no LLM call, no persistent state), so it can
 * ship and validate the full canvas pipeline at zero API cost. Version 2 will
 * swap the ranking strategy for a small LLM behind the same request/response
 * shape; the `strategy` discriminator on the response lets clients tell the two
 * apart without a breaking change.
 */

/** Ranking strategy that produced a response. `llm` is reserved for V2. */
export type OperatorRecommendationStrategy = "hardcoded" | "llm";

/** Hard ceiling on how many suggestions a single request may ask for. */
export const MAX_OPERATOR_RECOMMENDATION_LIMIT = 5;

/** Default number of suggestions when the request does not specify a limit. */
export const DEFAULT_OPERATOR_RECOMMENDATION_LIMIT = 3;

/** Longest operator type string the route accepts. */
export const MAX_OPERATOR_TYPE_LENGTH = 128;

/** Output port suggestions attach to when the request does not name one. */
export const DEFAULT_SOURCE_PORT_ID = "output-0";

/** Longest source port ID the route accepts. */
export const MAX_PORT_ID_LENGTH = 16;

/**
 * Canonical output port ID: `output-<index>` with no leading zeros, so each
 * port has exactly one spelling (`output-00` is rejected, not read as port 0).
 */
export const OUTPUT_PORT_ID_PATTERN = "^output-(0|[1-9][0-9]*)$";

export interface OperatorRecommendationRequest {
  /**
   * The operator type that was just added and whose output we are suggesting
   * successors for (e.g. `"CSVFileScan"`). Primary ranking signal.
   */
  operatorType: string;

  /**
   * Optional: the output port of `operatorType` the suggestions will be wired
   * from, using the canvas port ID format (`"output-<index>"`, see
   * {@link OUTPUT_PORT_ID_PATTERN}). Defaults to {@link DEFAULT_SOURCE_PORT_ID}.
   * A port the operator does not have yields no suggestions.
   */
  sourcePortID?: string;

  /**
   * Optional: maximum number of suggestions to return. Defaults to
   * {@link DEFAULT_OPERATOR_RECOMMENDATION_LIMIT}, clamped to
   * `[1, {@link MAX_OPERATOR_RECOMMENDATION_LIMIT}]`.
   */
  limit?: number;
}

export interface OperatorRecommendation {
  /** Recommended operator type, present in the live operator catalog. */
  operatorType: string;
  /**
   * Input port of the recommended operator that the source port should be
   * wired to, using the canvas port ID format (`"input-<index>"`). Matters for
   * multi-input operators such as Sklearn Prediction, whose first port takes a
   * trained model and whose second takes data rows.
   */
  targetPortID: string;
  /** Confidence in `[0, 1]`, monotonically non-increasing down the list. */
  score: number;
  /** Short, human-readable rationale shown alongside the suggested operator. */
  reason: string;
  /** Display name from operator metadata, when it has one. */
  userFriendlyName?: string;
}

export interface OperatorRecommendationResponse {
  recommendations: OperatorRecommendation[];
  /** Which ranking strategy produced these results. */
  strategy: OperatorRecommendationStrategy;
}
