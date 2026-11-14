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

import { z } from "zod";
import { LAST_RUN_STATES, type LastRunReport } from "../types/execution";

/** Longest error message accepted from the client, per entry. */
export const LAST_RUN_MESSAGE_MAX_CHARS = 4000;
/** Most operator entries and most workflow-level errors accepted from the client. */
export const LAST_RUN_MAX_ENTRIES = 10;

/** Most per-operator stats entries accepted from the client. */
export const LAST_RUN_MAX_OPERATOR_STATS = 50;
/** Longest operator state name accepted from the client. */
export const LAST_RUN_STATE_MAX_CHARS = 40;
/** Longest operator id accepted from the client. */
export const LAST_RUN_OPERATOR_ID_MAX_CHARS = 200;

const message = z.string().max(LAST_RUN_MESSAGE_MAX_CHARS);
const rowCount = z.number().int().nonnegative();
const operatorId = z.string().min(1).max(LAST_RUN_OPERATOR_ID_MAX_CHARS);

const operatorStats = z.object({
  operatorId,
  state: z.string().max(LAST_RUN_STATE_MAX_CHARS),
  inputRows: rowCount,
  outputRows: rowCount,
  reusedFromCache: z.boolean().optional(),
});

export const LastRunReportSchema = z.object({
  state: z.enum(LAST_RUN_STATES),
  operatorErrors: z.array(z.object({ operatorId, message })).max(LAST_RUN_MAX_ENTRIES),
  workflowErrors: z.array(message).max(LAST_RUN_MAX_ENTRIES),
  operatorStats: z.array(operatorStats).max(LAST_RUN_MAX_OPERATOR_STATS).optional(),
});

/** Returns the report when it matches the schema, otherwise undefined. */
export function parseLastRun(value: unknown): LastRunReport | undefined {
  if (value === undefined || value === null) return undefined;
  const result = LastRunReportSchema.safeParse(value);
  return result.success ? result.data : undefined;
}
