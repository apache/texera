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

/**
 * What the evaluation needs besides the agent service's own settings. Those (the Dashboard
 * Service, compiling service, computing unit and LLM endpoints) are read from the environment as
 * the agent service reads them, and passed on to the agent service the evaluation starts.
 */
const EvalEnvSchema = z.object({
  /** The model the agents run with, as `/api/models` lists it. */
  EVAL_MODEL: z.string().min(1),
  EVAL_USERNAME: z.string().default("texera"),
  EVAL_PASSWORD: z.string().default("texera"),
  /** Where datasets are created: the file service, or a gateway in front of it. */
  EVAL_FILE_SERVICE_ENDPOINT: z.string().url().default("http://localhost:9092"),
  EVAL_COMPUTING_UNIT_ID: z.coerce.number().int().positive().default(1),
  /** The warehouse executions write to; leave it unset where warehouses are off. */
  EVAL_WAREHOUSE_ID: z.coerce.number().int().positive().optional(),
  /** How many times each case runs: a real model does not answer the same way twice. */
  EVAL_REPEAT: z.coerce.number().int().positive().default(3),
  /** Comma-separated case ids to run; all of them when unset. */
  EVAL_CASES: z.string().optional(),
  /** Where to write the full report as JSON; not written when unset. */
  EVAL_OUT: z.string().optional(),
});

export interface EvalConfig {
  model: string;
  username: string;
  password: string;
  fileServiceEndpoint: string;
  computingUnitId: number;
  warehouseId?: number;
  repeat: number;
  caseIds?: string[];
  out?: string;
}

export function readEvalConfig(source: Record<string, string | undefined> = process.env): EvalConfig {
  const e = EvalEnvSchema.parse(source);
  return {
    model: e.EVAL_MODEL,
    username: e.EVAL_USERNAME,
    password: e.EVAL_PASSWORD,
    fileServiceEndpoint: e.EVAL_FILE_SERVICE_ENDPOINT.replace(/\/$/, ""),
    computingUnitId: e.EVAL_COMPUTING_UNIT_ID,
    warehouseId: e.EVAL_WAREHOUSE_ID,
    repeat: e.EVAL_REPEAT,
    caseIds: e.EVAL_CASES?.split(",")
      .map(id => id.trim())
      .filter(Boolean),
    out: e.EVAL_OUT,
  };
}
