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
 * A boolean flag read from an environment variable. `z.coerce.boolean()` is
 * `Boolean(value)`, which reads any non-empty string, "false" and "0"
 * included, as true. This accepts true/1/yes/on and false/0/no/off in any
 * case, treats an empty value as unset, and rejects anything else.
 */
const envFlag = z
  .preprocess(
    value => {
      if (typeof value !== "string") return value;
      const normalized = value.trim().toLowerCase();
      return normalized === "" ? undefined : normalized;
    },
    z.enum(["true", "false", "1", "0", "yes", "no", "on", "off"]).optional()
  )
  .transform(value => value === "true" || value === "1" || value === "yes" || value === "on");

export const EnvSchema = z.object({
  PORT: z.coerce.number().default(3001),
  API_PREFIX: z.string().default("/api"),
  TEXERA_SERVICE_LOG_LEVEL: z
    .enum(["ERROR", "WARN", "INFO", "DEBUG"])
    .transform(v => v.toLowerCase() as "error" | "warn" | "info" | "debug")
    .default("INFO"),
  LOG_PRETTY: envFlag,

  TEXERA_DASHBOARD_SERVICE_ENDPOINT: z.string().url().default("http://localhost:8080"),
  LLM_ENDPOINT: z.string().url().default("http://localhost:9096"),
  WORKFLOW_COMPILING_SERVICE_ENDPOINT: z.string().url().default("http://localhost:9090"),
  WORKFLOW_EXECUTION_SERVICE_ENDPOINT: z.string().url().default("http://localhost:8085"),
  EXECUTION_ENDPOINT_TEMPLATE: z.string().optional(),
});

export const env = EnvSchema.parse(process.env);
