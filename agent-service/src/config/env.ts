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

export const EnvSchema = z.object({
  PORT: z.coerce.number().default(3001),
  API_PREFIX: z.string().default("/api"),
  TEXERA_SERVICE_LOG_LEVEL: z
    .enum(["ERROR", "WARN", "INFO", "DEBUG"])
    .transform(v => v.toLowerCase() as "error" | "warn" | "info" | "debug")
    .default("INFO"),
  LOG_PRETTY: z.coerce.boolean().default(false),

  TEXERA_DASHBOARD_SERVICE_ENDPOINT: z.string().url().default("http://localhost:8080"),
  LLM_ENDPOINT: z.string().url().default("http://localhost:9096"),
  WORKFLOW_COMPILING_SERVICE_ENDPOINT: z.string().url().default("http://localhost:9090"),
  WORKFLOW_EXECUTION_SERVICE_ENDPOINT: z.string().url().default("http://localhost:8085"),
  EXECUTION_ENDPOINT_TEMPLATE: z.string().optional(),

  // The secret the dashboard service signs login tokens with (HS256). The same
  // variable overrides it there, and the default here is the one in
  // common/config/src/main/resources/auth.conf, so the two agree unless a
  // deployment sets it for one service and not the other. It is read the way
  // AuthConfig.scala reads it: lowercased, since the dashboard service signs with
  // the lowercased value, and never "random", which there makes each process
  // invent its own key that no other service can know.
  AUTH_JWT_SECRET: z
    .string()
    .min(1)
    .default("8a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d")
    .transform(secret => secret.toLowerCase())
    .refine(secret => secret !== "random", {
      message: "AUTH_JWT_SECRET cannot be 'random': set it to the secret the dashboard service signs login tokens with",
    }),
});

export const env = EnvSchema.parse(process.env);
