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

const EnvSchema = z.object({
  PORT: z.coerce.number().default(3001),
  API_PREFIX: z.string().default("/api"),
  // Master key for the LiteLLM gateway. The agent service is a trusted backend,
  // so it holds this secret and calls LiteLLM directly (no access-control proxy).
  LITELLM_MASTER_KEY: z.string().default("dummy"),
  TEXERA_SERVICE_LOG_LEVEL: z
    .enum(["ERROR", "WARN", "INFO", "DEBUG"])
    .transform(v => v.toLowerCase() as "error" | "warn" | "info" | "debug")
    .default("INFO"),
  LOG_PRETTY: z.coerce.boolean().default(false),

  TEXERA_DASHBOARD_SERVICE_ENDPOINT: z.string().url().default("http://localhost:8080"),
  LITELLM_BASE_URL: z.string().url().default("http://localhost:4000"),
  WORKFLOW_COMPILING_SERVICE_ENDPOINT: z.string().url().default("http://localhost:9090"),
  WORKFLOW_EXECUTION_SERVICE_ENDPOINT: z.string().url().default("http://localhost:8085"),
  EXECUTION_ENDPOINT_TEMPLATE: z.string().optional(),

  // Shared JWT secret (HS256). Overrides the default in auth.conf, mirroring
  // common/config AuthConfig. When unset, the secret is read from auth.conf.
  AUTH_JWT_SECRET: z.string().optional(),
  // Path to auth.conf; defaults are probed in config/jwt.ts (bundled in the
  // image, or the repo path in local dev).
  AUTH_CONF_PATH: z.string().optional(),
});

export const env = EnvSchema.parse(process.env);
