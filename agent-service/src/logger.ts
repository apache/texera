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

import pino, { type Logger } from "pino";
import { env } from "./config/env";

const rootLogger: Logger = pino({
  level: env.TEXERA_SERVICE_LOG_LEVEL,
  base: undefined,
  ...(env.LOG_PRETTY
    ? {
        transport: {
          target: "pino-pretty",
          options: {
            colorize: true,
            translateTime: "HH:MM:ss.l",
            ignore: "pid,hostname",
          },
        },
      }
    : {}),
});

// Prefer child loggers over manual `[Module agentId]` prefixes: `module` and
// `agent` become structured fields in JSON output and render as a prefix in
// pretty mode.
export function createLogger(module: string, bindings: Record<string, unknown> = {}): Logger {
  return rootLogger.child({ module, ...bindings });
}

export const logger = rootLogger;
