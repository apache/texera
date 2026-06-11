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

import { createHmac, timingSafeEqual } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { env } from "./env";
import { createLogger } from "../logger";

const log = createLogger("Jwt");

// Token issuance lives in the Scala services (org.apache.texera.auth.JwtAuth):
// HS256 over the UTF-8 bytes of the secret, with a required `exp` and `sub`
// and a 30s allowed clock skew. This module mirrors the verification so the
// agent service can validate the same tokens without a gateway.
const CLOCK_SKEW_SECONDS = 30;

// auth.conf default, used as the last-resort fallback when neither the env
// override nor the file is available (matches AuthConfig's literal default).
const AUTH_CONF_DEFAULT_SECRET = "8a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d";

/** Read the `256-bit-secret` default out of auth.conf, if the file is found. */
function readSecretFromAuthConf(): string | undefined {
  const candidates = [
    env.AUTH_CONF_PATH, // explicit override
    "auth.conf", // bundled next to the app in the container image
    "../common/config/src/main/resources/auth.conf", // repo layout in local dev
  ].filter((p): p is string => typeof p === "string" && p.length > 0);

  for (const candidate of candidates) {
    const path = resolve(process.cwd(), candidate);
    if (!existsSync(path)) continue;
    // The first quoted `256-bit-secret = "..."` is the literal default; the
    // second line is the `${?AUTH_JWT_SECRET}` env override (handled below).
    const match = readFileSync(path, "utf-8").match(/256-bit-secret\s*=\s*"([^"]*)"/);
    if (match) {
      log.info({ path }, "loaded JWT secret default from auth.conf");
      return match[1];
    }
  }
  log.warn("auth.conf not found; falling back to the built-in default JWT secret");
  return undefined;
}

// Resolution order mirrors HOCON's `256-bit-secret = "<default>"; 256-bit-secret
// = ${?AUTH_JWT_SECRET}`, and the `.toLowerCase()` normalization AuthConfig
// applies. ("random" is intentionally unsupported here — it cannot match across
// processes, so deployments share a fixed secret.)
export const JWT_SECRET: string = (
  env.AUTH_JWT_SECRET ||
  readSecretFromAuthConf() ||
  AUTH_CONF_DEFAULT_SECRET
).toLowerCase();

const SECRET_BYTES = new TextEncoder().encode(JWT_SECRET);

export interface JwtUser {
  uid: number;
  name: string;
  email: string;
  role: string;
}

function decodeSegment(segment: string): unknown {
  return JSON.parse(Buffer.from(segment, "base64url").toString("utf-8"));
}

/**
 * Verify an HS256 JWT issued by the Scala services and return its claims, or
 * null if the token is missing, malformed, mis-signed, expired, or uses a
 * different algorithm. Mirrors org.apache.texera.auth.JwtParser.parseToken.
 */
export function verifyToken(token: string | undefined | null): JwtUser | null {
  if (!token) return null;
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [headerB64, payloadB64, signatureB64] = parts;

    const header = decodeSegment(headerB64) as { alg?: string };
    // Reject anything that is not HS256 (defends against "none" / alg-confusion).
    if (header.alg !== "HS256") return null;

    const expected = createHmac("sha256", SECRET_BYTES).update(`${headerB64}.${payloadB64}`).digest();
    const actual = Buffer.from(signatureB64, "base64url");
    if (expected.length !== actual.length || !timingSafeEqual(expected, actual)) {
      return null;
    }

    const payload = decodeSegment(payloadB64) as {
      exp?: number;
      sub?: string;
      userId?: number | string;
      email?: string;
      role?: string;
    };

    // `exp` and `sub` are required by JwtAuth.jwtConsumer.
    if (typeof payload.exp !== "number") return null;
    if (typeof payload.sub !== "string" || payload.sub.length === 0) return null;
    if (payload.userId === undefined || payload.userId === null) return null;

    const nowSeconds = Math.floor(Date.now() / 1000);
    if (nowSeconds > payload.exp + CLOCK_SKEW_SECONDS) return null;

    return {
      uid: Number(payload.userId),
      name: payload.sub,
      email: typeof payload.email === "string" ? payload.email : "",
      role: typeof payload.role === "string" ? payload.role : "REGULAR",
    };
  } catch {
    return null;
  }
}
