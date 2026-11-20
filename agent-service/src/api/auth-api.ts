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
import { env } from "../config/env";
import type { UserInfo } from "../types/agent";

export type { UserInfo } from "../types/agent";

function decodeJWT(token: string): any {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) {
      throw new Error("Invalid JWT format");
    }
    return JSON.parse(Buffer.from(parts[1], "base64").toString("utf-8"));
  } catch (error) {
    throw new Error(`Failed to decode JWT: ${error}`);
  }
}

export function extractUserFromToken(token: string): UserInfo {
  const payload = decodeJWT(token);
  return {
    uid: payload.userId,
    name: payload.sub,
    email: payload.email || "",
    role: payload.role || "REGULAR",
  };
}

function isTokenExpired(token: string): boolean {
  try {
    const payload = decodeJWT(token);
    if (!payload.exp) return false;
    return Date.now() >= payload.exp * 1000;
  } catch {
    return true;
  }
}

// Whether the dashboard service issued this token: its signature is the HMAC-SHA256
// of the first two segments under the shared secret, as JwtAuth.scala writes it. The
// header's own `alg` is never consulted, so a token cannot choose a weaker check.
function isSignedByTexera(token: string): boolean {
  const parts = token.split(".");
  if (parts.length !== 3) {
    return false;
  }
  const expected = createHmac("sha256", env.AUTH_JWT_SECRET).update(`${parts[0]}.${parts[1]}`).digest();
  const actual = Buffer.from(parts[2], "base64url");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

// A token's claims can be trusted only when this holds: anyone can write the claims,
// only the dashboard service can sign them.
export function validateToken(token: string): boolean {
  return isSignedByTexera(token) && !isTokenExpired(token);
}

export function extractBearerToken(header: string | undefined): string | undefined {
  if (!header) return undefined;
  const [scheme, token] = header.split(" ");
  return scheme?.toLowerCase() === "bearer" && token ? token : undefined;
}

export function createAuthHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}
