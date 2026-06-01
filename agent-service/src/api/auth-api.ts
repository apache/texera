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

import { createHmac, timingSafeEqual } from "crypto";
import { existsSync, readFileSync } from "fs";
import { resolve } from "path";
import type { UserInfo } from "../types/agent";
import { createLogger } from "../logger";

export type { UserInfo } from "../types/agent";

const log = createLogger("Auth");

// Matches the backend JwtAuth (org.apache.texera.auth): HMAC-SHA256 over
// `${header}.${payload}` keyed by AUTH_JWT_SECRET (UTF-8), with a 30s clock skew.
const JWT_ALGORITHM = "HS256";
const CLOCK_SKEW_SECONDS = 30;
const AUTH_CONF_RELATIVE_PATH = "common/config/src/main/resources/auth.conf";

function readJwtSecretFromAuthConf(): string {
  const candidates = [
    resolve(import.meta.dir, "../../..", AUTH_CONF_RELATIVE_PATH),
    resolve(process.cwd(), "..", AUTH_CONF_RELATIVE_PATH),
    resolve(process.cwd(), AUTH_CONF_RELATIVE_PATH),
  ];

  for (const path of candidates) {
    if (!existsSync(path)) continue;

    const content = readFileSync(path, "utf-8");
    const matches = content.matchAll(/256-bit-secret\s*=\s*(?:"([^"]+)"|([^\s#]+))/g);
    for (const match of matches) {
      const value = (match[1] ?? match[2] ?? "").trim();
      if (!value || value.startsWith("${?")) continue;
      if (value.toLowerCase() === "random") {
        log.warn("auth.conf uses a random JWT secret; set AUTH_JWT_SECRET for agent-service");
        return "";
      }
      return value.toLowerCase();
    }
  }

  return "";
}

// Auth settings are read at call time (not cached) so tests can override the
// shared AUTH_JWT_SECRET without rebuilding the app.
function getJwtSecret(): string {
  const envSecret = process.env.AUTH_JWT_SECRET?.trim();
  return (envSecret || readJwtSecretFromAuthConf()).toLowerCase();
}

function base64UrlToBuffer(segment: string): Buffer {
  return Buffer.from(segment.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

function base64UrlEncode(buf: Buffer): string {
  return buf.toString("base64").replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

interface JwtParts {
  header: any;
  payload: any;
}

function decodeJwtParts(token: string): JwtParts {
  const parts = token.split(".");
  if (parts.length !== 3) {
    throw new Error("Invalid JWT format");
  }
  try {
    return {
      header: JSON.parse(base64UrlToBuffer(parts[0]).toString("utf-8")),
      payload: JSON.parse(base64UrlToBuffer(parts[1]).toString("utf-8")),
    };
  } catch (error) {
    throw new Error(`Failed to decode JWT: ${error}`);
  }
}

function decodeJWT(token: string): any {
  return decodeJwtParts(token).payload;
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

export function getUidFromToken(token: string): number | undefined {
  try {
    const uid = decodeJWT(token).userId;
    return typeof uid === "number" ? uid : undefined;
  } catch {
    return undefined;
  }
}

function isExpired(payload: any): boolean {
  if (typeof payload.exp !== "number") return true;
  return Math.floor(Date.now() / 1000) > payload.exp + CLOCK_SKEW_SECONDS;
}

/**
 * Cryptographically verify an HS256 JWT against AUTH_JWT_SECRET and check its
 * expiry and required claims. Returns false (never throws) on any failure.
 */
export function verifyToken(token: string): boolean {
  const secret = getJwtSecret();
  if (!secret) {
    log.warn("token verification requested but AUTH_JWT_SECRET is not set");
    return false;
  }

  const parts = token.split(".");
  if (parts.length !== 3) return false;

  let parsed: JwtParts;
  try {
    parsed = decodeJwtParts(token);
  } catch {
    return false;
  }

  if (parsed.header?.alg !== JWT_ALGORITHM) return false;
  // The backend requires both a subject and an expiration time.
  if (!parsed.payload?.sub || typeof parsed.payload?.exp !== "number") return false;
  if (isExpired(parsed.payload)) return false;

  const expected = base64UrlEncode(createHmac("sha256", secret).update(`${parts[0]}.${parts[1]}`).digest());
  const provided = parts[2];
  if (expected.length !== provided.length) return false;
  return timingSafeEqual(Buffer.from(expected), Buffer.from(provided));
}

/**
 * Accept a token for use. The agent service always requires the same verified
 * HS256 user JWT as Texera's JVM services.
 */
export function validateToken(token: string): boolean {
  return verifyToken(token);
}

export function createAuthHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}
