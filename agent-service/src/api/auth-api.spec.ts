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

import { describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import { createAuthHeaders, extractBearerToken, extractUserFromToken, validateToken } from "./auth-api";
import { env, EnvSchema } from "../config/env";

function segment(value: Record<string, unknown>): string {
  return Buffer.from(JSON.stringify(value), "utf-8").toString("base64url");
}

/**
 * Builds a token carrying the given payload, signed the way the dashboard service signs one:
 * HS256 over the first two segments, with the shared secret unless a test names another.
 */
function tokenWith(payload: Record<string, unknown>, secret: string = env.AUTH_JWT_SECRET): string {
  const signed = `${segment({ alg: "HS256" })}.${segment(payload)}`;
  return `${signed}.${createHmac("sha256", secret).update(signed).digest("base64url")}`;
}

/** `exp` is a UNIX second count, so expiries are built in seconds throughout. */
function nowInSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

describe("extractUserFromToken", () => {
  test("maps the JWT payload onto the user record", () => {
    const token = tokenWith({ userId: 42, sub: "ada", email: "ada@example.com", role: "ADMIN" });

    expect(extractUserFromToken(token)).toEqual({
      uid: 42,
      name: "ada",
      email: "ada@example.com",
      role: "ADMIN",
    });
  });

  test("defaults a missing email to empty and a missing role to REGULAR", () => {
    // Role defaulting is a privilege decision: absent must mean least privilege, never admin.
    const user = extractUserFromToken(tokenWith({ userId: 7, sub: "grace" }));

    expect(user.email).toBe("");
    expect(user.role).toBe("REGULAR");
  });

  test("rejects a token missing its signature segment even when the payload decodes", () => {
    // The payload here is perfectly good JSON, so only the segment-count check stands between an
    // unsigned token and a populated user record.
    const payload = Buffer.from(JSON.stringify({ userId: 1, sub: "ada" }), "utf-8").toString("base64");

    expect(() => extractUserFromToken(`header.${payload}`)).toThrow(/Failed to decode JWT/);
  });

  test("rejects a token with too many segments", () => {
    const payload = Buffer.from(JSON.stringify({ userId: 1, sub: "ada" }), "utf-8").toString("base64");

    expect(() => extractUserFromToken(`header.${payload}.signature.extra`)).toThrow(/Failed to decode JWT/);
  });

  test("rejects a token whose payload is not JSON", () => {
    const notJson = Buffer.from("<html>", "utf-8").toString("base64");
    expect(() => extractUserFromToken(`header.${notJson}.signature`)).toThrow(/Failed to decode JWT/);
  });
});

describe("validateToken", () => {
  test("accepts a token whose expiry is still in the future", () => {
    expect(validateToken(tokenWith({ userId: 1, exp: nowInSeconds() + 60 }))).toBe(true);
  });

  test("rejects a token whose expiry has passed", () => {
    expect(validateToken(tokenWith({ userId: 1, exp: nowInSeconds() - 60 }))).toBe(false);
  });

  test("treats a token with no expiry as valid forever", () => {
    // Deliberate: tokens minted without `exp` never expire. Pinned because it is a policy choice
    // that reads like an oversight, and flipping it would lock out every non-expiring token.
    expect(validateToken(tokenWith({ userId: 1 }))).toBe(true);
  });

  test("rejects a malformed token rather than letting the decode failure escape", () => {
    // isTokenExpired swallows the decode error and reports "expired", so callers get false here
    // instead of an exception. A caller that only catches would otherwise let a junk token through.
    expect(validateToken("not-a-token")).toBe(false);
  });

  test("expiry is compared in seconds, not milliseconds", () => {
    // exp is a UNIX second count; reading it as milliseconds would place every real token in 1970
    // and reject it. A far-future second count must still validate.
    expect(validateToken(tokenWith({ exp: nowInSeconds() + 3600 }))).toBe(true);
  });

  test("rejects a token signed with another secret, whatever it claims", () => {
    // Anyone can write these claims; only the dashboard service's secret can sign them. The
    // agent service gives a token's user their agents, so an unsigned claim must count for nothing.
    const forged = tokenWith({ userId: 1, sub: "texera", role: "ADMIN", exp: nowInSeconds() + 60 }, "not-the-secret");

    expect(validateToken(forged)).toBe(false);
  });

  test("rejects a token whose claims were edited after it was signed", () => {
    const [header, , signature] = tokenWith({ userId: 2, sub: "bob", exp: nowInSeconds() + 60 }).split(".");
    const edited = segment({ userId: 1, sub: "bob", exp: nowInSeconds() + 60 });

    expect(validateToken(`${header}.${edited}.${signature}`)).toBe(false);
  });

  test("rejects an unsigned token that declares no algorithm", () => {
    // The check never reads the header's `alg`, so a token cannot ask for a weaker one.
    const unsigned = `${segment({ alg: "none" })}.${segment({ userId: 1, sub: "texera", exp: nowInSeconds() + 60 })}.`;

    expect(validateToken(unsigned)).toBe(false);
  });

  test("defaults to the secret auth.conf gives the dashboard service", () => {
    // env.ts repeats auth.conf's default. If the two drift, a stack that sets neither (local
    // development, single-node) would reject every login token.
    const conf = readFileSync(new URL("../../../common/config/src/main/resources/auth.conf", import.meta.url), "utf-8");

    expect(EnvSchema.parse({}).AUTH_JWT_SECRET).toBe(/256-bit-secret = "([^"]+)"/.exec(conf)![1]);
  });
});

describe("extractBearerToken", () => {
  test("returns the token from a Bearer header", () => {
    expect(extractBearerToken("Bearer abc123")).toBe("abc123");
  });

  test("accepts the scheme in any case", () => {
    expect(extractBearerToken("bearer abc123")).toBe("abc123");
    expect(extractBearerToken("BEARER abc123")).toBe("abc123");
  });

  test("ignores a non-Bearer scheme", () => {
    expect(extractBearerToken("Basic abc123")).toBeUndefined();
  });

  test("returns undefined for a missing header", () => {
    expect(extractBearerToken(undefined)).toBeUndefined();
  });

  test("returns undefined when the scheme carries no token", () => {
    expect(extractBearerToken("Bearer")).toBeUndefined();
    expect(extractBearerToken("Bearer ")).toBeUndefined();
  });
});

describe("createAuthHeaders", () => {
  test("sends the token as a Bearer credential alongside a JSON content type", () => {
    expect(createAuthHeaders("abc123")).toEqual({
      Authorization: "Bearer abc123",
      "Content-Type": "application/json",
    });
  });
});
