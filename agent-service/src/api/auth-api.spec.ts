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
import { createAuthHeaders, extractBearerToken, extractUserFromToken, validateToken } from "./auth-api";

/** Builds a token with the given payload. Only the payload segment is ever read back. */
function tokenWith(payload: Record<string, unknown>): string {
  const encoded = Buffer.from(JSON.stringify(payload), "utf-8").toString("base64");
  return `header.${encoded}.signature`;
}

const SECONDS = 1000;

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
    const exp = Math.floor((Date.now() + 60 * SECONDS) / 1000);
    expect(validateToken(tokenWith({ userId: 1, exp }))).toBe(true);
  });

  test("rejects a token whose expiry has passed", () => {
    const exp = Math.floor((Date.now() - 60 * SECONDS) / 1000);
    expect(validateToken(tokenWith({ userId: 1, exp }))).toBe(false);
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
    const exp = Math.floor(Date.now() / 1000) + 3600;
    expect(validateToken(tokenWith({ exp }))).toBe(true);
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
