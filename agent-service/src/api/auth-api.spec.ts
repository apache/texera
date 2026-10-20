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
import { JWT_SECRET } from "../config/jwt";
import { createAuthHeaders, extractBearerToken, extractUserFromToken, validateToken } from "./auth-api";

function b64url(input: string | Buffer): string {
  return Buffer.from(input).toString("base64url");
}

/** `exp` is a UNIX second count, so expiries are built in seconds throughout. */
function nowInSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

/**
 * Builds a properly signed HS256 JWT using JWT_SECRET with default valid fields.
 */
function tokenWith(payload: Record<string, unknown>, secret = JWT_SECRET): string {
  const fullPayload = {
    sub: "test-user",
    userId: 1,
    exp: nowInSeconds() + 3600,
    ...payload,
  };
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(fullPayload));
  const signature = b64url(createHmac("sha256", new TextEncoder().encode(secret)).update(`${header}.${body}`).digest());
  return `${header}.${body}.${signature}`;
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
    const token = tokenWith({ userId: 7, sub: "grace", email: undefined, role: undefined });
    const user = extractUserFromToken(token);

    expect(user.email).toBe("");
    expect(user.role).toBe("REGULAR");
  });

  test("rejects a token missing its signature segment", () => {
    const payload = Buffer.from(JSON.stringify({ userId: 1, sub: "ada" }), "utf-8").toString("base64url");
    expect(() => extractUserFromToken(`header.${payload}`)).toThrow(/Invalid or expired token/);
  });

  test("rejects a token with too many segments", () => {
    const payload = Buffer.from(JSON.stringify({ userId: 1, sub: "ada" }), "utf-8").toString("base64url");
    expect(() => extractUserFromToken(`header.${payload}.signature.extra`)).toThrow(/Invalid or expired token/);
  });

  test("rejects a token whose payload is not JSON", () => {
    const notJson = Buffer.from("<html>", "utf-8").toString("base64url");
    expect(() => extractUserFromToken(`header.${notJson}.signature`)).toThrow(/Invalid or expired token/);
  });

  test("rejects a mis-signed token", () => {
    const badToken = tokenWith({ userId: 1, sub: "ada" }, "wrong-secret");
    expect(() => extractUserFromToken(badToken)).toThrow(/Invalid or expired token/);
  });
});

describe("validateToken", () => {
  test("accepts a token whose expiry is still in the future", () => {
    expect(validateToken(tokenWith({ userId: 1, exp: nowInSeconds() + 60 }))).toBe(true);
  });

  test("rejects a token whose expiry has passed", () => {
    expect(validateToken(tokenWith({ userId: 1, exp: nowInSeconds() - 60 }))).toBe(false);
  });

  test("rejects a token without exp (required claim)", () => {
    const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
    const body = b64url(JSON.stringify({ sub: "test-user", userId: 1 }));
    const signature = b64url(createHmac("sha256", new TextEncoder().encode(JWT_SECRET)).update(`${header}.${body}`).digest());
    expect(validateToken(`${header}.${body}.${signature}`)).toBe(false);
  });

  test("rejects a malformed token rather than letting error escape", () => {
    expect(validateToken("not-a-token")).toBe(false);
  });

  test("expiry is compared in seconds, not milliseconds", () => {
    expect(validateToken(tokenWith({ exp: nowInSeconds() + 3600 }))).toBe(true);
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
