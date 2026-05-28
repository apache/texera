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

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHmac } from "crypto";
import {
  createAuthHeaders,
  extractUserFromToken,
  getUidFromToken,
  isAuthRequired,
  validateToken,
  verifyToken,
} from "./auth-api";

const SECRET = "unit-test-secret-key";

function b64url(input: string | Buffer): string {
  return Buffer.from(input).toString("base64").replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

function signJwt(payload: Record<string, unknown>, opts: { secret?: string; alg?: string } = {}): string {
  const header = b64url(JSON.stringify({ alg: opts.alg ?? "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const sig = b64url(
    createHmac("sha256", opts.secret ?? SECRET)
      .update(`${header}.${body}`)
      .digest()
  );
  return `${header}.${body}.${sig}`;
}

function futureExp(): number {
  return Math.floor(Date.now() / 1000) + 3600;
}

const prevSecret = process.env.AUTH_JWT_SECRET;
const prevRequired = process.env.AGENT_AUTH_REQUIRED;

beforeEach(() => {
  delete process.env.AUTH_JWT_SECRET;
  delete process.env.AGENT_AUTH_REQUIRED;
});

afterEach(() => {
  if (prevSecret === undefined) delete process.env.AUTH_JWT_SECRET;
  else process.env.AUTH_JWT_SECRET = prevSecret;
  if (prevRequired === undefined) delete process.env.AGENT_AUTH_REQUIRED;
  else process.env.AGENT_AUTH_REQUIRED = prevRequired;
});

describe("extractUserFromToken / getUidFromToken", () => {
  test("maps claims to UserInfo", () => {
    const token = signJwt({ sub: "alice", userId: 9, email: "a@b.c", role: "ADMIN", exp: futureExp() });
    expect(extractUserFromToken(token)).toEqual({ uid: 9, name: "alice", email: "a@b.c", role: "ADMIN" });
    expect(getUidFromToken(token)).toBe(9);
  });

  test("getUidFromToken returns undefined for a malformed token", () => {
    expect(getUidFromToken("not-a-jwt")).toBeUndefined();
  });
});

describe("isAuthRequired", () => {
  test("defaults to false", () => {
    expect(isAuthRequired()).toBe(false);
  });

  test("is true for 'true' or '1'", () => {
    process.env.AGENT_AUTH_REQUIRED = "true";
    expect(isAuthRequired()).toBe(true);
    process.env.AGENT_AUTH_REQUIRED = "1";
    expect(isAuthRequired()).toBe(true);
  });
});

describe("verifyToken", () => {
  beforeEach(() => {
    process.env.AUTH_JWT_SECRET = SECRET;
  });

  test("accepts a correctly signed, unexpired token", () => {
    expect(verifyToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }))).toBe(true);
  });

  test("rejects a token signed with the wrong secret", () => {
    expect(verifyToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }, { secret: "other" }))).toBe(false);
  });

  test("rejects an expired token", () => {
    const exp = Math.floor(Date.now() / 1000) - 3600;
    expect(verifyToken(signJwt({ sub: "u", userId: 1, exp }))).toBe(false);
  });

  test("rejects a token missing the subject claim", () => {
    expect(verifyToken(signJwt({ userId: 1, exp: futureExp() }))).toBe(false);
  });

  test("rejects a non-HS256 algorithm", () => {
    expect(verifyToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }, { alg: "none" }))).toBe(false);
  });

  test("rejects a structurally invalid token", () => {
    expect(verifyToken("a.b")).toBe(false);
  });

  test("returns false when no secret is configured", () => {
    delete process.env.AUTH_JWT_SECRET;
    expect(verifyToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }))).toBe(false);
  });
});

describe("validateToken", () => {
  test("enforced mode requires a valid signature", () => {
    process.env.AUTH_JWT_SECRET = SECRET;
    process.env.AGENT_AUTH_REQUIRED = "true";
    expect(validateToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }))).toBe(true);
    expect(validateToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }, { secret: "x" }))).toBe(false);
  });

  test("permissive mode accepts any unexpired, decodable token", () => {
    // No enforcement: a token signed with an arbitrary secret is still accepted
    // as long as it is well-formed and unexpired (prior behavior).
    expect(validateToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }, { secret: "anything" }))).toBe(true);
  });

  test("permissive mode rejects an expired token", () => {
    const exp = Math.floor(Date.now() / 1000) - 3600;
    expect(validateToken(signJwt({ sub: "u", userId: 1, exp }))).toBe(false);
  });

  test("permissive mode rejects a malformed token", () => {
    expect(validateToken("nonsense")).toBe(false);
  });
});

describe("createAuthHeaders", () => {
  test("builds a Bearer header with JSON content type", () => {
    expect(createAuthHeaders("t.o.k")).toEqual({
      Authorization: "Bearer t.o.k",
      "Content-Type": "application/json",
    });
  });
});
