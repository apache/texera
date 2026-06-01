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
import { createAuthHeaders, extractUserFromToken, getUidFromToken, validateToken, verifyToken } from "./auth-api";

const SECRET = "unit-test-secret-key";
const AUTH_CONF_SECRET = "8a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d";

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

beforeEach(() => {
  delete process.env.AUTH_JWT_SECRET;
});

afterEach(() => {
  if (prevSecret === undefined) delete process.env.AUTH_JWT_SECRET;
  else process.env.AUTH_JWT_SECRET = prevSecret;
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

  test("falls back to auth.conf when AUTH_JWT_SECRET is unset or empty", () => {
    delete process.env.AUTH_JWT_SECRET;
    expect(verifyToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }, { secret: AUTH_CONF_SECRET }))).toBe(true);

    process.env.AUTH_JWT_SECRET = "";
    expect(verifyToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }, { secret: AUTH_CONF_SECRET }))).toBe(true);
  });
});

describe("validateToken", () => {
  beforeEach(() => {
    process.env.AUTH_JWT_SECRET = SECRET;
  });

  test("requires a valid signature", () => {
    expect(validateToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }))).toBe(true);
    expect(validateToken(signJwt({ sub: "u", userId: 1, exp: futureExp() }, { secret: "x" }))).toBe(false);
  });

  test("rejects an expired token", () => {
    const exp = Math.floor(Date.now() / 1000) - 3600;
    expect(validateToken(signJwt({ sub: "u", userId: 1, exp }))).toBe(false);
  });

  test("rejects a malformed token", () => {
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
