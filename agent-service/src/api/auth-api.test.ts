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
import { createAuthHeaders, extractUserFromToken, validateToken } from "./auth-api";

// Builds a syntactically valid (unsigned) JWT for the given payload. The
// service only decodes the payload - it does not verify the signature - so a
// dummy signature segment is sufficient for these tests.
function makeJwt(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  return `${header}.${body}.signature`;
}

describe("extractUserFromToken", () => {
  test("maps JWT claims to UserInfo", () => {
    const token = makeJwt({ userId: 42, sub: "alice", email: "alice@example.com", role: "ADMIN" });
    const user = extractUserFromToken(token);
    expect(user).toEqual({ uid: 42, name: "alice", email: "alice@example.com", role: "ADMIN" });
  });

  test("defaults email to empty string and role to REGULAR when absent", () => {
    const token = makeJwt({ userId: 7, sub: "bob" });
    const user = extractUserFromToken(token);
    expect(user.email).toBe("");
    expect(user.role).toBe("REGULAR");
  });

  test("throws on a token without three segments", () => {
    expect(() => extractUserFromToken("not-a-jwt")).toThrow(/Failed to decode JWT/);
  });
});

describe("validateToken", () => {
  test("accepts a token with no exp claim", () => {
    expect(validateToken(makeJwt({ userId: 1, sub: "x" }))).toBe(true);
  });

  test("accepts a token whose exp is in the future", () => {
    const future = Math.floor(Date.now() / 1000) + 3600;
    expect(validateToken(makeJwt({ userId: 1, sub: "x", exp: future }))).toBe(true);
  });

  test("rejects a token whose exp is in the past", () => {
    const past = Math.floor(Date.now() / 1000) - 3600;
    expect(validateToken(makeJwt({ userId: 1, sub: "x", exp: past }))).toBe(false);
  });

  test("rejects a malformed token", () => {
    expect(validateToken("obviously-not-a-jwt")).toBe(false);
  });
});

describe("createAuthHeaders", () => {
  test("produces a Bearer Authorization header and JSON content type", () => {
    expect(createAuthHeaders("abc.def.ghi")).toEqual({
      Authorization: "Bearer abc.def.ghi",
      "Content-Type": "application/json",
    });
  });
});
