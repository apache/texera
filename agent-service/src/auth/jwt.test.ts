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
import { extractUserFromToken, validateToken, createAuthHeaders } from "./jwt";

function makeToken(payload: Record<string, unknown>): string {
  const encode = (o: Record<string, unknown>) => Buffer.from(JSON.stringify(o)).toString("base64");
  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.signature`;
}

const nowSeconds = () => Math.floor(Date.now() / 1000);

describe("extractUserFromToken", () => {
  test("maps JWT claims onto a UserInfo", () => {
    const token = makeToken({ userId: 7, sub: "alice", email: "alice@example.com", role: "ADMIN" });
    expect(extractUserFromToken(token)).toEqual({ uid: 7, name: "alice", email: "alice@example.com", role: "ADMIN" });
  });

  test("defaults missing email and role", () => {
    const token = makeToken({ userId: 1, sub: "bob" });
    expect(extractUserFromToken(token)).toEqual({ uid: 1, name: "bob", email: "", role: "REGULAR" });
  });

  test("throws on a malformed token", () => {
    expect(() => extractUserFromToken("not-a-jwt")).toThrow("Failed to decode JWT");
  });
});

describe("validateToken", () => {
  test("accepts a token expiring in the future", () => {
    expect(validateToken(makeToken({ sub: "a", exp: nowSeconds() + 3600 }))).toBe(true);
  });

  test("rejects an expired token", () => {
    expect(validateToken(makeToken({ sub: "a", exp: nowSeconds() - 3600 }))).toBe(false);
  });

  test("treats a token without exp as valid", () => {
    expect(validateToken(makeToken({ sub: "a" }))).toBe(true);
  });

  test("rejects a malformed token", () => {
    expect(validateToken("garbage")).toBe(false);
  });
});

describe("createAuthHeaders", () => {
  test("builds bearer auth headers", () => {
    expect(createAuthHeaders("tok")).toEqual({ Authorization: "Bearer tok", "Content-Type": "application/json" });
  });
});
