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
import { JWT_SECRET, verifyToken } from "./jwt";

function b64url(input: string | Buffer): string {
  return Buffer.from(input).toString("base64url");
}

// Mints an HS256 token the same way org.apache.texera.auth.JwtAuth does.
function sign(payload: Record<string, unknown>, opts: { secret?: string; alg?: string } = {}): string {
  const alg = opts.alg ?? "HS256";
  const secret = opts.secret ?? JWT_SECRET;
  const header = b64url(JSON.stringify({ alg, typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const signature = b64url(createHmac("sha256", new TextEncoder().encode(secret)).update(`${header}.${body}`).digest());
  return `${header}.${body}.${signature}`;
}

const now = () => Math.floor(Date.now() / 1000);
const validClaims = () => ({
  sub: "alice",
  userId: 42,
  email: "alice@example.com",
  role: "REGULAR",
  exp: now() + 3600,
});

describe("verifyToken", () => {
  test("accepts a correctly-signed, unexpired token and returns its claims", () => {
    const user = verifyToken(sign(validClaims()));
    expect(user).not.toBeNull();
    expect(user).toEqual({ uid: 42, name: "alice", email: "alice@example.com", role: "REGULAR" });
  });

  test("coerces a string userId claim to a number", () => {
    const user = verifyToken(sign({ ...validClaims(), userId: "7" }));
    expect(user?.uid).toBe(7);
  });

  test("rejects a token signed with a different secret", () => {
    expect(verifyToken(sign(validClaims(), { secret: "not-the-real-secret" }))).toBeNull();
  });

  test("rejects a tampered payload (signature no longer matches)", () => {
    const token = sign(validClaims());
    const [h, , s] = token.split(".");
    const forged = b64url(JSON.stringify({ ...validClaims(), userId: 999 }));
    expect(verifyToken(`${h}.${forged}.${s}`)).toBeNull();
  });

  test("rejects an expired token (beyond the 30s clock skew)", () => {
    expect(verifyToken(sign({ ...validClaims(), exp: now() - 60 }))).toBeNull();
  });

  test("accepts a just-expired token within the 30s clock skew", () => {
    expect(verifyToken(sign({ ...validClaims(), exp: now() - 10 }))).not.toBeNull();
  });

  test("rejects a non-HS256 algorithm (none / alg-confusion)", () => {
    expect(verifyToken(sign(validClaims(), { alg: "none" }))).toBeNull();
  });

  test("rejects tokens missing required exp / sub / userId claims", () => {
    expect(verifyToken(sign({ sub: "alice", userId: 42 }))).toBeNull(); // no exp
    expect(verifyToken(sign({ userId: 42, exp: now() + 60 }))).toBeNull(); // no sub
    expect(verifyToken(sign({ sub: "alice", exp: now() + 60 }))).toBeNull(); // no userId
  });

  test("rejects malformed / empty tokens", () => {
    expect(verifyToken(undefined)).toBeNull();
    expect(verifyToken("")).toBeNull();
    expect(verifyToken("not-a-jwt")).toBeNull();
    expect(verifyToken("only.two")).toBeNull();
  });
});
