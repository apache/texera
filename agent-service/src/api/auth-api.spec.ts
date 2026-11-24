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
import { createAuthHeaders, extractBearerToken, verifyToken } from "./auth-api";
import { encodeSegment, signTestToken } from "./test-token";
import { env, EnvSchema } from "../config/env";

/** `exp` is a UNIX second count, so expiries are built in seconds throughout. */
function nowInSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

describe("verifyToken", () => {
  test("maps a signed token's claims onto the user record", () => {
    const token = signTestToken({ userId: 42, sub: "ada", email: "ada@example.com", role: "ADMIN" });

    expect(verifyToken(token)).toEqual({ uid: 42, name: "ada", email: "ada@example.com", role: "ADMIN" });
  });

  test("defaults a missing email to empty and a missing role to REGULAR", () => {
    // Role defaulting is a privilege decision: absent must mean least privilege, never admin.
    const user = verifyToken(signTestToken({ userId: 7, sub: "grace" }));

    expect(user?.email).toBe("");
    expect(user?.role).toBe("REGULAR");
  });

  test("accepts a token whose expiry is still in the future", () => {
    expect(verifyToken(signTestToken({ userId: 1, exp: nowInSeconds() + 60 }))).toBeDefined();
  });

  test("rejects a token whose expiry has passed", () => {
    expect(verifyToken(signTestToken({ userId: 1, exp: nowInSeconds() - 60 }))).toBeUndefined();
  });

  test("treats a token with no expiry as valid forever", () => {
    // Deliberate: tokens minted without `exp` never expire. Pinned because it is a policy choice
    // that reads like an oversight, and flipping it would lock out every non-expiring token.
    expect(verifyToken(signTestToken({ userId: 1 }))).toBeDefined();
  });

  test("expiry is compared in seconds, not milliseconds", () => {
    // exp is a UNIX second count; reading it as milliseconds would place every real token in 1970
    // and reject it. A far-future second count must still verify.
    expect(verifyToken(signTestToken({ userId: 1, exp: nowInSeconds() + 3600 }))).toBeDefined();
  });

  test("rejects a missing or malformed token rather than throwing", () => {
    expect(verifyToken(undefined)).toBeUndefined();
    expect(verifyToken("not-a-token")).toBeUndefined();
    // A good payload without its signature segment, and a signed token with one segment too many.
    expect(verifyToken(`${encodeSegment({ alg: "HS256" })}.${encodeSegment({ userId: 1 })}`)).toBeUndefined();
    expect(verifyToken(`${signTestToken({ userId: 1 })}.extra`)).toBeUndefined();
  });

  test("rejects a signed token whose claims are not JSON", () => {
    // The signature holds, so only the decode stands between this token and a user record.
    const signed = `${encodeSegment({ alg: "HS256" })}.${Buffer.from("<html>", "utf-8").toString("base64url")}`;
    const token = `${signed}.${createHmac("sha256", env.AUTH_JWT_SECRET).update(signed).digest("base64url")}`;

    expect(verifyToken(token)).toBeUndefined();
  });

  test("rejects a signed token without a whole-number user id", () => {
    // Agents are matched to their owner by this id, so a token must name exactly one user.
    expect(verifyToken(signTestToken({ sub: "ada" }))).toBeUndefined();
    expect(verifyToken(signTestToken({ userId: "7", sub: "ada" }))).toBeUndefined();
    expect(verifyToken(signTestToken({ userId: 1.5, sub: "ada" }))).toBeUndefined();
  });

  test("rejects a token signed with another secret, whatever it claims", () => {
    // Anyone can write these claims; only the dashboard service's secret can sign them. The
    // agent service gives a token's user their agents, so an unsigned claim must count for nothing.
    const claims = { userId: 1, sub: "texera", role: "ADMIN", exp: nowInSeconds() + 60 };

    expect(verifyToken(signTestToken(claims, "not-the-secret"))).toBeUndefined();
  });

  test("rejects a token whose claims were edited after it was signed", () => {
    const [header, , signature] = signTestToken({ userId: 2, sub: "bob", exp: nowInSeconds() + 60 }).split(".");
    const edited = encodeSegment({ userId: 1, sub: "bob", exp: nowInSeconds() + 60 });

    expect(verifyToken(`${header}.${edited}.${signature}`)).toBeUndefined();
  });

  test("rejects an unsigned token that declares no algorithm", () => {
    // The check never reads the header's `alg`, so a token cannot ask for a weaker one.
    const claims = encodeSegment({ userId: 1, sub: "texera", exp: nowInSeconds() + 60 });

    expect(verifyToken(`${encodeSegment({ alg: "none" })}.${claims}.`)).toBeUndefined();
  });
});

describe("AUTH_JWT_SECRET", () => {
  test("defaults to the secret auth.conf gives the dashboard service", () => {
    // env.ts repeats auth.conf's default. If the two drift, a stack that sets neither (local
    // development, single-node) would reject every login token.
    const conf = readFileSync(new URL("../../../common/config/src/main/resources/auth.conf", import.meta.url), "utf-8");

    expect(EnvSchema.parse({}).AUTH_JWT_SECRET).toBe(/256-bit-secret = "([^"]+)"/.exec(conf)![1]);
  });

  test("reads a configured secret the way the dashboard service does", () => {
    // AuthConfig.scala lowercases AUTH_JWT_SECRET before signing, so a deployment that sets
    // "Xk9F-Shared" signs every login token with "xk9f-shared". Read as written, the secret
    // would reject them all.
    expect(EnvSchema.parse({ AUTH_JWT_SECRET: "Xk9F-Shared" }).AUTH_JWT_SECRET).toBe("xk9f-shared");
  });

  test('refuses "random", which makes each backend process invent its own key', () => {
    // No other service can know such a key, so starting with it would only reject every token.
    expect(() => EnvSchema.parse({ AUTH_JWT_SECRET: "Random" })).toThrow("cannot be 'random'");
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
