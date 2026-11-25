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

// Login tokens for tests, signed the way the dashboard service signs one (JwtAuth.scala):
// HS256 over the first two segments, with the shared secret unless a test names another.

import { createHmac } from "node:crypto";
import { env } from "../config/env";

/** One base64url segment of a token, holding `value` as JSON. */
export function encodeSegment(value: Record<string, unknown>): string {
  return Buffer.from(JSON.stringify(value), "utf-8").toString("base64url");
}

/** A token carrying `claims`, signed with `secret`. */
export function signTestToken(claims: Record<string, unknown>, secret: string = env.AUTH_JWT_SECRET): string {
  const signed = `${encodeSegment({ alg: "HS256", typ: "JWT" })}.${encodeSegment(claims)}`;
  return `${signed}.${createHmac("sha256", secret).update(signed).digest("base64url")}`;
}

/** A REGULAR user's login token, an hour from expiry. */
export function mintTestToken(userId = 1, secret: string = env.AUTH_JWT_SECRET): string {
  return signTestToken(
    {
      sub: "tester",
      userId,
      email: "tester@example.com",
      role: "REGULAR",
      exp: Math.floor(Date.now() / 1000) + 3600,
    },
    secret
  );
}
