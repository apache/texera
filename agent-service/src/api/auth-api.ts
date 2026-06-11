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

import type { UserInfo } from "../types/agent";
import { verifyToken } from "../config/jwt";

export type { UserInfo } from "../types/agent";
export { verifyToken } from "../config/jwt";

/** Verify the token's signature + expiry and return its user claims. Throws on
 *  an invalid/expired/mis-signed token. */
export function extractUserFromToken(token: string): UserInfo {
  const user = verifyToken(token);
  if (!user) {
    throw new Error("Invalid or expired token");
  }
  return {
    uid: user.uid,
    name: user.name,
    email: user.email,
    role: user.role,
  };
}

/** True only when the token is genuinely valid (signature + expiry verified). */
export function validateToken(token: string): boolean {
  return verifyToken(token) !== null;
}

export function createAuthHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}
