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

export type { UserInfo } from "../types/agent";

function decodeJWT(token: string): any {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) {
      throw new Error("Invalid JWT format");
    }
    return JSON.parse(Buffer.from(parts[1], "base64").toString("utf-8"));
  } catch (error) {
    throw new Error(`Failed to decode JWT: ${error}`);
  }
}

export function extractUserFromToken(token: string): UserInfo {
  const payload = decodeJWT(token);
  return {
    uid: payload.userId,
    name: payload.sub,
    email: payload.email || "",
    role: payload.role || "REGULAR",
  };
}

function isTokenExpired(token: string): boolean {
  try {
    const payload = decodeJWT(token);
    if (!payload.exp) return false;
    return Date.now() >= payload.exp * 1000;
  } catch {
    return true;
  }
}

export function validateToken(token: string): boolean {
  return !isTokenExpired(token);
}

export function createAuthHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}
