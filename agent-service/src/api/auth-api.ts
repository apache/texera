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

/**
 * Authentication API client for Texera Agent Service.
 * Handles user authentication against the Texera backend.
 */

import { getBackendConfig } from "./backend-api";
import type { UserInfo } from "../types/agent";

// Re-export UserInfo for backwards compatibility
export type { UserInfo } from "../types/agent";

// ============================================================================
// Types
// ============================================================================

export interface LoginResponse {
  accessToken: string;
}

export interface AuthResult {
  accessToken: string;
  user: UserInfo;
}

// ============================================================================
// JWT Parsing
// ============================================================================

/**
 * Decode a JWT token and extract the payload.
 * Note: This does not verify the signature, only decodes.
 */
function decodeJWT(token: string): any {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) {
      throw new Error("Invalid JWT format");
    }
    const payload = parts[1];
    const decoded = Buffer.from(payload, "base64").toString("utf-8");
    return JSON.parse(decoded);
  } catch (error) {
    throw new Error(`Failed to decode JWT: ${error}`);
  }
}

/**
 * Extract user info from a JWT token.
 */
export function extractUserFromToken(token: string): UserInfo {
  const payload = decodeJWT(token);
  return {
    uid: payload.userId,
    name: payload.sub,
    email: payload.email || "",
    role: payload.role || "REGULAR",
  };
}

/**
 * Check if a JWT token is expired.
 */
export function isTokenExpired(token: string): boolean {
  try {
    const payload = decodeJWT(token);
    if (!payload.exp) {
      return false; // No expiration claim
    }
    const expirationTime = payload.exp * 1000; // Convert to milliseconds
    return Date.now() >= expirationTime;
  } catch {
    return true; // Assume expired if we can't decode
  }
}

// ============================================================================
// API Functions
// ============================================================================

/**
 * Login to the Texera backend with username and password.
 * @param username - User's username
 * @param password - User's password
 * @returns AuthResult containing access token and user info
 */
export async function login(username: string, password: string): Promise<AuthResult> {
  const config = getBackendConfig();
  const url = `${config.apiEndpoint}/api/auth/login`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ username, password }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Login failed: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const data: LoginResponse = await response.json();
  const user = extractUserFromToken(data.accessToken);

  return {
    accessToken: data.accessToken,
    user,
  };
}

/**
 * Validate an existing token by checking if it's expired.
 * @param token - JWT token to validate
 * @returns true if token is valid and not expired
 */
export function validateToken(token: string): boolean {
  return !isTokenExpired(token);
}

/**
 * Create authorization headers for API requests.
 * @param token - JWT token
 * @returns Headers object with Authorization header
 */
export function createAuthHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}
