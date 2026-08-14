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
 * Syntactic check for an address the user typed, shared by every form that collects one:
 * registration (`UserService.validateEmail`) and the prompt an identity-only login triggers
 * (`AuthService`). It lives here rather than on either service because those two import each
 * other's module, and one rule in one place is the point.
 *
 * Authoritative validation stays on the backend (`EmailUtil.isValid`); this only catches the
 * typo before a round trip.
 */
export function validateEmailFormat(email: string): { result: boolean; message: string } {
  const trimmed = (email ?? "").trim();
  if (trimmed.length === 0) {
    return { result: false, message: "Email should not be empty." };
  }
  // Pragmatic email regex: non-whitespace + @ + non-whitespace + . + non-whitespace.
  // Matches what most users expect; we leave authoritative validation to the backend.
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(trimmed)) {
    return { result: false, message: "Email format is invalid." };
  }
  return { result: true, message: "Email frontend validation success." };
}
