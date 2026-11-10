/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export { safeReturnUrl } from "./core/auth";

export function accessRedirect(path: string, status: string, role?: string): string | undefined {
  const protectedRoute = /^\/(user|admin)(\/|$)/.test(path);
  if (!protectedRoute) return undefined;
  if (status === "email-required" || status === "approval-required") return "/login";
  if (status !== "authenticated") return `/login?returnUrl=${encodeURIComponent(path)}`;
  if (/^\/admin(\/|$)/.test(path) && role !== "ADMIN") return "/user/workflow";
  return undefined;
}
