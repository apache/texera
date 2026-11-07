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

import { describe, expect, it } from "vitest";
import { accessRedirect, safeReturnUrl } from "./navigation";

describe("navigation access contracts", () => {
  it.each(["/user/workflow/42?tab=console#results", "/hub/workflow/result", "/about"])(
    "preserves a local return URL: %s",
    url => expect(safeReturnUrl(url)).toBe(url)
  );

  it.each([undefined, "", "//outside.example", "https://outside.example", "/\\outside.example", "/login", "/callback/orcid", "javascript:alert(1)", "/%2foutside.example", "/%5coutside.example"])(
    "rejects an unsafe or cyclic return URL: %s",
    url => expect(safeReturnUrl(url)).toBe("/user/workflow")
  );

  it("leaves public routes accessible without a token", () => {
    expect(accessRedirect("/about", "anonymous")).toBeUndefined();
    expect(accessRedirect("/hub/dataset/result/detail/4", "anonymous")).toBeUndefined();
  });

  it("keeps protected deep links through sign-in", () => {
    expect(accessRedirect("/user/workflow/42?tab=console", "anonymous")).toBe(
      "/login?returnUrl=%2Fuser%2Fworkflow%2F42%3Ftab%3Dconsole"
    );
  });

  it("checks admin access and rejects incomplete accounts", () => {
    expect(accessRedirect("/admin/settings", "authenticated", "ADMIN")).toBeUndefined();
    expect(accessRedirect("/admin/settings", "authenticated", "REGULAR")).toBe("/user/workflow");
    expect(accessRedirect("/user/workflow", "email-required", "ADMIN")).toBe("/login");
    expect(accessRedirect("/admin/settings", "approval-required", "INACTIVE")).toBe("/login");
  });
});
