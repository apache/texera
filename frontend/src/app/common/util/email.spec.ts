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

import { validateEmailFormat } from "./email";

describe("validateEmailFormat", () => {
  it("rejects an empty address as empty rather than as malformed", () => {
    // The two failure messages are what the caller shows the user
    // (`notificationService.error(validation.message)` in AuthService), so the
    // message — not just `result` — is the contract: "you left it blank" and
    // "what you typed is not an address" are different instructions to the user.
    expect(validateEmailFormat("")).toEqual({
      result: false,
      message: "Email should not be empty.",
    });
  });

  it("treats a whitespace-only address as empty, not as a format error", () => {
    // A user who types spaces into the field has effectively left it blank.
    expect(validateEmailFormat("   ")).toEqual({
      result: false,
      message: "Email should not be empty.",
    });
  });

  it("trims surrounding whitespace before validating, so a padded address is accepted", () => {
    // The other half of the trim contract: copy-pasted addresses routinely
    // carry leading/trailing spaces, and the regex's `[^\s@]+` would reject
    // them outright if the value were not trimmed first.
    expect(validateEmailFormat("  someone@example.com  ")).toEqual({
      result: true,
      message: "Email frontend validation success.",
    });
  });

  it("treats a null or undefined address as empty rather than throwing", () => {
    // No current caller can reach this: both of them coalesce one line earlier
    // (`(this.email ?? "").trim()` in email-request-modal.component.ts, and
    // `(this.form.get("email")?.value ?? "").trim()` in
    // texera-login.component.ts), and the declared parameter type is `string`
    // — hence the casts below. The `??` in the implementation is therefore
    // belt-and-braces for a future caller that hands over a raw Angular form
    // control value (`T | null` at runtime); this test pins that defensive
    // contract so the guard is not "cleaned up" later, and it is deliberately
    // NOT counted as coverage of a reachable arm.
    expect(validateEmailFormat(null as unknown as string)).toEqual({
      result: false,
      message: "Email should not be empty.",
    });
    expect(validateEmailFormat(undefined as unknown as string)).toEqual({
      result: false,
      message: "Email should not be empty.",
    });
  });

  // ─── the pattern itself ───────────────────────────────────────────────────
  // The three cases below raise no coverage count: lines 36-38 are already hit
  // indirectly (auth.service.spec.ts and texera-login.component.spec.ts both
  // drive an address with no `@`, and the latter drives `alice@example.com`).
  // What no existing test constrains is the *shape* the pattern accepts, in
  // either direction — so each case below pins one boundary of the rule this
  // function exists for.

  it("rejects an address with no dot after the @, as a format error", () => {
    // Pins the `\.` requirement: without it the rule degrades to "contains an
    // @", which would wave through every internal-looking typo.
    expect(validateEmailFormat("someone@example")).toEqual({
      result: false,
      message: "Email format is invalid.",
    });
  });

  it("rejects trailing junk after an otherwise valid address", () => {
    // Pins the `^`/`$` anchors. Unanchored, the pattern only has to match
    // *somewhere* in the input, so a pasted "Name <a@b.com>" or a second
    // address after a space would be accepted and sent to the backend.
    expect(validateEmailFormat("someone@example.com extra")).toEqual({
      result: false,
      message: "Email format is invalid.",
    });
  });

  it("accepts a non-.com top-level domain", () => {
    // The other direction, and the one that matters most here: this validator
    // gates registration (`UserService.validateEmail`) and the missing-address
    // prompt, and this project's users are largely @uci.edu. A pattern that
    // narrowed to `.com` would lock them out before any round trip, so the
    // permissive `[^\s@]+` tail is pinned as deliberate.
    expect(validateEmailFormat("student@uci.edu")).toEqual({
      result: true,
      message: "Email frontend validation success.",
    });
  });
});
