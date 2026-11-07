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

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ApiClient } from "../core/api-client";
import { WorkflowRepository } from "./workflow-repository";
import { WorkflowShareDialog } from "./workflow-list-page";
afterEach(cleanup);

const entry = {
  workflow: { wid: 4, name: "Workflow", content: {} },
  isOwner: false,
  ownerId: 8,
  accessLevel: "WRITE",
} as never;
const user = { uid: 7, name: "Tester", email: "tester@example.org", role: "REGULAR" as const, expiresAt: 9999999999 };
function mount(write = true, notifyFails = false) {
  let privilege = write ? "WRITE" : "READ";
  let published = false;
  const fetch = vi.fn(async (path: string) => {
    if (path.includes("/access/workflow/list/"))
      return Response.json([{ email: user.email, name: user.name, privilege }]);
    if (path.includes("/access/workflow/owner/")) return new Response("owner@example.org");
    if (path.includes("/workflow/type/")) return new Response(published ? "Public" : "Private");
    if (path.includes("/workflow/public/")) published = true;
    if (path.includes("/grant/") && path.endsWith("/READ") && path.includes(encodeURIComponent(user.email)))
      privilege = "READ";
    if (path === "/api/gmail/send" && notifyFails) return new Response("Mail unavailable", { status: 503 });
    return Response.json({});
  });
  const close = vi.fn();
  render(
    <WorkflowShareDialog
      entry={entry}
      repository={new WorkflowRepository(new ApiClient({ fetch }))}
      user={user}
      close={close}
    />
  );
  return { fetch, close };
}
describe("workflow sharing in React", () => {
  it("uses server-listed permissions to keep a reader's dialog read only", async () => {
    mount(false);
    expect(await screen.findByText("Owner: owner@example.org")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Grant access" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Publish workflow" })).toBeNull();
    expect(screen.queryByRole("button", { name: /Revoke tester/ })).toBeNull();
  });
  it("validates a recipient before sending a grant and retains granted access if notification fails", async () => {
    const { fetch } = mount(true, true);
    await screen.findByRole("button", { name: "Grant access" });
    fireEvent.change(screen.getByLabelText("Email addresses"), { target: { value: "invalid" } });
    fireEvent.click(screen.getByRole("button", { name: "Grant access" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Email"));
    expect(fetch.mock.calls.some(([path]) => path.includes("/grant/"))).toBe(false);
    fireEvent.change(screen.getByLabelText("Email addresses"), { target: { value: "a+b@example.org" } });
    fireEvent.click(screen.getByRole("button", { name: "Grant access" }));
    await waitFor(() => expect(screen.getByRole("status").textContent).toContain("Access was granted"));
    expect(fetch.mock.calls.some(([path]) => path.endsWith("/a%2Bb%40example.org/WRITE"))).toBe(true);
  });
  it("requires confirmation before revoking the viewer's own access", async () => {
    const { fetch, close } = mount();
    fireEvent.click(await screen.findByRole("button", { name: "Revoke tester@example.org" }));
    expect(fetch.mock.calls.some(([path]) => path.includes("/revoke/"))).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Confirm access change" }));
    await waitFor(() => expect(close).toHaveBeenCalledTimes(1));
    expect(fetch.mock.calls.some(([path]) => path.endsWith("/revoke/4/tester%40example.org"))).toBe(true);
  });
  it("requires confirmation for self-downgrade and then removes management controls", async () => {
    const { fetch } = mount();
    await screen.findByLabelText("Access for tester@example.org");
    fireEvent.change(screen.getByLabelText("Access for tester@example.org"), { target: { value: "READ" } });
    expect(fetch.mock.calls.some(([path]) => path.includes("/grant/"))).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Confirm access change" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Grant access" })).toBeNull());
  });
  it("publishes through the existing endpoint and refreshes the real visibility state", async () => {
    const { fetch } = mount();
    fireEvent.click(await screen.findByRole("button", { name: "Publish workflow" }));
    expect(await screen.findByRole("button", { name: "Make private" })).toBeTruthy();
    expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/public/4")).toBe(true);
  });
});
