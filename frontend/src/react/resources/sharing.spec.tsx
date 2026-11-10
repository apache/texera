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
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ApiClient } from "../core/api-client";
import { ResourceShareDialog } from "./sharing";
afterEach(cleanup);
const user = { uid: 7, name: "Owner", email: "owner@example.test", role: "REGULAR" as const, expiresAt: Infinity };
function setup(kind: "dataset" | "model" | "computing-unit" = "dataset", readOnly = false) {
  let entries = [{ name: "Reader", email: "reader@example.test", privilege: "READ" }];
  let isPublic = false;
  const fetch = vi.fn(async (url: string, _init?: RequestInit) => {
    if (url.includes("/owner/")) return new Response("owner@example.test");
    if (url.includes("/list/")) return Response.json(entries);
    if (url.includes("/grant/")) {
      const parts = url.split("/");
      const email = decodeURIComponent(parts.at(-2)!);
      entries = [...entries.filter(entry => entry.email !== email), { name: email, email, privilege: parts.at(-1)! }];
      return new Response(null, { status: 204 });
    }
    if (url.includes("/revoke/")) {
      entries = entries.filter(entry => entry.email !== decodeURIComponent(url.split("/").at(-1)!));
      return new Response(null, { status: 204 });
    }
    if (url.includes("/publicity")) {
      isPublic = !isPublic;
      return Response.json({});
    }
    if (url.includes("gmail/send")) return new Response(null, { status: 204 });
    if (url.includes("owners")) return Response.json(["suggested@example.test"]);
    return Response.json({
      [kind]: {
        [kind === "dataset" ? "did" : "mid"]: 7,
        name: "Resource",
        description: "",
        isPublic,
        isDownloadable: true,
      },
    });
  });
  const close = vi.fn();
  const api = new ApiClient({ fetch });
  render(
    <ResourceShareDialog
      api={api}
      kind={kind}
      id={7}
      user={readOnly ? { ...user, email: "reader@example.test" } : user}
      onClose={close}
    />
  );
  return { fetch, close };
}
it.each(["dataset", "model", "computing-unit"] as const)(
  "grants %s access through exact encoded contracts and sends an invitation only after success",
  async kind => {
    const { fetch } = setup(kind);
    await screen.findByText("reader@example.test");
    fireEvent.change(screen.getByLabelText("Invite email addresses"), {
      target: { value: "one+tag@example.test; second@example.test" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Grant access" }));
    await waitFor(() => expect(fetch.mock.calls.filter(([url]) => url.includes("/grant/")).length).toBe(2));
    expect(fetch.mock.calls.some(([url]) => url === `/api/access/${kind}/grant/7/one%2Btag%40example.test/WRITE`)).toBe(
      true
    );
    await waitFor(() => expect(fetch.mock.calls.filter(([url]) => url.includes("gmail/send"))).toHaveLength(2));
    expect(
      fetch.mock.calls
        .filter(([url]) => url.includes("gmail/send"))
        .every(([, init]) => String(init?.body).includes(kind))
    ).toBe(true);
    if (kind === "computing-unit") expect(screen.queryByRole("button", { name: "Publish" })).toBeNull();
  }
);
it("confirms publish and revoke, and renders read-only access without mutation controls", async () => {
  const { fetch } = setup();
  await screen.findByText("reader@example.test");
  fireEvent.click(screen.getByRole("button", { name: "Publish" }));
  expect(fetch.mock.calls.some(([url]) => url.includes("publicity"))).toBe(false);
  fireEvent.click(screen.getByRole("button", { name: "Confirm change" }));
  await screen.findByText("Public");
  fireEvent.click(screen.getByRole("button", { name: "Revoke reader@example.test" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm change" }));
  await waitFor(() =>
    expect(fetch.mock.calls.some(([url]) => url.includes("/revoke/7/reader%40example.test"))).toBe(true)
  );
  cleanup();
  setup("model", true);
  await screen.findByText("reader@example.test");
  expect(screen.queryByRole("button", { name: "Grant access" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Publish" })).toBeNull();
});
it("rejects invalid email batches before any grant request and keeps server failures visible", async () => {
  const { fetch } = setup();
  await screen.findByText("reader@example.test");
  fireEvent.change(screen.getByLabelText("Invite email addresses"), { target: { value: "not-an-email" } });
  fireEvent.click(screen.getByRole("button", { name: "Grant access" }));
  await screen.findByText(/valid email/i);
  expect(fetch.mock.calls.some(([url]) => url.includes("/grant/"))).toBe(false);
  const responder = fetch.getMockImplementation()!;
  fetch.mockImplementation((url, init) =>
    url.includes("/grant/") ? Promise.resolve(new Response("Denied", { status: 403 })) : responder(url, init)
  );
  fireEvent.change(screen.getByLabelText("Invite email addresses"), { target: { value: "new@example.test" } });
  fireEvent.click(screen.getByRole("button", { name: "Grant access" }));
  await screen.findByText("Denied");
  expect(fetch.mock.calls.some(([url]) => url.includes("gmail/send"))).toBe(false);
});
it("retains completed recipients after a partial batch failure and retries only the unsent invitation", async () => {
  const { fetch } = setup();
  await screen.findByText("reader@example.test");
  const responder = fetch.getMockImplementation()!;
  let failing = true;
  fetch.mockImplementation((url, init) =>
    failing && url.includes("/grant/7/fail%40example.test")
      ? Promise.resolve(new Response("Unavailable", { status: 503 }))
      : responder(url, init)
  );
  fireEvent.change(screen.getByLabelText("Invite email addresses"), {
    target: { value: "good@example.test; fail@example.test" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Grant access" }));
  await screen.findByText("Unavailable");
  expect(screen.getByRole("row", { name: /good@example.test/ })).toBeDefined();
  expect((screen.getByLabelText("Invite email addresses") as HTMLTextAreaElement).value).toBe("fail@example.test");
  failing = false;
  fireEvent.click(screen.getByRole("button", { name: "Grant access" }));
  await screen.findByText("Access granted.");
  expect(fetch.mock.calls.filter(([url]) => url.includes("/grant/7/good%40example.test"))).toHaveLength(1);
});
