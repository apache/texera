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
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { ApiClient } from "../core/api-client";
import { ResourceApi } from "./api";
import { AdminMailPage, AdminSettingsPage, PythonEnvironmentsPage, WarehousePage } from "./pages";

afterEach(cleanup);
function setup(responder: (url: string, init: RequestInit) => Response | Promise<Response>) {
  const fetch = vi.fn((url: string, init: RequestInit = {}) => responder(url, init));
  return { fetch, resources: new ResourceApi(new ApiClient({ fetch })) };
}
const warehouse = {
  whid: 1,
  name: "Original",
  ownerName: "Owner",
  ownerAvatar: null,
  flavor: "local",
  lakekeeperWarehouseName: "catalog",
  createdAtMillis: 0,
};
const change = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });

describe("WarehousePage", () => {
  it("distinguishes disabled deployment from failed status and retries a failure", async () => {
    let failed = true;
    const test = setup(() =>
      failed ? new Response("Offline", { status: 503 }) : Response.json({ enabled: false, warehouses: [] })
    );
    render(<WarehousePage resources={test.resources} />);
    expect(await screen.findByRole("alert")).toHaveProperty("textContent", expect.stringContaining("Offline"));
    expect(screen.queryByText(/disabled in this deployment/i)).toBeNull();
    failed = false;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(await screen.findByText(/disabled in this deployment/i)).toBeDefined();
    expect(screen.queryByRole("button", { name: "Create warehouse" })).toBeNull();
  });

  it("creates a valid warehouse, rejects invalid names and confirms destructive deletion", async () => {
    const test = setup((url, init) =>
      url.endsWith("/status")
        ? Response.json({ enabled: true, warehouses: [warehouse] })
        : init.method === "POST"
          ? Response.json({ ...warehouse, whid: 2, name: "New_1" })
          : new Response(null, { status: 204 })
    );
    render(<WarehousePage resources={test.resources} />);
    await screen.findByText("Original");
    fireEvent.click(screen.getByRole("button", { name: "Create warehouse" }));
    change("Warehouse name", "_invalid");
    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    expect(test.fetch).toHaveBeenCalledTimes(1);
    change("Warehouse name", " New_1 ");
    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await screen.findByText("New_1");
    expect(JSON.parse(String(test.fetch.mock.calls[1][1].body))).toEqual({ name: "New_1" });
    fireEvent.click(screen.getByRole("button", { name: "Delete warehouse New_1" }));
    const dialog = screen.getByRole("dialog", { name: 'Delete warehouse "New_1"?' });
    expect(dialog.textContent).toContain("all data stored in it");
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(test.fetch).toHaveBeenCalledTimes(2);
    fireEvent.click(screen.getByRole("button", { name: "Delete warehouse New_1" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.queryByText("New_1")).toBeNull());
    expect(test.fetch.mock.calls.at(-1)?.[0]).toBe("/api/warehouse/2");
  });

  it("retains rows on failed deletion and prevents duplicate submissions while pending", async () => {
    let reject!: (error: Error) => void;
    const test = setup((url, init) =>
      init.method === "DELETE"
        ? new Promise((_resolve, fail) => {
            reject = fail;
          })
        : Response.json({ enabled: true, warehouses: [warehouse] })
    );
    render(<WarehousePage resources={test.resources} />);
    await screen.findByText("Original");
    fireEvent.click(screen.getByRole("button", { name: "Delete warehouse Original" }));
    const button = within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" });
    fireEvent.click(button);
    fireEvent.click(button);
    expect(test.fetch).toHaveBeenCalledTimes(2);
    reject(new Error("Cannot delete"));
    expect(await screen.findByRole("alert")).toHaveProperty("textContent", expect.stringContaining("Cannot delete"));
    expect(screen.getByText("Original")).toBeDefined();
  });
});

describe("PythonEnvironmentsPage", () => {
  it("creates and edits package rows with unchanged operators and exact database contracts", async () => {
    let records = [{ veid: 1, name: "Existing", packages: { numpy: ">=2", pandas: "" } }];
    const test = setup((url, init) => {
      if (init.method === "POST") records = [...records, { veid: 2, ...JSON.parse(String(init.body)) }];
      if (init.method === "PUT")
        records = records.map(record => (record.veid === 1 ? { veid: 1, ...JSON.parse(String(init.body)) } : record));
      return Response.json(init.method ? { veid: 2 } : records);
    });
    render(<PythonEnvironmentsPage resources={test.resources} />);
    await screen.findByRole("button", { name: "Edit environment Existing" });
    fireEvent.click(screen.getByRole("button", { name: "Create environment" }));
    change("Environment name", "Env2");
    fireEvent.click(screen.getByRole("button", { name: "Add package" }));
    change("Package 1 name", "scipy");
    change("Package 1 operator", "<=");
    change("Package 1 version", "2");
    fireEvent.click(screen.getByRole("button", { name: "Save environment" }));
    await screen.findByRole("button", { name: "Edit environment Env2" });
    expect(JSON.parse(String(test.fetch.mock.calls.find(([, init]) => init.method === "POST")?.[1].body))).toEqual({
      name: "Env2",
      packages: { scipy: "<=2" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Edit environment Existing" }));
    expect((screen.getByLabelText("Package 1 operator") as HTMLSelectElement).value).toBe(">=");
    change("Environment name", "Edited");
    fireEvent.click(screen.getByRole("button", { name: "Save environment" }));
    await screen.findByRole("button", { name: "Edit environment Edited" });
    expect(test.fetch.mock.calls.some(([url, init]) => url === "/api/pve/db/1" && init.method === "PUT")).toBe(true);
  });

  it("validates alphanumeric names, duplicate environments and duplicate packages before sending", async () => {
    const test = setup(() => Response.json([{ veid: 1, name: "Existing", packages: {} }]));
    render(<PythonEnvironmentsPage resources={test.resources} />);
    await screen.findByRole("button", { name: "Edit environment Existing" });
    fireEvent.click(screen.getByRole("button", { name: "Create environment" }));
    for (const [name, message] of [
      ["bad_name", "letters and numbers"],
      ["Existing", "already exists"],
    ]) {
      change("Environment name", name);
      fireEvent.click(screen.getByRole("button", { name: "Save environment" }));
      expect(screen.getByRole("alert").textContent).toContain(message);
    }
    change("Environment name", "Valid");
    fireEvent.click(screen.getByRole("button", { name: "Add package" }));
    fireEvent.click(screen.getByRole("button", { name: "Add package" }));
    change("Package 1 name", "numpy");
    change("Package 2 name", "numpy");
    fireEvent.click(screen.getByRole("button", { name: "Save environment" }));
    expect(screen.getByRole("alert").textContent).toContain("Duplicate package");
    expect(test.fetch).toHaveBeenCalledTimes(1);
  });

  it("cancel leaves saved data intact and deletion requires confirmation", async () => {
    let records = [{ veid: 1, name: "Existing", packages: { numpy: "==2" } }];
    const test = setup((_url, init) => {
      if (init.method === "DELETE") records = [];
      return Response.json(init.method ? {} : records);
    });
    render(<PythonEnvironmentsPage resources={test.resources} />);
    await screen.findByRole("button", { name: "Edit environment Existing" });
    fireEvent.click(screen.getByRole("button", { name: "Edit environment Existing" }));
    change("Environment name", "Unsaved");
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    fireEvent.click(screen.getByRole("button", { name: "Edit environment Existing" }));
    expect((screen.getByLabelText("Environment name") as HTMLInputElement).value).toBe("Existing");
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    fireEvent.click(screen.getByRole("button", { name: "Delete environment Existing" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" }));
    await screen.findByText(/No environments yet/);
    expect(test.fetch.mock.calls.some(([url, init]) => url === "/api/pve/db/1" && init.method === "DELETE")).toBe(true);
  });

  it("shows load errors with retry and preserves draft when save fails", async () => {
    let failLoad = true;
    const test = setup((_url, init) =>
      init.method
        ? new Response("Save denied", { status: 403 })
        : failLoad
          ? new Response("Offline", { status: 503 })
          : Response.json([])
    );
    render(<PythonEnvironmentsPage resources={test.resources} />);
    await screen.findByRole("alert");
    failLoad = false;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await screen.findByText(/No environments yet/);
    fireEvent.click(screen.getByRole("button", { name: "Create environment" }));
    change("Environment name", "Draft");
    fireEvent.click(screen.getByRole("button", { name: "Save environment" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Save denied"));
    expect((screen.getByLabelText("Environment name") as HTMLInputElement).value).toBe("Draft");
  });
});

describe("AdminMailPage", () => {
  it("loads sender, validates fields and reports successful mail delivery", async () => {
    const test = setup((_url, init) =>
      init.method ? new Response(null, { status: 204 }) : new Response("sender@example.com")
    );
    render(<AdminMailPage resources={test.resources} />);
    await screen.findByText("sender@example.com");
    fireEvent.click(screen.getByRole("button", { name: "Send test email" }));
    expect(test.fetch).toHaveBeenCalledTimes(1);
    change("Recipient email", "bad");
    change("Subject", "Subject");
    change("Message", "Message");
    fireEvent.click(screen.getByRole("button", { name: "Send test email" }));
    expect(test.fetch).toHaveBeenCalledTimes(1);
    change("Recipient email", "to@example.com");
    fireEvent.click(screen.getByRole("button", { name: "Send test email" }));
    await screen.findByText("Email sent successfully");
    expect(JSON.parse(String(test.fetch.mock.calls.at(-1)?.[1].body))).toEqual({
      receiver: "to@example.com",
      subject: "Subject",
      content: "Message",
    });
  });
  it("reports sender/send failures as escaped text", async () => {
    const test = setup((_url, init) => new Response(init.method ? "<b>send denied</b>" : "No sender", { status: 503 }));
    const rendered = render(<AdminMailPage resources={test.resources} />);
    await screen.findByText(/No sender/);
    change("Recipient email", "to@example.com");
    change("Subject", "Hi");
    change("Message", "Hello");
    fireEvent.click(screen.getByRole("button", { name: "Send test email" }));
    await screen.findByText("<b>send denied</b>");
    expect(rendered.container.querySelector("b")).toBeNull();
  });
});

describe("AdminSettingsPage", () => {
  it("loads real settings, edits explicit sidebar controls and writes string values", async () => {
    const changed = vi.fn();
    const test = setup((_url, init) =>
      init.method
        ? new Response(null, { status: 204 })
        : Response.json({ hub_enabled: "true", home_enabled: "true", csv_parser_max_columns: "1000" })
    );
    render(
      <AdminSettingsPage
        resources={test.resources}
        onSettingsChanged={changed}
      />
    );
    await waitFor(() => expect((screen.getByLabelText("Hub") as HTMLInputElement).checked).toBe(true));
    fireEvent.click(screen.getByLabelText("Home"));
    fireEvent.click(screen.getByRole("button", { name: "Save sidebar tabs" }));
    await waitFor(() => expect(changed).toHaveBeenCalledTimes(1));
    expect(
      test.fetch.mock.calls.some(
        ([url, init]) => url.endsWith("/home_enabled") && JSON.parse(String(init.body)).value === "false"
      )
    ).toBe(true);
    expect((screen.getByLabelText("Maximum CSV columns") as HTMLInputElement).value).toBe("1000");
  });
  it("guards writes after failed load, retries and refuses invalid multipart/CSV limits", async () => {
    let failed = true;
    const test = setup(() => (failed ? new Response("Offline", { status: 503 }) : Response.json({})));
    render(<AdminSettingsPage resources={test.resources} />);
    await screen.findByRole("alert");
    expect((screen.getByRole("button", { name: "Save sidebar tabs" }) as HTMLButtonElement).disabled).toBe(true);
    failed = false;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() =>
      expect((screen.getByRole("button", { name: "Save sidebar tabs" }) as HTMLButtonElement).disabled).toBe(false)
    );
    change("Dataset max file size (MiB)", "50001");
    change("Dataset part size (MiB)", "5");
    fireEvent.click(screen.getByRole("button", { name: "Save dataset upload settings" }));
    expect(screen.getByRole("alert").textContent).toContain("10,000");
    change("Maximum CSV columns", "0");
    fireEvent.click(screen.getByRole("button", { name: "Save result panel settings" }));
    expect(screen.getByRole("alert").textContent).toContain("1 and 100000");
    expect(test.fetch).toHaveBeenCalledTimes(2);
  });
  it("saves resource-family keys and resets only the requested group", async () => {
    const changed = vi.fn();
    const test = setup((_url, init) => (init.method ? new Response(null, { status: 204 }) : Response.json({})));
    render(
      <AdminSettingsPage
        resources={test.resources}
        onSettingsChanged={changed}
      />
    );
    await waitFor(() =>
      expect((screen.getByRole("button", { name: "Save model upload settings" }) as HTMLButtonElement).disabled).toBe(
        false
      )
    );
    fireEvent.click(screen.getByRole("button", { name: "Save model upload settings" }));
    await screen.findByText("Model upload settings saved successfully.");
    expect(
      test.fetch.mock.calls
        .filter(([, init]) => init.method === "PUT")
        .map(([url]) => url.split("/").at(-1))
        .sort()
    ).toEqual([
      "model_max_number_of_concurrent_uploading_file",
      "model_max_number_of_concurrent_uploading_file_chunks",
      "model_multipart_upload_chunk_size_mib",
      "model_single_file_upload_max_size_mib",
    ]);
    fireEvent.click(screen.getByRole("button", { name: "Reset result panel settings" }));
    await waitFor(() => expect(changed).toHaveBeenCalledTimes(1));
    expect(test.fetch.mock.calls.at(-1)?.[0]).toBe("/api/config/settings/reset/csv_parser_max_columns");
  });
  it("validates branding files and keeps server failure visible without reloading", async () => {
    const changed = vi.fn();
    const test = setup((_url, init) => (init.method ? new Response("Denied", { status: 403 }) : Response.json({})));
    render(
      <AdminSettingsPage
        resources={test.resources}
        onSettingsChanged={changed}
      />
    );
    await waitFor(() =>
      expect((screen.getByRole("button", { name: "Save branding" }) as HTMLButtonElement).disabled).toBe(false)
    );
    fireEvent.change(screen.getByLabelText("Logo image"), {
      target: { files: [new File(["text"], "bad.txt", { type: "text/plain" })] },
    });
    expect(screen.getByRole("alert").textContent).toContain("valid image");
    fireEvent.change(screen.getByLabelText("Logo image"), {
      target: { files: [new File(["png"], "logo.png", { type: "image/png" })] },
    });
    await screen.findByAltText("Logo preview");
    fireEvent.click(screen.getByRole("button", { name: "Save branding" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Denied"));
    expect(changed).not.toHaveBeenCalled();
  });
});
