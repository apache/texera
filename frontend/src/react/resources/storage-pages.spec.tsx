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
import { StorageApi, type ResourceKind } from "./storage-api";
import { ResourceCreatePage, ResourceDetailPage, ResourceUploadPanel, sanitizeStorageName } from "./storage-pages";
import { filesFromDrop } from "./storage-upload";
import JSZip from "jszip";
import { NEVER } from "rxjs";
afterEach(cleanup);

it("reads dropped directories recursively across browser entry batches while preserving relative paths", async () => {
  const file = new File(["data"], "a.txt");
  const entry = {
    name: "folder",
    isDirectory: true,
    isFile: false,
    createReader: () => {
      let read = false;
      return {
        readEntries: (done: (entries: unknown[]) => void) => {
          done(
            read
              ? []
              : [{ name: "a.txt", isFile: true, isDirectory: false, file: (done: (file: File) => void) => done(file) }]
          );
          read = true;
        },
      };
    },
  };
  expect(
    await filesFromDrop({
      items: [{ kind: "file", webkitGetAsEntry: () => entry }],
      files: [],
    } as unknown as DataTransfer)
  ).toEqual([{ file, path: "folder/a.txt" }]);
  expect(await filesFromDrop({ items: [], files: [file] } as unknown as DataTransfer)).toEqual([
    { file, path: "a.txt" },
  ]);
});
const change = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });
const user = { uid: 1, name: "Owner", email: "owner@example.com", role: "REGULAR" as const, expiresAt: 9999999999999 };
function setup(
  kind: ResourceKind,
  options: { public?: boolean; writable?: boolean; downloadable?: boolean; fail?: boolean } = {}
) {
  const resource = {
    [kind === "dataset" ? "did" : "mid"]: 7,
    name: "Resource",
    description: "**Description**",
    isPublic: true,
    isDownloadable: options.downloadable ?? true,
    creationTime: 0,
    ownerUid: 1,
    framework: "onnx",
    format: "onnx",
  };
  let staged = [{ path: "new.csv", pathType: "file", diffType: "added", sizeBytes: 10 }];
  const fetch = vi.fn(async (url: string, init: RequestInit = {}) => {
    if (options.fail) return new Response("Denied", { status: 403 });
    if (url.startsWith("https://storage.example")) return new Response('name,value\n"A,B",2\n東京,3');
    if (url.includes("presign-download")) return Response.json({ presignedUrl: "https://storage.example/file" });
    if (url.includes("hub/view")) return Response.json(3);
    if (url.includes("hub/like") || url.includes("hub/unlike")) return Response.json(true);
    if (url.includes("hub/counts"))
      return Response.json([{ entityType: kind, entityId: 7, counts: { like: 2, view: 3 } }]);
    if (url.includes("hub/isLiked") || url.includes("hub/user-access")) return Response.json([]);
    if (url.includes("dashboard/resultsOwnersInfo")) return Response.json({ 1: { userName: "Owner" } });
    if (url.includes("/cover-url")) return Response.json({ url: null });
    if (url.includes("/diff")) {
      if (init.method === "PUT") staged = [];
      return Response.json(init.method ? {} : staged);
    }
    if (url.endsWith("/file") || url.includes("/file?")) return Response.json({});
    if (url.includes("rootFileNodes"))
      return Response.json({
        size: 10,
        fileNodes: [{ name: "file.csv", type: "file", parentDir: `/${kind}/owner@example.com/Resource/v1`, size: 10 }],
      });
    if (url.includes("version/list") || url.includes("publicVersion/list"))
      return Response.json([
        { [kind === "dataset" ? "dvid" : "mvid"]: 1, name: "v1", creationTime: 1000 },
        { [kind === "dataset" ? "dvid" : "mvid"]: 0, name: "v0", creationTime: 0 },
      ]);
    if (url.includes("/version/create"))
      return Response.json({
        [`${kind}Version`]: { [kind === "dataset" ? "dvid" : "mvid"]: 2, name: "v2" },
        fileNodes: [],
      });
    if (url.includes("versionZip")) return new Response("zip");
    if (url.includes("/update/name")) resource.name = JSON.parse(String(init.body)).name;
    return Response.json({
      [kind]: resource,
      ownerEmail: "owner@example.com",
      isOwner: options.writable !== false,
      accessPrivilege: options.writable === false ? "READ" : "WRITE",
      size: 10,
      contributors: [{ name: "Owner", creator: true }],
    });
  });
  const api = new ApiClient({ fetch });
  const storage = new StorageApi(api, { fetch });
  return { api, storage, fetch };
}

async function spreadsheet(): Promise<Blob> {
  const zip = new JSZip();
  zip.file(
    "[Content_Types].xml",
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/></Types>'
  );
  zip.file(
    "xl/workbook.xml",
    '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>'
  );
  zip.file(
    "xl/_rels/workbook.xml.rels",
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>'
  );
  zip.file(
    "xl/worksheets/sheet1.xml",
    '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>City</t></is></c><c r="B1" t="inlineStr"><is><t>Value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>東京</t></is></c><c r="B2"><v>0</v></c></row></sheetData></worksheet>'
  );
  return new Blob([await zip.generateAsync({ type: "uint8array" })]);
}

it("automatically previews the first leaf file and parses an actual Excel workbook, including Unicode and zero", async () => {
  const test = setup("dataset", { writable: false });
  const responder = test.fetch.getMockImplementation()!;
  test.fetch.mockImplementation((url: string, init: RequestInit = {}) =>
    url.includes("rootFileNodes")
      ? Promise.resolve(
          Response.json({
            size: 10,
            fileNodes: [
              {
                name: "folder",
                type: "directory",
                parentDir: "/dataset/owner@example.com/Resource/v1",
                children: [
                  {
                    name: "sheet.xlsx",
                    type: "file",
                    parentDir: "/dataset/owner@example.com/Resource/v1/folder",
                    size: 10,
                  },
                ],
              },
            ],
          })
        )
      : responder(url, init)
  );
  const file = vi.spyOn(test.storage, "file").mockResolvedValue(await spreadsheet());
  render(
    <ResourceDetailPage
      {...test}
      kind="dataset"
      id={7}
      publicView
    />
  );
  await screen.findByText("東京");
  expect(screen.getByRole("cell", { name: "0" })).toBeDefined();
  expect(file).toHaveBeenCalledWith(
    "dataset",
    "/dataset/owner@example.com/Resource/v1/folder/sheet.xlsx",
    true,
    expect.any(AbortSignal)
  );
});

it("rejects oversized previews before fetching bytes and reports malformed workbook errors", async () => {
  const test = setup("model", { writable: false });
  const responder = test.fetch.getMockImplementation()!;
  let large = true;
  test.fetch.mockImplementation((url: string, init: RequestInit = {}) =>
    url.includes("rootFileNodes")
      ? Promise.resolve(
          Response.json({
            size: 20 * 1024 * 1024,
            fileNodes: [
              {
                name: "sheet.xlsx",
                type: "file",
                parentDir: "/model/owner@example.com/Resource/v1",
                size: large ? 20 * 1024 * 1024 : 10,
              },
            ],
          })
        )
      : responder(url, init)
  );
  const file = vi.spyOn(test.storage, "file").mockResolvedValue(new Blob(["Invalid workbook"]));
  const view = render(
    <ResourceDetailPage
      {...test}
      kind="model"
      id={7}
      publicView
    />
  );
  await screen.findByText(/too large to preview/i);
  expect(file).not.toHaveBeenCalled();
  view.unmount();
  large = false;
  render(
    <ResourceDetailPage
      {...test}
      kind="model"
      id={7}
      publicView
    />
  );
  await screen.findByText(/workbook.*could not be read/i);
  expect(screen.getAllByRole("button", { name: "Retry" }).length).toBeGreaterThan(0);
});

it("preserves the different dataset/model name sanitization rules", () => {
  expect(sanitizeStorageName("dataset", " Name_One ! ")).toBe("name-one-");
  expect(sanitizeStorageName("model", " Name_One ! ")).toBe("name_one-");
});

describe.each<ResourceKind>(["dataset", "model"])("%s pages", kind => {
  it("creates a real resource with explicit metadata/contributor inputs and sanitized name", async () => {
    const test = setup(kind);
    const onCreated = vi.fn();
    render(
      <ResourceCreatePage
        api={test.api}
        storage={test.storage}
        kind={kind}
        onCreated={onCreated}
      />
    );
    change("Resource name", " My Resource ");
    change("Resource description", "Description");
    if (kind === "dataset") {
      fireEvent.click(screen.getByRole("button", { name: "Add contributor" }));
      change("Contributor 1 name", "Researcher");
      change("Contributor 1 affiliation", " UCI ");
    } else {
      change("Model framework", "onnx");
      change("Model format", "onnx");
    }
    fireEvent.click(screen.getByRole("button", { name: `Create ${kind}` }));
    await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
    const call = test.fetch.mock.calls.find(([url]) => url.endsWith(`/${kind}/create`))!;
    expect(JSON.parse(String(call[1]?.body))[`${kind}Name`]).toBe("my-resource-");
  });

  it("renders public cards/version files read-only and previews quoted CSV safely", async () => {
    const test = setup(kind, { writable: false });
    render(
      <ResourceDetailPage
        {...test}
        kind={kind}
        id={7}
        publicView
      />
    );
    await screen.findByRole("heading", { name: `${kind === "dataset" ? "Dataset" : "Model"}: Resource` });
    expect(screen.queryByRole("button", { name: "Edit metadata" })).toBeNull();
    expect(screen.queryByLabelText("Upload files")).toBeNull();
    await screen.findByRole("button", { name: "Open file file.csv" });
    fireEvent.click(screen.getByRole("button", { name: "Open file file.csv" }));
    await screen.findByText("A,B");
    expect(screen.getByText("東京")).toBeDefined();
    expect(test.fetch.mock.calls.some(([url]) => url === `/api/${kind}/public/7`)).toBe(true);
    expect(test.fetch.mock.calls.some(([url]) => url.includes(`/${kind}/7/publicVersion/1/rootFileNodes`))).toBe(true);
    expect(test.fetch.mock.calls.some(([url]) => url.includes("/diff"))).toBe(false);
  });

  it("edits metadata and stages/reverts files without changing historical version behavior", async () => {
    const test = setup(kind);
    render(
      <ResourceDetailPage
        {...test}
        kind={kind}
        id={7}
        user={user}
      />
    );
    await screen.findByRole("button", { name: "Edit metadata" });
    fireEvent.click(screen.getByRole("button", { name: "Edit metadata" }));
    change("Resource name", "Renamed");
    fireEvent.click(screen.getByRole("button", { name: "Save metadata" }));
    await screen.findByRole("heading", { name: `${kind === "dataset" ? "Dataset" : "Model"}: Renamed` });
    await screen.findByRole("button", { name: "Revert staged file new.csv" });
    fireEvent.click(screen.getByRole("button", { name: "Revert staged file new.csv" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Revert staged file new.csv" })).toBeNull());
    change("Resource version", "0");
    await waitFor(() =>
      expect(test.fetch.mock.calls.some(([url]) => url.includes(`/${kind}/7/version/0/rootFileNodes`))).toBe(true)
    );
    expect(screen.queryByRole("button", { name: "Stage deletion file.csv" })).toBeNull();
  });

  it("honors download restrictions and keeps rejected mutations visible", async () => {
    const test = setup(kind, { writable: false, downloadable: false });
    render(
      <ResourceDetailPage
        {...test}
        kind={kind}
        id={7}
        publicView
      />
    );
    await screen.findByRole("button", { name: "Open file file.csv" });
    expect(screen.queryByRole("button", { name: "Download version ZIP" })).toBeNull();
  });
});

it("shows failed metadata load with retry and never renders a writable empty placeholder", async () => {
  const test = setup("dataset", { fail: true });
  render(
    <ResourceDetailPage
      {...test}
      kind="dataset"
      id={7}
    />
  );
  await screen.findByText("Denied");
  expect(screen.queryByRole("button", { name: "Edit metadata" })).toBeNull();
  expect(screen.getAllByRole("button", { name: "Retry" }).length).toBeGreaterThan(0);
});

it("guards metadata writes before starting requests when a form is submitted twice", async () => {
  const test = setup("dataset");
  const responder = test.fetch.getMockImplementation()!;
  let finish!: () => void;
  test.fetch.mockImplementation((url: string, init: RequestInit = {}) =>
    url.endsWith("/update/name")
      ? new Promise<Response>(resolve => {
          finish = () => resolve(Response.json({}));
        })
      : responder(url, init)
  );
  render(
    <ResourceDetailPage
      {...test}
      kind="dataset"
      id={7}
      user={user}
    />
  );
  await screen.findByRole("button", { name: "Edit metadata" });
  fireEvent.click(screen.getByRole("button", { name: "Edit metadata" }));
  change("Resource name", "Renamed");
  const form = screen.getByRole("button", { name: "Save metadata" }).closest("form")!;
  fireEvent.submit(form);
  fireEvent.submit(form);
  expect(test.fetch.mock.calls.filter(([url]) => url.endsWith("/update/name"))).toHaveLength(1);
  finish();
  await screen.findByText("Metadata updated.");
});

it("retries only unfinished metadata writes after a partial failure instead of toggling successful permissions back", async () => {
  const test = setup("model");
  const responder = test.fetch.getMockImplementation()!;
  let failed = true;
  test.fetch.mockImplementation((url: string, init: RequestInit = {}) =>
    url.endsWith("/update/downloadable") && failed
      ? Promise.resolve(new Response("Unavailable", { status: 503 }))
      : responder(url, init)
  );
  render(
    <ResourceDetailPage
      {...test}
      kind="model"
      id={7}
      user={user}
    />
  );
  await screen.findByRole("button", { name: "Edit metadata" });
  fireEvent.click(screen.getByRole("button", { name: "Edit metadata" }));
  change("Resource name", "Renamed");
  fireEvent.click(screen.getByLabelText("Public resource"));
  fireEvent.click(screen.getByLabelText("Allow downloads"));
  fireEvent.click(screen.getByRole("button", { name: "Save metadata" }));
  await screen.findByText("Unavailable");
  failed = false;
  fireEvent.click(screen.getByRole("button", { name: "Save metadata" }));
  await screen.findByText("Metadata updated.");
  expect(test.fetch.mock.calls.filter(([url]) => url.endsWith("/update/name"))).toHaveLength(1);
  expect(test.fetch.mock.calls.filter(([url]) => url.endsWith("/update/publicity"))).toHaveLength(1);
  expect(test.fetch.mock.calls.filter(([url]) => url.endsWith("/update/downloadable"))).toHaveLength(2);
});

it("refreshes renamed storage paths and keeps the selected relative file open", async () => {
  const test = setup("model");
  const responder = test.fetch.getMockImplementation()!;
  let name = "Resource";
  test.fetch.mockImplementation((url: string, init: RequestInit = {}) => {
    if (url.includes("/update/name")) name = JSON.parse(String(init.body)).name;
    return url.includes("rootFileNodes")
      ? Promise.resolve(
          Response.json({
            size: 20,
            fileNodes: ["first.csv", "second.csv"].map(file => ({
              name: file,
              type: "file",
              parentDir: `/model/owner@example.com/${name}/v1`,
              size: 10,
            })),
          })
        )
      : responder(url, init);
  });
  const file = vi.spyOn(test.storage, "file").mockResolvedValue(new Blob(["header\nrow"]));
  render(
    <ResourceDetailPage
      {...test}
      kind="model"
      id={7}
      user={user}
    />
  );
  await screen.findByRole("button", { name: "Open file second.csv" });
  fireEvent.click(screen.getByRole("button", { name: "Open file second.csv" }));
  await waitFor(() => expect(file.mock.calls.at(-1)?.[1]).toContain("Resource/v1/second.csv"));
  fireEvent.click(screen.getByRole("button", { name: "Edit metadata" }));
  change("Resource name", "Renamed");
  fireEvent.click(screen.getByRole("button", { name: "Save metadata" }));
  await waitFor(() => expect(file.mock.calls.at(-1)?.[1]).toBe("/model/owner@example.com/Renamed/v1/second.csv"));
});

it("requires an in-flight upload to finish or pause before renaming its resource", async () => {
  const test = setup("dataset");
  vi.spyOn(test.storage, "existingFiles").mockResolvedValue([]);
  vi.spyOn(test.storage, "pendingUploads").mockResolvedValue([]);
  vi.spyOn(test.storage, "upload").mockReturnValue(NEVER);
  render(
    <ResourceDetailPage
      {...test}
      kind="dataset"
      id={7}
      user={user}
    />
  );
  await waitFor(() => expect((screen.getByLabelText("Upload files") as HTMLInputElement).disabled).toBe(false));
  fireEvent.change(screen.getByLabelText("Upload files"), { target: { files: [new File(["data"], "new.txt")] } });
  await screen.findByRole("button", { name: "Pause new.txt" });
  fireEvent.click(screen.getByRole("button", { name: "Edit metadata" }));
  change("Resource name", "Renamed");
  fireEvent.click(screen.getByRole("button", { name: "Save metadata" }));
  await screen.findByText(/finish or cancel.*upload.*renaming/i);
  expect(test.fetch.mock.calls.some(([url]) => url.includes("/update/name"))).toBe(false);
});

it("lets readers page through CSV rows after the first hundred without losing Unicode cells", async () => {
  const test = setup("dataset", { writable: false });
  vi.spyOn(test.storage, "file").mockResolvedValue(
    new Blob(["name,value\n" + Array.from({ length: 150 }, (_, index) => `行${index},${index}`).join("\n")])
  );
  render(
    <ResourceDetailPage
      {...test}
      kind="dataset"
      id={7}
      publicView
    />
  );
  await screen.findByRole("cell", { name: "行0" });
  fireEvent.click(screen.getByRole("button", { name: "Next preview page" }));
  fireEvent.click(screen.getByRole("button", { name: "Next preview page" }));
  expect(await screen.findByRole("cell", { name: "行149" })).toBeDefined();
  expect((screen.getByRole("button", { name: "Next preview page" }) as HTMLButtonElement).disabled).toBe(true);
});

it("uploads through resumable multipart, exposes conflicts and lets skipped files stay untouched", async () => {
  const fetch = vi.fn(async (url: string) =>
    Response.json(
      url.includes("config/settings")
        ? {}
        : url.includes("existing-upload-files")
          ? { filePaths: ["file.txt"] }
          : url.includes("multipart-upload")
            ? new URL(url, "https://test").searchParams.get("type") === "list"
              ? { filePaths: [] }
              : { missingParts: [], completedPartsCount: 0 }
            : {}
    )
  );
  const api = new ApiClient({ fetch });
  const storage = new StorageApi(api, { fetch });
  const changed = vi.fn();
  render(
    <ResourceUploadPanel
      storage={storage}
      kind="dataset"
      id={7}
      name="Name"
      ownerEmail="o@e.com"
      onChanged={changed}
    />
  );
  await waitFor(() => expect((screen.getByLabelText("Upload files") as HTMLInputElement).disabled).toBe(false));
  fireEvent.change(screen.getByLabelText("Upload files"), { target: { files: [new File([], "file.txt")] } });
  await screen.findByRole("dialog", { name: "Conflicting files" });
  fireEvent.click(screen.getByRole("button", { name: "Apply upload choices" }));
  expect(fetch.mock.calls.some(([url]) => new URL(url, "https://test").searchParams.get("type") === "init")).toBe(
    false
  );
  fireEvent.change(screen.getByLabelText("Upload files"), { target: { files: [new File([], "new.txt")] } });
  await waitFor(() => expect(changed).toHaveBeenCalled());
  expect(fetch.mock.calls.some(([url]) => new URL(url, "https://test").searchParams.get("type") === "finish")).toBe(
    true
  );
});
