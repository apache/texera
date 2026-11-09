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

import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
import { persistWorkflow, retrieveWorkflow, type Workflow } from "./workflow-api";
import type { WorkflowContent } from "../types/workflow";

const TOKEN = "tok-123";

const content = {
  operators: [{ operatorID: "opX", operatorType: "CSVFileScan" }],
  links: [],
} as unknown as WorkflowContent;

/** The backend stores `content` as a JSON string, so responses echo it back in that form. */
function storedWorkflow(overrides: Partial<Workflow> = {}): Record<string, unknown> {
  return { wid: 5, name: "flow", content: JSON.stringify(content), ...overrides };
}

describe("persistWorkflow", () => {
  afterEach(() => {
    mock.restore();
  });

  test("POSTs the workflow with the caller's bearer token", async () => {
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(storedWorkflow()), { status: 200 })
    );

    await persistWorkflow(TOKEN, 5, "flow", content, "a description");

    const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(url).toMatch(/\/api\/workflow\/persist$/);
    expect(init.method).toBe("POST");
    expect(init.headers).toEqual({
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    });
  });

  test("serializes the content as a nested JSON string, not a nested object", async () => {
    // The persist endpoint takes `content` as a string field. Sending the object directly is the
    // obvious-looking mistake and the backend rejects it, so the double encoding is pinned here.
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(storedWorkflow()), { status: 200 })
    );

    await persistWorkflow(TOKEN, 5, "flow", content, "a description");

    const body = JSON.parse((fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string);
    expect(typeof body.content).toBe("string");
    expect(JSON.parse(body.content)).toEqual(content);
    expect(body).toMatchObject({ wid: 5, name: "flow", description: "a description", isPublic: false });
  });

  test("sends an empty description when none is given", async () => {
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(storedWorkflow()), { status: 200 })
    );

    await persistWorkflow(TOKEN, 5, "flow", content);

    const body = JSON.parse((fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string);
    expect(body.description).toBe("");
  });

  test("parses the stringified content on the way back out", async () => {
    spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify(storedWorkflow()), { status: 200 }));

    const saved = await persistWorkflow(TOKEN, 5, "flow", content);

    expect(saved.content).toEqual(content);
  });

  test("leaves an already-parsed content object alone", async () => {
    spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ wid: 5, name: "flow", content }), { status: 200 })
    );

    const saved = await persistWorkflow(TOKEN, 5, "flow", content);

    expect(saved.content).toEqual(content);
  });

  test("reports the status and the server's message when the save is refused", async () => {
    spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("workflow is read-only", { status: 403, statusText: "Forbidden" })
    );

    await expect(persistWorkflow(TOKEN, 5, "flow", content)).rejects.toThrow(
      /Failed to persist workflow: 403 Forbidden - workflow is read-only/
    );
  });
});

describe("retrieveWorkflow", () => {
  afterEach(() => {
    mock.restore();
  });

  test("GETs the workflow by id with the caller's bearer token", async () => {
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(storedWorkflow()), { status: 200 })
    );

    await retrieveWorkflow(TOKEN, 5);

    const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(url).toMatch(/\/api\/workflow\/5$/);
    expect(init.method).toBe("GET");
    expect(init.headers).toEqual({
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    });
  });

  test("parses the stringified content on the way back out", async () => {
    spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify(storedWorkflow()), { status: 200 }));

    const loaded = await retrieveWorkflow(TOKEN, 5);

    expect(loaded.content).toEqual(content);
  });

  test("reports the status and the server's message when the workflow is missing", async () => {
    spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("no such workflow", { status: 404, statusText: "Not Found" })
    );

    await expect(retrieveWorkflow(TOKEN, 5)).rejects.toThrow(
      /Failed to retrieve workflow: 404 Not Found - no such workflow/
    );
  });
});
