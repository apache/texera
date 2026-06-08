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

import { describe, expect, test, afterEach } from "bun:test";
import { persistWorkflow, retrieveWorkflow } from "./workflow-client";
import { ExecutionMode, type WorkflowContent } from "../types/workflow";

const realFetch = globalThis.fetch;

interface CapturedRequest {
  url: string;
  init: any;
}

let captured: CapturedRequest | undefined;

function mockFetch(responder: (req: CapturedRequest) => Response): void {
  globalThis.fetch = (async (input: any, init: any) => {
    captured = { url: String(input), init };
    return responder(captured);
  }) as unknown as typeof fetch;
}

afterEach(() => {
  globalThis.fetch = realFetch;
  captured = undefined;
});

const CONTENT: WorkflowContent = {
  operators: [],
  operatorPositions: {},
  links: [],
  commentBoxes: [],
  settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
};

describe("retrieveWorkflow", () => {
  test("GETs the workflow with a bearer token and parses the stringified content", async () => {
    mockFetch(
      () => new Response(JSON.stringify({ wid: 5, name: "W", content: JSON.stringify(CONTENT) }), { status: 200 })
    );

    const workflow = await retrieveWorkflow("tok", 5);

    expect(captured?.url).toBe("http://localhost:8080/api/workflow/5");
    expect(captured?.init.method).toBe("GET");
    expect(captured?.init.headers.Authorization).toBe("Bearer tok");
    expect(workflow.content).toEqual(CONTENT);
  });

  test("throws when the response is not OK", async () => {
    mockFetch(() => new Response("missing", { status: 404, statusText: "Not Found" }));

    await expect(retrieveWorkflow("tok", 5)).rejects.toThrow("Failed to retrieve workflow: 404");
  });
});

describe("persistWorkflow", () => {
  test("POSTs the workflow with a stringified content payload", async () => {
    mockFetch(
      () => new Response(JSON.stringify({ wid: 5, name: "W", content: JSON.stringify(CONTENT) }), { status: 200 })
    );

    const workflow = await persistWorkflow("tok", 5, "W", CONTENT, "desc");

    expect(captured?.url).toBe("http://localhost:8080/api/workflow/persist");
    expect(captured?.init.method).toBe("POST");
    expect(captured?.init.headers.Authorization).toBe("Bearer tok");

    const body = JSON.parse(captured!.init.body);
    expect(body.wid).toBe(5);
    expect(body.name).toBe("W");
    expect(body.description).toBe("desc");
    expect(body.isPublic).toBe(false);
    expect(typeof body.content).toBe("string");
    expect(JSON.parse(body.content)).toEqual(CONTENT);
    expect(workflow.content).toEqual(CONTENT);
  });

  test("throws when persistence fails", async () => {
    mockFetch(() => new Response("bad", { status: 400, statusText: "Bad Request" }));

    await expect(persistWorkflow("tok", 5, "W", CONTENT)).rejects.toThrow("Failed to persist workflow: 400");
  });
});
