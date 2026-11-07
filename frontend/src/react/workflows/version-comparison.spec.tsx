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
import { VersionComparison } from "./version-comparison";
import { ApiClient } from "../core/api-client";
vi.mock("../code-editor/code-editor", () => ({
  CodeEditorDialog: ({
    session,
    onClose,
  }: {
    session: { originalCode: string; code: { toString(): string }; readOnly: boolean };
    onClose: () => void;
  }) => (
    <section aria-label="Code comparison">
      <pre>{session.originalCode}</pre>
      <pre>{session.code.toString()}</pre>
      <output>{String(session.readOnly)}</output>
      <button onClick={onClose}>Close code diff</button>
    </section>
  ),
}));
afterEach(cleanup);
const operator = (id: string, code: string) => ({
  operatorID: id,
  operatorType: "PythonUDFV2",
  operatorVersion: "1",
  operatorProperties: { code },
  inputPorts: [],
  outputPorts: [],
  showAdvanced: false,
});
const content = (operators: ReturnType<typeof operator>[]) => ({
  operators,
  links: [],
  commentBoxes: [],
  operatorPositions: Object.fromEntries(operators.map((op, index) => [op.operatorID, { x: index, y: 0 }])),
  settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
});
describe("historical workflow comparison", () => {
  it("shows changed values and readonly code diff with legacy canvas highlights and no workflow writes", async () => {
    const before = content([operator("stay", "print('new')"), operator("deleted", "x")]),
      after = content([operator("stay", "print('old ü')"), operator("added", "y")]);
    const fetcher = vi.fn(async (path: string) =>
      Response.json({
        wid: 7,
        name: "Flow",
        readonly: path.includes("version"),
        content: JSON.stringify(path.includes("version") ? after : before),
      })
    );
    const highlight = vi.fn();
    const view = render(
      <VersionComparison
        api={new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null })}
        wid={7}
        versionId={2}
        onHighlight={highlight}
      />
    );
    await screen.findByRole("button", { name: "Compare operator stay" });
    await waitFor(() =>
      expect(highlight).toHaveBeenCalledWith(
        expect.objectContaining({ added: ["added"], modified: ["stay"], deleted: ["deleted"] })
      )
    );
    fireEvent.click(screen.getByRole("button", { name: "Compare operator stay" }));
    expect(screen.getByText("print('new')")).toBeTruthy();
    expect(screen.getByText("print('old ü')")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Open code diff" }));
    expect(screen.getByLabelText("Code comparison")).toBeTruthy();
    expect(screen.getByText("true")).toBeTruthy();
    expect(fetcher.mock.calls).toHaveLength(2);
    view.unmount();
    expect(highlight).toHaveBeenLastCalledWith(undefined);
  });
  it("shows malformed identifiers/backend errors without issuing mutations or retaining stale comparison highlights", async () => {
    const fetcher = vi.fn(async () => new Response("Forbidden", { status: 403 })),
      highlight = vi.fn();
    const view = render(
      <VersionComparison
        api={new ApiClient({ fetch: fetcher, getToken: () => null })}
        wid={7}
        versionId={2}
        onHighlight={highlight}
      />
    );
    await screen.findByText("Forbidden");
    expect(highlight.mock.calls.every(([value]) => value === undefined)).toBe(true);
    view.unmount();
    const invalid = vi.fn();
    render(
      <VersionComparison
        api={new ApiClient({ fetch: invalid })}
        wid={NaN}
        versionId={2}
      />
    );
    await screen.findByText("Invalid workflow or version identifier.");
    expect(invalid).not.toHaveBeenCalled();
  });
});
