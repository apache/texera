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
import { WorkflowListPage } from "./workflow-list-page";
import { ApiClient } from "../core/api-client";
vi.mock("./notebook-generation", () => ({
  NotebookGenerationDialog: ({ onCreated }: { onCreated: (wid: number) => void }) => (
    <button onClick={() => onCreated(9)}>Finish generated workflow</button>
  ),
}));
afterEach(cleanup);
it("opens a generated notebook workflow on the canvas with automatic layout requested", async () => {
  const api = new ApiClient({
    fetch: vi.fn(async () => Response.json({ results: [], more: false })),
    getToken: () => null,
  });
  const onOpen = vi.fn();
  render(
    <WorkflowListPage
      api={api}
      user={{ uid: 7, name: "User", email: "user@example.org", role: "REGULAR", expiresAt: 999999999999 }}
      config={{ pythonNotebookMigrationEnabled: true }}
      onOpen={onOpen}
    />
  );
  await screen.findByText("No workflows match your search.");
  fireEvent.click(screen.getByRole("button", { name: "Generate from Python" }));
  fireEvent.click(screen.getByRole("button", { name: "Finish generated workflow" }));
  await waitFor(() => expect(onOpen).toHaveBeenCalledWith(9, "CANVAS", true));
});
