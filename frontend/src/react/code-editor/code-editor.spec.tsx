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

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";
import { CodeareaField } from "./code-editor";

const { mounting } = vi.hoisted(() => ({ mounting: vi.fn() }));
vi.mock("./runtime", async importOriginal => ({ ...(await importOriginal()), mountCodeEditor: mounting }));
afterEach(() => {
  cleanup();
  mounting.mockReset();
  localStorage.clear();
});

function setup() {
  const doc = new Y.Doc();
  const code = doc.getText("code");
  code.insert(0, "print(1)");
  const awareness = new Awareness(doc);
  const mounted = {
    editor: undefined,
    updateReadOnly: vi.fn(),
    layout: vi.fn(),
    undo: vi.fn(),
    redo: vi.fn(),
    dispose: vi.fn().mockResolvedValue(undefined),
  };
  mounting.mockResolvedValue(mounted);
  const props = {
    schema: { type: "string" as const, title: "Code" },
    value: "print(1)",
    path: ["code"],
    disabled: false,
    required: true,
    issues: [],
    onChange: vi.fn(),
  };
  const session = { operatorId: "op-1", code, awareness, language: "python" as const, title: "My UDF" };
  return { doc, awareness, mounted, props, session };
}

describe("React Monaco code editor", () => {
  it("opens the real runtime surface, publishes awareness and closes with disposal", async () => {
    const test = setup();
    render(
      <CodeareaField
        {...test.props}
        session={test.session}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Edit code" }));
    await screen.findByRole("dialog", { name: "Python UDF: My UDF" });
    await waitFor(() => expect(mounting).toHaveBeenCalledOnce());
    expect(test.awareness.getLocalState()?.editingCode).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Undo code edit" }));
    expect(test.mounted.undo).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole("button", { name: "Close code editor" }));
    await waitFor(() => expect(test.mounted.dispose).toHaveBeenCalledOnce());
    expect(test.awareness.getLocalState()?.editingCode).toBe(false);
    cleanup();
    test.awareness.destroy();
    test.doc.destroy();
  });

  it("updates interactivity without remounting and shows initialization errors without losing code", async () => {
    const test = setup();
    const { rerender } = render(
      <CodeareaField
        {...test.props}
        session={test.session}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Edit code" }));
    await waitFor(() => expect(test.mounted.updateReadOnly).toHaveBeenCalled());
    rerender(
      <CodeareaField
        {...test.props}
        disabled
        session={test.session}
      />
    );
    expect(test.mounted.updateReadOnly).toHaveBeenLastCalledWith(true);
    expect(mounting).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole("button", { name: "Close code editor" }));
    mounting.mockRejectedValueOnce(new Error("Worker failed"));
    fireEvent.click(screen.getByRole("button", { name: "Edit code" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Worker failed"));
    expect(test.session.code.toString()).toBe("print(1)");
    cleanup();
    test.awareness.destroy();
    test.doc.destroy();
  });

  it("cancels initialization and disposes a late runtime result when closed immediately", async () => {
    const test = setup();
    let ready!: (value: typeof test.mounted) => void;
    mounting.mockImplementation(
      () =>
        new Promise(resolve => {
          ready = resolve;
        })
    );
    render(
      <CodeareaField
        {...test.props}
        session={test.session}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Edit code" }));
    fireEvent.click(screen.getByRole("button", { name: "Close code editor" }));
    expect(mounting.mock.calls[0][0].signal.aborted).toBe(true);
    await act(async () => ready(test.mounted));
    expect(test.mounted.dispose).toHaveBeenCalledOnce();
    cleanup();
    test.awareness.destroy();
    test.doc.destroy();
  });
});
