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
import { MarkdownEditor, RichText } from "./rich-text";
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
describe("RichText", () => {
  it("renders markdown formatting, tables, code and safe links rather than literal markdown", () => {
    const { container } = render(
      <RichText
        content={
          "# Results\n\n**Strong** and _emphasis_\n\n| Name | Value |\n| --- | --- |\n| A | 1 |\n\n`code`\n\n[Docs](https://texera.apache.org)"
        }
      />
    );
    expect(screen.getByRole("heading", { name: "Results" })).toBeTruthy();
    expect(container.querySelector("strong")?.textContent).toBe("Strong");
    expect(container.querySelector("em")?.textContent).toBe("emphasis");
    expect(screen.getByRole("table")).toBeTruthy();
    expect(screen.getByRole("link", { name: "Docs" }).getAttribute("href")).toBe("https://texera.apache.org");
  });
  it("sanitizes provider/user HTML while keeping safe structural content and unicode", () => {
    const { container } = render(
      <RichText
        format="html"
        content={
          '<h2>日本語</h2><p style="position:fixed" onclick="alert(1)">Text<img src="x" onerror="alert(1)"></p><script>alert(1)</script><iframe src="https://evil.example"></iframe><a href="javascript:alert(1)">Bad</a>'
        }
      />
    );
    expect(screen.getByRole("heading", { name: "日本語" })).toBeTruthy();
    expect(container.querySelector("script, iframe, [onclick], [onerror], [style]")).toBeNull();
    expect(screen.getByText("Bad").getAttribute("href")).toBeNull();
  });
  it("removes unsafe markdown HTML and updates to empty content without leaving stale markup", () => {
    const { container, rerender } = render(
      <RichText content={'<svg><a xlink:href="javascript:alert(1)">x</a></svg>\n\n<img src="javascript:alert(1)">'} />
    );
    expect(container.querySelector("svg")).toBeNull();
    expect(container.querySelector("img")?.hasAttribute("src")).toBe(false);
    rerender(<RichText content={undefined} />);
    expect(container.textContent).toBe("");
  });
  it("expands and collapses overflowing descriptions", () => {
    vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockReturnValue(700);
    render(
      <RichText
        content="Long description"
        collapsedHeight={320}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "View more" }));
    expect(screen.getByRole("button", { name: "View less" })).toBeTruthy();
  });
});
describe("MarkdownEditor", () => {
  it("preserves edits across incoming value changes until save and applies toolbar formatting", async () => {
    const save = vi.fn();
    const { rerender } = render(
      <MarkdownEditor
        value="Original"
        onChange={save}
        label="Description"
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Edit Description" }));
    const input = screen.getByLabelText("Description") as HTMLTextAreaElement;
    fireEvent.change(input, { target: { value: "Draft" } });
    input.setSelectionRange(0, 5);
    fireEvent.click(screen.getByRole("button", { name: "Bold" }));
    rerender(
      <MarkdownEditor
        value="Remote edit"
        onChange={save}
        label="Description"
      />
    );
    expect((screen.getByLabelText("Description") as HTMLTextAreaElement).value).toBe("**Draft**");
    fireEvent.click(screen.getByRole("button", { name: "Save Description" }));
    expect(save).toHaveBeenCalledWith("**Draft**");
    await waitFor(() => expect(screen.queryByRole("button", { name: "Save Description" })).toBeNull());
  });
  it("cancels edits without publishing them", () => {
    const save = vi.fn();
    render(
      <MarkdownEditor
        value="Original"
        onChange={save}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Edit Description" }));
    fireEvent.change(screen.getByLabelText("Description"), { target: { value: "Discard me" } });
    fireEvent.click(screen.getByRole("button", { name: "Cancel editing" }));
    expect(save).not.toHaveBeenCalled();
    expect(screen.getByText("Original")).toBeTruthy();
  });
});
