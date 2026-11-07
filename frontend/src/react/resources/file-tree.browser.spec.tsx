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
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { page } from "vitest/browser";
import { ResourceFileTree, FILE_TREE_ROW_HEIGHT } from "./file-tree";
import type { StorageFileNode } from "./storage-api";
afterEach(cleanup);
const files = (count: number): StorageFileNode[] =>
  Array.from({ length: count }, (_, index) => ({
    name: `file_${String(index).padStart(4, "0")}.txt`,
    type: "file",
    parentDir: "/dataset/owner/name/v1",
    size: 1,
  }));
it("renders a bounded window of fixed-height rows for a thousand files and opens a file after scrolling", async () => {
  const open = vi.fn();
  render(
    <ResourceFileTree
      nodes={files(1000)}
      onOpen={open}
    />
  );
  const tree = screen.getByRole("tree");
  const rows = screen.getAllByRole("treeitem");
  expect(rows.length).toBeGreaterThan(0);
  expect(rows.length).toBeLessThan(100);
  expect(rows[0].getBoundingClientRect().height).toBe(FILE_TREE_ROW_HEIGHT);
  const button = rows[0].querySelector("button")!;
  const bounds = button.getBoundingClientRect();
  expect(bounds.height).toBeLessThanOrEqual(FILE_TREE_ROW_HEIGHT);
  expect(bounds.top).toBeGreaterThanOrEqual(rows[0].getBoundingClientRect().top);
  expect(bounds.bottom).toBeLessThanOrEqual(rows[0].getBoundingClientRect().bottom);
  await act(async () => {
    tree.scrollTop = 1000 * FILE_TREE_ROW_HEIGHT;
    fireEvent.scroll(tree);
  });
  await page.getByRole("button", { name: "Open file file_0999.txt" }).click();
  expect(open).toHaveBeenCalledWith(expect.objectContaining({ name: "file_0999.txt" }));
});
it("shows asynchronous files after an empty or hidden tree becomes visible", async () => {
  const view = render(
    <div style={{ display: "none" }}>
      <ResourceFileTree
        nodes={[]}
        onOpen={() => {}}
      />
    </div>
  );
  view.rerender(
    <div style={{ display: "none" }}>
      <ResourceFileTree
        nodes={files(1000)}
        onOpen={() => {}}
      />
    </div>
  );
  view.rerender(
    <div>
      <ResourceFileTree
        nodes={files(1000)}
        onOpen={() => {}}
      />
    </div>
  );
  await expect.element(page.getByRole("button", { name: "Open file file_0000.txt" })).toBeVisible();
  expect(screen.getAllByRole("treeitem").length).toBeLessThan(100);
});
it("expands and collapses nested folders with real pointer interaction without losing their files", async () => {
  const open = vi.fn();
  render(
    <ResourceFileTree
      nodes={[{ name: "folder", type: "directory", parentDir: "", children: files(2) }]}
      onOpen={open}
    />
  );
  await act(async () => {
    await page.getByRole("button", { name: "Collapse directory folder" }).click();
  });
  expect(screen.queryByRole("button", { name: "Open file file_0000.txt" })).toBeNull();
  await act(async () => {
    await page.getByRole("button", { name: "Expand directory folder" }).click();
  });
  await page.getByRole("button", { name: "Open file file_0001.txt" }).click();
  expect(open).toHaveBeenCalledOnce();
});
