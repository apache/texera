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

// Browser-mode companion to user-dataset-version-filetree.component.spec.ts.
// jsdom performs no layout — getBoundingClientRect() is all zeros there — so
// the tree's virtual scroll measures a 0-height viewport and renders zero
// rows, and the jsdom spec can only assert an upper bound on rendered rows.
// These tests run in vitest's Playwright/Chromium browser mode, where the
// container really lays out, to pin the behaviors that need real geometry:
// virtualization renders a non-empty window of rows, rows stay exactly
// TREE_NODE_HEIGHT_PX tall, per-row action buttons fit inside the row, and
// the container hugs small trees.

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserDatasetVersionFiletreeComponent } from "./user-dataset-version-filetree.component";
import { DatasetFileNode } from "../../../../../../common/type/datasetVersionFileTree";

describe("UserDatasetVersionFiletreeComponent (browser)", () => {
  let fixture: ComponentFixture<UserDatasetVersionFiletreeComponent>;
  let component: UserDatasetVersionFiletreeComponent;

  const FILE_COUNT = 1000;
  function makeFlatFileNodes(count: number): DatasetFileNode[] {
    return Array.from({ length: count }, (_, i) => ({
      name: `file_${String(i + 1).padStart(4, "0")}.txt`,
      type: "file",
      parentDir: "/owner/dataset/v1",
      size: 1,
    }));
  }

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [UserDatasetVersionFiletreeComponent],
    });
    fixture = TestBed.createComponent(UserDatasetVersionFiletreeComponent);
    component = fixture.componentInstance;
  });

  // whenStable flushes the tree-viewport's setTimeout that measures the
  // viewport height; the second detectChanges renders the measured window.
  // The input is assigned inside the Angular zone, as a host template binding
  // would be, so whenStable also waits for the re-measure timer the
  // fileTreeNodes setter schedules.
  async function renderTree(nodes: DatasetFileNode[]): Promise<void> {
    if (fixture.ngZone) {
      fixture.ngZone.run(() => (component.fileTreeNodes = nodes));
    } else {
      component.fileTreeNodes = nodes;
    }
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
  }

  it("renders a non-empty virtualized window of rows for a large tree", async () => {
    await renderTree(makeFlatFileNodes(FILE_COUNT));

    const renderedRows = fixture.nativeElement.querySelectorAll("tree-node").length;
    expect(renderedRows).toBeGreaterThan(0);
    expect(renderedRows).toBeLessThan(FILE_COUNT / 5);
  });

  it("renders rows at exactly the virtual-scroll node height", async () => {
    await renderTree(makeFlatFileNodes(FILE_COUNT));

    const row = fixture.nativeElement.querySelector(".node-content-wrapper") as HTMLElement;
    expect(row).not.toBeNull();
    expect(row.getBoundingClientRect().height).toBe(component.TREE_NODE_HEIGHT_PX);
  });

  it("keeps row action buttons inside the fixed-height row", async () => {
    component.isTreeNodeDeletable = true;
    await renderTree(makeFlatFileNodes(5));

    const row = fixture.nativeElement.querySelector(".node-content-wrapper") as HTMLElement;
    const button = row.querySelector("button.icon-button") as HTMLElement;
    expect(button).not.toBeNull();

    const rowRect = row.getBoundingClientRect();
    const buttonRect = button.getBoundingClientRect();
    expect(buttonRect.height).toBeLessThanOrEqual(rowRect.height);
    expect(buttonRect.top).toBeGreaterThanOrEqual(rowRect.top);
    expect(buttonRect.bottom).toBeLessThanOrEqual(rowRect.bottom);
  });

  // The tree measures its viewport height once after init (and again only on
  // scroll). Both hosts create this component with an empty tree and fill it
  // when data arrives, so a container sized to content must trigger a
  // re-measure when its height changes — otherwise the 0px initial
  // measurement sticks and the tree stays blank forever.
  it("renders rows when files arrive after an initially-empty tree", async () => {
    await renderTree([]);
    await renderTree(makeFlatFileNodes(FILE_COUNT));

    const renderedRows = fixture.nativeElement.querySelectorAll("tree-node").length;
    expect(renderedRows).toBeGreaterThan(0);
  });

  it("lays out the container at content height for small trees", async () => {
    await renderTree(makeFlatFileNodes(3));

    const container = fixture.nativeElement.querySelector(".file-tree-container") as HTMLElement;
    // 3 rows x 26px pitch + 2px leading drop slot.
    expect(container.getBoundingClientRect().height).toBe(80);
  });
});
