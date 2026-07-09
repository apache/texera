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
// jsdom does no layout — getBoundingClientRect() is all zeros — so the
// virtual scroll measures a 0-height viewport and renders zero rows there.
// These tests run in vitest's Playwright/Chromium browser mode to pin the
// behaviors that need real geometry.

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserDatasetVersionFiletreeComponent } from "./user-dataset-version-filetree.component";
import { DatasetFileNode } from "../../../../../../common/type/datasetVersionFileTree";
import { FILE_COUNT, makeFlatFileNodes } from "./user-dataset-version-filetree.test-utils";

describe("UserDatasetVersionFiletreeComponent (browser)", () => {
  let fixture: ComponentFixture<UserDatasetVersionFiletreeComponent>;
  let component: UserDatasetVersionFiletreeComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [UserDatasetVersionFiletreeComponent],
    });
    fixture = TestBed.createComponent(UserDatasetVersionFiletreeComponent);
    component = fixture.componentInstance;
  });

  // Assign inside the Angular zone, as a host binding would, so whenStable
  // waits for the viewport-measure timers; the second detectChanges renders
  // the measured window.
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

  // Both hosts create the tree empty and fill it when data arrives; the
  // height change must trigger a viewport re-measure or the 0px initial
  // measurement sticks and the tree stays blank.
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
