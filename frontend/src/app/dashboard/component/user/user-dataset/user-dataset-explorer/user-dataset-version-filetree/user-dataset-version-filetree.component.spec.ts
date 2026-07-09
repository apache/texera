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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserDatasetVersionFiletreeComponent } from "./user-dataset-version-filetree.component";
import { DatasetFileNode } from "../../../../../../common/type/datasetVersionFileTree";

describe("UserDatasetVersionFiletreeComponent", () => {
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

  // Regression tests for the freeze on versions with hundreds of files: the
  // tree must virtualize instead of rendering one component per file.
  it("enables virtual scrolling with the 24px node height the row styles pin", () => {
    expect(component.fileTreeDisplayOptions.useVirtualScroll).toBe(true);
    // 24 is the single design value; the SCSS consumes it via --tree-node-height.
    expect(component.fileTreeDisplayOptions.nodeHeight).toBe(24);
  });

  it("keeps the full tree in the model without one DOM row per file", async () => {
    component.fileTreeNodes = makeFlatFileNodes(FILE_COUNT);

    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();

    expect(component.tree.treeModel.roots.length).toBe(FILE_COUNT);
    const renderedRows = fixture.nativeElement.querySelectorAll("tree-node").length;
    expect(renderedRows).toBeLessThan(FILE_COUNT / 5);
  });

  // The container hugs small trees and caps at 200px; heights follow the
  // tree's 26px row pitch plus a 2px leading drop slot.
  it("collapses the container when there are no files", () => {
    component.fileTreeNodes = [];
    fixture.detectChanges();

    const container = fixture.nativeElement.querySelector(".file-tree-container") as HTMLElement;
    expect(container.style.height).toBe("0px");
  });

  it("sizes the container to its content for small trees", () => {
    component.fileTreeNodes = makeFlatFileNodes(3);
    fixture.detectChanges();

    const container = fixture.nativeElement.querySelector(".file-tree-container") as HTMLElement;
    expect(container.style.height).toBe("80px"); // 3 rows x 26px pitch + 2px leading drop slot
  });

  it("caps the container height at 200px for large trees", () => {
    component.fileTreeNodes = makeFlatFileNodes(FILE_COUNT);
    fixture.detectChanges();

    const container = fixture.nativeElement.querySelector(".file-tree-container") as HTMLElement;
    expect(container.style.height).toBe("200px");
  });

  it("toggles expansion without emitting selectedTreeNode when a folder is clicked", () => {
    const emitted: DatasetFileNode[] = [];
    component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));

    // The folder branch delegates to TOGGLE_EXPANDED, which only calls
    // node.toggleExpanded().
    let toggleCalls = 0;
    const onClick = component.fileTreeDisplayOptions.actionMapping!.mouse!.click!;
    const folderNode = {
      hasChildren: true,
      toggleExpanded: () => {
        toggleCalls++;
      },
      data: { name: "dir", type: "directory", parentDir: "/owner/dataset/v1" },
    } as never;
    onClick(undefined as never, folderNode, undefined as never);

    expect(toggleCalls).toBe(1);
    expect(emitted).toEqual([]);
  });

  it("emits selectedTreeNode when a leaf node is clicked", () => {
    component.fileTreeNodes = makeFlatFileNodes(1);
    const emitted: DatasetFileNode[] = [];
    component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));

    // The handler only reads hasChildren and data; tree and $event are unused.
    const onClick = component.fileTreeDisplayOptions.actionMapping!.mouse!.click!;
    const leafNode = { hasChildren: false, data: component.fileTreeNodes[0] } as never;
    onClick(undefined as never, leafNode, undefined as never);

    expect(emitted).toEqual([component.fileTreeNodes[0]]);
  });

  it("emits deletedTreeNode when a node deletion is requested", () => {
    component.fileTreeNodes = makeFlatFileNodes(1);
    const emitted: DatasetFileNode[] = [];
    component.deletedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));

    component.onNodeDeleted(component.fileTreeNodes[0]);

    expect(emitted).toEqual([component.fileTreeNodes[0]]);
  });
});
