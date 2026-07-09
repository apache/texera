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
      type: "file" as const,
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

  // Regression tests for the frontend freeze when a dataset version has
  // hundreds of files: the tree must virtualize its rows instead of creating
  // one component per file.
  it("enables virtual scrolling with a fixed node height", () => {
    expect(component.fileTreeDisplayOptions.useVirtualScroll).toBe(true);
    expect(typeof component.fileTreeDisplayOptions.nodeHeight).toBe("number");
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

  it("emits selectedTreeNode when a leaf node is clicked", () => {
    component.fileTreeNodes = makeFlatFileNodes(1);
    const emitted: DatasetFileNode[] = [];
    component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));

    // Invoke the tree's real click handler with a stub leaf node; the handler
    // only reads hasChildren and data, so tree and $event are unused.
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
