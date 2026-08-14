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
import { FILE_COUNT, makeFlatFileNodes } from "./user-dataset-version-filetree.test-utils";

describe("UserDatasetVersionFiletreeComponent", () => {
  let fixture: ComponentFixture<UserDatasetVersionFiletreeComponent>;
  let component: UserDatasetVersionFiletreeComponent;

  function makeFolderNode(fileCount: number): DatasetFileNode {
    return {
      name: "dir",
      type: "directory",
      parentDir: "/owner/dataset/v1",
      children: makeFlatFileNodes(fileCount),
    };
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

  it("counts nested folder contents when sizing the container", () => {
    component.fileTreeNodes = [makeFolderNode(2)]; // 1 folder + 2 files = 3 rows when expanded
    fixture.detectChanges();

    const container = fixture.nativeElement.querySelector(".file-tree-container") as HTMLElement;
    expect(container.style.height).toBe("80px");
  });

  it("treats a missing files input as an empty tree", () => {
    component.fileTreeNodes = undefined as unknown as DatasetFileNode[];
    fixture.detectChanges();

    expect(component.fileTreeNodes).toEqual([]);
    const container = fixture.nativeElement.querySelector(".file-tree-container") as HTMLElement;
    expect(container.style.height).toBe("0px");
  });

  it("expands all folders after view init when isExpandAllAfterViewInit is set", () => {
    component.isExpandAllAfterViewInit = true;
    component.fileTreeNodes = [makeFolderNode(2)];
    fixture.detectChanges();

    expect(component.tree.treeModel.roots[0].isExpanded).toBe(true);
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

  it("identifies image files by extension, case-insensitively", () => {
    expect(component.isImageFile("photo.PNG")).toBe(true);
    expect(component.isImageFile("data.csv")).toBe(false);
  });

  /**
   * The node template decides what a row looks like: which icon marks a folder against a
   * file, and which of the two row actions a node is entitled to. Rendering it needs rows,
   * and with `useVirtualScroll` on there are none under jsdom — the viewport measures 0
   * high, as the browser-mode companion spec records. Browser mode is not an option here
   * either: CI uploads coverage from the jsdom run only (`test:ci` -> lcov) and takes just
   * the JUnit file from `gui:test-browser`. So these switch virtualization off, which
   * changes how many rows the tree draws but not what a row contains.
   */
  describe("node template", () => {
    const folderWith = (children: DatasetFileNode[]): DatasetFileNode => ({
      name: "dir",
      type: "directory",
      parentDir: "/owner/dataset/v1",
      children,
    });

    const file = (name: string): DatasetFileNode => ({
      name,
      type: "file",
      parentDir: "/datasets/owner/dataset/v1",
    });

    // Deliberately synchronous: the tree keeps a pending timer, so awaiting whenStable()
    // never settles here. Two change-detection passes are what the rows need — the first
    // builds the tree model, the second draws the expanded nodes.
    function renderRows(nodes: DatasetFileNode[]): void {
      component.fileTreeDisplayOptions = { ...component.fileTreeDisplayOptions, useVirtualScroll: false };
      // Folders start collapsed, so their children would not be drawn otherwise.
      component.isExpandAllAfterViewInit = true;
      component.fileTreeNodes = nodes;
      fixture.detectChanges();
      fixture.detectChanges();
    }

    const rowTitles = (): string[] =>
      Array.from(fixture.nativeElement.querySelectorAll("span[title]")).map(span =>
        (span as HTMLElement).getAttribute("title")
      ) as string[];

    const rowFor = (title: string): HTMLElement =>
      fixture.nativeElement.querySelector(`span[title="${title}"]`) as HTMLElement;

    it("marks folders and files with their own icon, and labels every row", () => {
      renderRows([folderWith([file("a.csv")])]);

      expect(rowTitles()).toEqual(["dir", "a.csv"]);
      expect(rowFor("dir").textContent?.trim()).toBe("dir");
      expect(rowFor("dir").querySelector("i[nztype='folder']")).not.toBeNull();
      expect(rowFor("dir").querySelector("i[nztype='file']")).toBeNull();
      expect(rowFor("a.csv").querySelector("i[nztype='file']")).not.toBeNull();
      expect(rowFor("a.csv").querySelector("i[nztype='folder']")).toBeNull();
    });

    it("offers deletion on files only, and never on the folders holding them", () => {
      const deleted: DatasetFileNode[] = [];
      component.deletedTreeNode.subscribe((n: DatasetFileNode) => deleted.push(n));
      component.isTreeNodeDeletable = true;

      renderRows([folderWith([file("a.csv")])]);

      expect(rowFor("dir").querySelector("i[nztype='delete']")).toBeNull();
      const deleteButton = rowFor("a.csv").querySelector("i[nztype='delete']")?.closest("button");
      expect(deleteButton).not.toBeNull();

      deleteButton!.click();

      expect(deleted.map(node => node.name)).toEqual(["a.csv"]);
    });

    it("withholds deletion entirely when the tree is not deletable", () => {
      component.isTreeNodeDeletable = false;

      renderRows([folderWith([file("a.csv")])]);

      expect(fixture.nativeElement.querySelector("i[nztype='delete']")).toBeNull();
    });

    it("offers Set-as-cover on image files only", () => {
      const covers: string[] = [];
      component.setCoverImage.subscribe((path: string) => covers.push(path));

      renderRows([file("photo.png"), file("data.csv")]);

      expect(rowFor("data.csv").querySelector("i[nztype='picture']")).toBeNull();
      const coverButton = rowFor("photo.png").querySelector("i[nztype='picture']")?.closest("button");
      expect(coverButton).not.toBeNull();

      coverButton!.click();

      // parentDir strips to the dataset-relative path, which for a root file is its name.
      expect(covers).toEqual(["photo.png"]);
    });
  });

  it("emits the file's dataset-relative path when set as cover", () => {
    const emitted: string[] = [];
    component.setCoverImage.subscribe((path: string) => emitted.push(path));

    // parentDir has exactly the four stripped segments (datasets/owner/dataset/version),
    // so the relative path is just the file name.
    component.onSetCover({ name: "photo.png", type: "file", parentDir: "/datasets/owner/dataset/v1" });

    expect(emitted).toEqual(["photo.png"]);
  });
});
