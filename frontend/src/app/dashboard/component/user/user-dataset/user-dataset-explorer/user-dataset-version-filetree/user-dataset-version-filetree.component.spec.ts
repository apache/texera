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
import { KEYS } from "@ali-hm/angular-tree-component";
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

  // Clicks go through the library's own mouseAction dispatch on real tree nodes, so what these
  // pin is the state a row's node-content-wrapper-active class is bound to -- not only that the
  // handler was reached.
  describe("clicking a node", () => {
    function renderAndClick(nodes: DatasetFileNode[], rootIndex: number): any {
      component.fileTreeNodes = nodes;
      fixture.detectChanges();
      const node = component.tree.treeModel.roots[rootIndex];
      node.mouseAction("click", new MouseEvent("click"));
      return node;
    }

    const activeNames = (): string[] =>
      component.tree.treeModel.activeNodes.map((node: any) => node.data.name as string);

    it("emits selectedTreeNode and makes the file the active node", () => {
      const emitted: DatasetFileNode[] = [];
      component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));

      const files = makeFlatFileNodes(3);
      const node = renderAndClick(files, 1);

      expect(emitted).toEqual([files[1]]);
      expect(node.isActive).toBe(true);
      expect(activeNames()).toEqual([files[1].name]);
    });

    // ACTIVATE, not TOGGLE_ACTIVE: a second click on the file the user already picked must not
    // strip the highlight off a selection that still stands.
    it("keeps the file active when the same file is clicked again, and emits it again", () => {
      const emitted: DatasetFileNode[] = [];
      component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));
      const files = makeFlatFileNodes(3);
      const node = renderAndClick(files, 1);

      node.mouseAction("click", new MouseEvent("click"));

      expect(node.isActive).toBe(true);
      expect(activeNames()).toEqual([files[1].name]);
      expect(emitted).toEqual([files[1], files[1]]);
    });

    // The library maps Enter and Space to TOGGLE_ACTIVE by default: Enter on the picked file would
    // strip its highlight while the host's selection stands, and on another row would mark a file
    // the host never heard of. Both keys now do what a click does.
    it("activates and emits on Enter or Space, as on a click, without toggling the pick off", () => {
      const emitted: DatasetFileNode[] = [];
      component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));
      const files = makeFlatFileNodes(3);
      component.fileTreeNodes = files;
      fixture.detectChanges();
      const keys = component.fileTreeDisplayOptions.actionMapping!.keys!;
      const node = component.tree.treeModel.roots[1];

      keys[KEYS.ENTER](component.tree.treeModel, node, new KeyboardEvent("keydown"));
      expect(node.isActive).toBe(true);
      keys[KEYS.SPACE](component.tree.treeModel, node, new KeyboardEvent("keydown"));

      expect(node.isActive).toBe(true);
      expect(activeNames()).toEqual([files[1].name]);
      expect(emitted).toEqual([files[1], files[1]]);
    });

    // A host that browses a version's files without being able to hold one (the dataset picker in
    // version mode, the model picker) gets no trace of a click: a blue row there would promise a
    // pick nothing takes. Folders still open.
    it("neither activates nor emits a file where files cannot be picked, and still expands a folder", () => {
      const emitted: DatasetFileNode[] = [];
      component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));
      component.isFileSelectable = false;

      const files = makeFlatFileNodes(3);
      const node = renderAndClick(files, 1);
      expect(emitted).toEqual([]);
      // Never activated at all, so the library leaves the flag unset rather than false.
      expect(node.isActive).toBeFalsy();
      expect(activeNames()).toEqual([]);

      const folder = renderAndClick([makeFolderNode(2)], 0);
      expect(folder.isExpanded).toBe(true);
    });

    // Every host that never binds the input (the dataset and model pages, the file selector) keeps
    // the click it has today.
    it("picks files by default, for a host that never binds the input", () => {
      expect(component.isFileSelectable).toBe(true);
    });

    it("moves the highlight to the newly clicked file", () => {
      const files = makeFlatFileNodes(3);
      renderAndClick(files, 1);

      component.tree.treeModel.roots[2].mouseAction("click", new MouseEvent("click"));

      expect(activeNames()).toEqual([files[2].name]);
    });

    it("toggles a folder's expansion, activating nothing and emitting nothing", () => {
      const emitted: DatasetFileNode[] = [];
      component.selectedTreeNode.subscribe((n: DatasetFileNode) => emitted.push(n));

      const folder = renderAndClick([makeFolderNode(2)], 0);
      expect(folder.isExpanded).toBe(true);

      folder.mouseAction("click", new MouseEvent("click"));
      expect(folder.isExpanded).toBe(false);

      expect(activeNames()).toEqual([]);
      expect(emitted).toEqual([]);
    });
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
      parentDir: "/dataset/owner/dataset/v1",
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

    it("withholds Set-as-cover unless the host allows it", () => {
      renderRows([file("photo.png")]);

      expect(fixture.nativeElement.querySelector("i[nztype='picture']")).toBeNull();
    });

    it("offers Set-as-cover on image files only", () => {
      const covers: string[] = [];
      component.isCoverSettable = true;
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

    // parentDir has exactly the four stripped segments (dataset/owner/dataset/version),
    // so the relative path is just the file name.
    component.onSetCover({ name: "photo.png", type: "file", parentDir: "/dataset/owner/dataset/v1" });

    expect(emitted).toEqual(["photo.png"]);
  });
});
