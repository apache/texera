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

import {
  DatasetFileNode,
  DatasetVersionFileTreeManager,
  getFullPathFromDatasetFileNode,
  getPathsUnderOrEqualDatasetFileNode,
  getRelativePathFromDatasetFileNode,
} from "./datasetVersionFileTree";

describe("getFullPathFromDatasetFileNode", () => {
  it("joins parentDir and name with a slash", () => {
    const node: DatasetFileNode = { name: "c.txt", type: "file", parentDir: "/a/b" };
    expect(getFullPathFromDatasetFileNode(node)).toBe("/a/b/c.txt");
  });

  it("produces a leading slash for a node whose parentDir is empty", () => {
    const node: DatasetFileNode = { name: "root", type: "directory", parentDir: "" };
    expect(getFullPathFromDatasetFileNode(node)).toBe("/root");
  });
});

describe("getRelativePathFromDatasetFileNode", () => {
  it("strips the first three path segments", () => {
    const node: DatasetFileNode = { name: "file.csv", type: "file", parentDir: "/owner/dataset/v1" };
    // full path is /owner/dataset/v1/file.csv -> segments [owner, dataset, v1, file.csv]
    expect(getRelativePathFromDatasetFileNode(node)).toBe("file.csv");
  });

  it("preserves nested relative segments beyond the first three", () => {
    const node: DatasetFileNode = { name: "f.txt", type: "file", parentDir: "/owner/dataset/v1/sub/dir" };
    expect(getRelativePathFromDatasetFileNode(node)).toBe("sub/dir/f.txt");
  });

  it("returns an empty string when there are three or fewer segments", () => {
    const node: DatasetFileNode = { name: "v1", type: "directory", parentDir: "/owner/dataset" };
    // full path /owner/dataset/v1 -> exactly 3 segments -> no relative path
    expect(getRelativePathFromDatasetFileNode(node)).toBe("");
  });

  it("ignores empty segments from duplicate slashes when counting", () => {
    const node: DatasetFileNode = { name: "file.csv", type: "file", parentDir: "/owner//dataset/v1" };
    // empty segment between the duplicate slashes is filtered out, leaving 4 real segments
    expect(getRelativePathFromDatasetFileNode(node)).toBe("file.csv");
  });
});

describe("getPathsUnderOrEqualDatasetFileNode", () => {
  it("returns the single path for a file node", () => {
    const file: DatasetFileNode = { name: "a.txt", type: "file", parentDir: "/dir" };
    expect(getPathsUnderOrEqualDatasetFileNode(file)).toEqual(["/dir/a.txt"]);
  });

  it("collects every file path under a directory", () => {
    const file1: DatasetFileNode = { name: "file1.txt", type: "file", parentDir: "/dir" };
    const file2: DatasetFileNode = { name: "file2.txt", type: "file", parentDir: "/dir" };
    const dir: DatasetFileNode = { name: "dir", type: "directory", parentDir: "", children: [file1, file2] };
    expect(getPathsUnderOrEqualDatasetFileNode(dir)).toEqual(["/dir/file1.txt", "/dir/file2.txt"]);
  });

  it("recurses into nested directories", () => {
    const deepFile: DatasetFileNode = { name: "deep.txt", type: "file", parentDir: "/a/b" };
    const subDir: DatasetFileNode = { name: "b", type: "directory", parentDir: "/a", children: [deepFile] };
    const topDir: DatasetFileNode = { name: "a", type: "directory", parentDir: "", children: [subDir] };
    expect(getPathsUnderOrEqualDatasetFileNode(topDir)).toEqual(["/a/b/deep.txt"]);
  });

  it("returns an empty array for an empty directory", () => {
    const emptyChildren: DatasetFileNode = { name: "dir", type: "directory", parentDir: "", children: [] };
    const noChildrenProp: DatasetFileNode = { name: "dir", type: "directory", parentDir: "" };
    expect(getPathsUnderOrEqualDatasetFileNode(emptyChildren)).toEqual([]);
    expect(getPathsUnderOrEqualDatasetFileNode(noChildrenProp)).toEqual([]);
  });
});

describe("DatasetVersionFileTreeManager", () => {
  describe("addNodeWithPath", () => {
    it("starts with no root nodes", () => {
      const manager = new DatasetVersionFileTreeManager();
      expect(manager.getRootNodes()).toEqual([]);
    });

    it("builds the intermediate directory structure and returns the leaf file node", () => {
      const manager = new DatasetVersionFileTreeManager();
      const leaf = manager.addNodeWithPath("/a/b/c.txt");

      expect(leaf.name).toBe("c.txt");
      expect(leaf.type).toBe("file");
      expect(getFullPathFromDatasetFileNode(leaf)).toBe("/a/b/c.txt");

      const roots = manager.getRootNodes();
      expect(roots.length).toBe(1);
      expect(roots[0].name).toBe("a");
      expect(roots[0].type).toBe("directory");

      const dirB = roots[0].children![0];
      expect(dirB.name).toBe("b");
      expect(dirB.type).toBe("directory");
      expect(dirB.children![0]).toBe(leaf);
    });

    it("is idempotent when adding the same path twice", () => {
      const manager = new DatasetVersionFileTreeManager();
      const first = manager.addNodeWithPath("/a/b/c.txt");
      const second = manager.addNodeWithPath("/a/b/c.txt");

      expect(second).toBe(first);
      expect(manager.getRootNodes().length).toBe(1);
      const dirB = manager.getRootNodes()[0].children![0];
      expect(dirB.children!.length).toBe(1);
    });

    it("adds siblings under an existing directory", () => {
      const manager = new DatasetVersionFileTreeManager();
      manager.addNodeWithPath("/a/b/c.txt");
      manager.addNodeWithPath("/a/b/d.txt");

      const dirB = manager.getRootNodes()[0].children![0];
      expect(dirB.children!.map(child => child.name).sort()).toEqual(["c.txt", "d.txt"]);
    });

    it("handles paths without a leading slash", () => {
      const manager = new DatasetVersionFileTreeManager();
      const leaf = manager.addNodeWithPath("x/y.txt");
      expect(getFullPathFromDatasetFileNode(leaf)).toBe("/x/y.txt");
      expect(manager.getRootNodes()[0].name).toBe("x");
    });
  });

  describe("initializeWithRootNodes / constructor", () => {
    it("exposes provided root nodes", () => {
      const dir: DatasetFileNode = {
        name: "dir",
        type: "directory",
        parentDir: "/",
        children: [{ name: "f.txt", type: "file", parentDir: "/dir" }],
      };
      const manager = new DatasetVersionFileTreeManager([dir]);
      expect(manager.getRootNodes()).toEqual([dir]);
    });
  });

  describe("removeNode", () => {
    it("removes a leaf node found by identity via BFS", () => {
      const manager = new DatasetVersionFileTreeManager();
      const leaf = manager.addNodeWithPath("/a/b/c.txt");
      const dirB = manager.getRootNodes()[0].children![0];

      manager.removeNode(leaf);
      expect(dirB.children).toEqual([]);
    });

    it("removes a whole subtree when removing an inner directory", () => {
      const manager = new DatasetVersionFileTreeManager();
      manager.addNodeWithPath("/a/b/c.txt");
      const rootA = manager.getRootNodes()[0];

      manager.removeNode(rootA);
      expect(manager.getRootNodes()).toEqual([]);
    });

    it("does nothing for a node that is not present in the tree", () => {
      const manager = new DatasetVersionFileTreeManager();
      manager.addNodeWithPath("/a/b/c.txt");
      const stranger: DatasetFileNode = { name: "z.txt", type: "file", parentDir: "/q" };

      manager.removeNode(stranger);
      expect(manager.getRootNodes().length).toBe(1);
      expect(manager.getRootNodes()[0].children![0].children!.length).toBe(1);
    });

    it("refuses to remove the synthetic root node", () => {
      const manager = new DatasetVersionFileTreeManager();
      manager.addNodeWithPath("/a/b/c.txt");
      const fakeRoot: DatasetFileNode = { name: "/", type: "directory", parentDir: "" };

      manager.removeNode(fakeRoot);
      expect(manager.getRootNodes().length).toBe(1);
    });
  });

  describe("removeNodeWithPath", () => {
    it("removes a node from its parent's children and the internal map", () => {
      const file1: DatasetFileNode = { name: "file1.txt", type: "file", parentDir: "/dir" };
      const file2: DatasetFileNode = { name: "file2.txt", type: "file", parentDir: "/dir" };
      const dir: DatasetFileNode = { name: "dir", type: "directory", parentDir: "/", children: [file1, file2] };
      const manager = new DatasetVersionFileTreeManager([dir]);

      manager.removeNodeWithPath("/dir/file1.txt");
      expect(dir.children!.map(child => child.name)).toEqual(["file2.txt"]);

      // A second removal of the same (now absent) path is a no-op.
      manager.removeNodeWithPath("/dir/file1.txt");
      expect(dir.children!.map(child => child.name)).toEqual(["file2.txt"]);
    });

    it("removes a whole subtree when removing a directory path", () => {
      const file: DatasetFileNode = { name: "f.txt", type: "file", parentDir: "/dir/sub" };
      const subDir: DatasetFileNode = { name: "sub", type: "directory", parentDir: "/dir", children: [file] };
      const dir: DatasetFileNode = { name: "dir", type: "directory", parentDir: "/", children: [subDir] };
      const manager = new DatasetVersionFileTreeManager([dir]);

      manager.removeNodeWithPath("/dir/sub");
      expect(dir.children).toEqual([]);

      // Removing a descendant path after the subtree is gone is also a no-op.
      manager.removeNodeWithPath("/dir/sub/f.txt");
      expect(dir.children).toEqual([]);
    });

    it("does nothing for an unknown path", () => {
      const dir: DatasetFileNode = {
        name: "dir",
        type: "directory",
        parentDir: "/",
        children: [{ name: "f.txt", type: "file", parentDir: "/dir" }],
      };
      const manager = new DatasetVersionFileTreeManager([dir]);

      manager.removeNodeWithPath("/does/not/exist");
      expect(dir.children!.length).toBe(1);
    });
  });
});
