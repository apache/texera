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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  DatasetFileNode,
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
  it("strips the datasets/owner/dataset/version prefix (4 segments)", () => {
    const node: DatasetFileNode = {
      name: "tw1.csv",
      type: "file",
      parentDir: "/dataset/bob@texera.com/twitterDataset/v1/california/irvine",
    };
    expect(getRelativePathFromDatasetFileNode(node)).toBe("california/irvine/tw1.csv");
  });

  it("returns the bare file name for a file at the version root", () => {
    const node: DatasetFileNode = {
      name: "readme.txt",
      type: "file",
      parentDir: "/dataset/bob@texera.com/twitterDataset/v1",
    };
    expect(getRelativePathFromDatasetFileNode(node)).toBe("readme.txt");
  });

  it("returns an empty string when there is no path below the version", () => {
    const node: DatasetFileNode = {
      name: "v1",
      type: "directory",
      parentDir: "/dataset/bob@texera.com/twitterDataset",
    };
    expect(getRelativePathFromDatasetFileNode(node)).toBe("");
  });

  it("ignores empty segments from duplicate slashes when counting", () => {
    const node: DatasetFileNode = {
      name: "f.csv",
      type: "file",
      parentDir: "/dataset/bob@texera.com/twitterDataset//v1/sub",
    };
    expect(getRelativePathFromDatasetFileNode(node)).toBe("sub/f.csv");
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
