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
  getFullPathFromDatasetFileNode,
  getRelativePathFromDatasetFileNode,
} from "./datasetVersionFileTree";

describe("datasetVersionFileTree path helpers", () => {
  const fileNode = (parentDir: string, name: string): DatasetFileNode => ({
    name,
    type: "file",
    parentDir,
  });

  describe("getFullPathFromDatasetFileNode", () => {
    it("joins parentDir and name", () => {
      const node = fileNode("/datasets/bob@texera.com/twitterDataset/v1/california/irvine", "tw1.csv");
      expect(getFullPathFromDatasetFileNode(node)).toBe(
        "/datasets/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv"
      );
    });
  });

  describe("getRelativePathFromDatasetFileNode", () => {
    it("strips the datasets/owner/dataset/version prefix (4 segments)", () => {
      const node = fileNode("/datasets/bob@texera.com/twitterDataset/v1/california/irvine", "tw1.csv");
      expect(getRelativePathFromDatasetFileNode(node)).toBe("california/irvine/tw1.csv");
    });

    it("returns the bare file name for a file at the version root", () => {
      const node = fileNode("/datasets/bob@texera.com/twitterDataset/v1", "readme.txt");
      expect(getRelativePathFromDatasetFileNode(node)).toBe("readme.txt");
    });

    it("returns empty string when there is no path below the version", () => {
      const node = fileNode("/datasets/bob@texera.com/twitterDataset", "v1");
      expect(getRelativePathFromDatasetFileNode(node)).toBe("");
    });
  });
});
