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

import { parseFilePathToDatasetFile, parseDatasetFileToFilePath } from "./dataset-file";

describe("dataset-file path helpers", () => {
  describe("parseFilePathToDatasetFile", () => {
    it("parses a datasets-prefixed path", () => {
      expect(
        parseFilePathToDatasetFile("/datasets/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv")
      ).toEqual({
        ownerEmail: "bob@texera.com",
        datasetName: "twitterDataset",
        versionName: "v1",
        fileRelativePath: "california/irvine/tw1.csv",
      });
    });

    it("still parses a legacy unprefixed path (backward compatibility)", () => {
      expect(parseFilePathToDatasetFile("/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv")).toEqual({
        ownerEmail: "bob@texera.com",
        datasetName: "twitterDataset",
        versionName: "v1",
        fileRelativePath: "california/irvine/tw1.csv",
      });
    });

    it("does not strip a non-datasets leading segment (only 'datasets' is a prefix)", () => {
      // "models" is treated as the ownerEmail segment, matching FileResolver on the backend.
      expect(parseFilePathToDatasetFile("/models/bob@texera.com/twitterDataset/v1/tw1.csv")).toEqual({
        ownerEmail: "models",
        datasetName: "bob@texera.com",
        versionName: "twitterDataset",
        fileRelativePath: "v1/tw1.csv",
      });
    });

    it("throws when the path has fewer than four segments", () => {
      expect(() => parseFilePathToDatasetFile("/datasets/bob@texera.com/twitterDataset")).toThrowError(
        "Invalid file path format"
      );
    });
  });

  describe("parseDatasetFileToFilePath", () => {
    it("emits the datasets prefix", () => {
      expect(
        parseDatasetFileToFilePath({
          ownerEmail: "bob@texera.com",
          datasetName: "twitterDataset",
          versionName: "v1",
          fileRelativePath: "california/irvine/tw1.csv",
        })
      ).toBe("/datasets/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv");
    });
  });

  it("round-trips a prefixed path through parse then build", () => {
    const path = "/datasets/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv";
    expect(parseDatasetFileToFilePath(parseFilePathToDatasetFile(path))).toBe(path);
  });
});
