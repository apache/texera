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

import { DatasetFile, parseDatasetFileToFilePath, parseFilePathToDatasetFile } from "./dataset-file";

describe("parseFilePathToDatasetFile", () => {
  it("parses owner, dataset, version, and single-segment relative path", () => {
    const result = parseFilePathToDatasetFile("/bob@texera.com/twitterDataset/v1/tw1.csv");
    expect(result).toEqual({
      ownerEmail: "bob@texera.com",
      datasetName: "twitterDataset",
      versionName: "v1",
      fileRelativePath: "tw1.csv",
    });
  });

  it("joins remaining segments into a nested relative path", () => {
    const result = parseFilePathToDatasetFile("/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv");
    expect(result.ownerEmail).toBe("bob@texera.com");
    expect(result.datasetName).toBe("twitterDataset");
    expect(result.versionName).toBe("v1");
    expect(result.fileRelativePath).toBe("california/irvine/tw1.csv");
  });

  it("ignores empty segments from leading, trailing, and duplicate slashes", () => {
    const result = parseFilePathToDatasetFile("//bob@texera.com//twitterDataset/v1/dir//file.csv/");
    expect(result).toEqual({
      ownerEmail: "bob@texera.com",
      datasetName: "twitterDataset",
      versionName: "v1",
      fileRelativePath: "dir/file.csv",
    });
  });

  it("throws when there are fewer than four path segments", () => {
    expect(() => parseFilePathToDatasetFile("/bob@texera.com/twitterDataset/v1")).toThrow("Invalid file path format");
    expect(() => parseFilePathToDatasetFile("")).toThrow("Invalid file path format");
    expect(() => parseFilePathToDatasetFile("/just/three/parts")).toThrow("Invalid file path format");
  });
});

describe("parseDatasetFileToFilePath", () => {
  it("assembles a slash-delimited path with a leading slash", () => {
    const datasetFile: DatasetFile = {
      ownerEmail: "bob@texera.com",
      datasetName: "twitterDataset",
      versionName: "v1",
      fileRelativePath: "california/irvine/tw1.csv",
    };
    expect(parseDatasetFileToFilePath(datasetFile)).toBe("/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv");
  });
});

describe("dataset-file round trips", () => {
  it("path -> DatasetFile -> path is stable for a canonical path", () => {
    const path = "/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv";
    expect(parseDatasetFileToFilePath(parseFilePathToDatasetFile(path))).toBe(path);
  });

  it("DatasetFile -> path -> DatasetFile is stable for a canonical object", () => {
    const datasetFile: DatasetFile = {
      ownerEmail: "alice@texera.com",
      datasetName: "sensorData",
      versionName: "v42",
      fileRelativePath: "2026/reading.json",
    };
    expect(parseFilePathToDatasetFile(parseDatasetFileToFilePath(datasetFile))).toEqual(datasetFile);
  });
});
