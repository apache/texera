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

// user given filePath is /ownerEmail/datasetName/versionName/fileRelativePath
// e.g. /bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
export interface DatasetFile {
  ownerEmail: string;
  datasetName: string;
  versionName: string;
  fileRelativePath: string;
}

/**
 * Parses a file path string to a DatasetFile interface.
 * @param filePath - The file path string to parse.
 * @returns The parsed DatasetFile object.
 */
export function parseFilePathToDatasetFile(filePath: string): DatasetFile {
  const parts = filePath.split("/").filter(part => part.length > 0);

  if (parts.length < 4) {
    throw new Error("Invalid file path format");
  }

  const [ownerEmail, datasetName, versionName, ...fileRelativePathParts] = parts;
  const fileRelativePath = fileRelativePathParts.join("/");

  return {
    ownerEmail,
    datasetName,
    versionName,
    fileRelativePath,
  };
}

/**
 * Converts a DatasetFile object to a file path string.
 * @param datasetFile - The DatasetFile object to convert.
 * @returns The file path string.
 */
export function parseDatasetFileToFilePath(datasetFile: DatasetFile): string {
  const { ownerEmail, datasetName, versionName, fileRelativePath } = datasetFile;
  return `/${ownerEmail}/${datasetName}/${versionName}/${fileRelativePath}`;
}
