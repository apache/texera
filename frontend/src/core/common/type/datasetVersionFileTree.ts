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

export interface DatasetFileNode {
  name: string;
  type: "file" | "directory";
  children?: DatasetFileNode[]; // Only populated if 'type' is 'directory'
  parentDir: string;
  ownerEmail?: string;
  size?: number; // Only populated if 'type' is 'file'
}

export function getFullPathFromDatasetFileNode(node: DatasetFileNode): string {
  return `${node.parentDir}/${node.name}`;
}

/**
 * Returns the relative path of a DatasetFileNode by stripping the first four segments
 * (dataset/ownerEmail/datasetName/versionName).
 * @param node The DatasetFileNode whose relative path is needed.
 * @returns The relative path (without the first four segments and without a leading slash).
 */
export function getRelativePathFromDatasetFileNode(node: DatasetFileNode): string {
  const fullPath = getFullPathFromDatasetFileNode(node); // Get the full path
  const pathSegments = fullPath.split("/").filter(segment => segment.length > 0); // Split and remove empty segments

  if (pathSegments.length <= 4) {
    return ""; // If there are 4 or fewer segments, return an empty string (no relative path exists)
  }

  return pathSegments.slice(4).join("/"); // Join remaining segments as the relative path
}

export function getPathsUnderOrEqualDatasetFileNode(node: DatasetFileNode): string[] {
  // Helper function to recursively gather paths
  const gatherPaths = (node: DatasetFileNode): string[] => {
    // Base case: if the node is a file, return its path
    if (node.type === "file") {
      return [getFullPathFromDatasetFileNode(node)];
    }

    // Recursive case: if the node is a directory, explore its children
    return node.children ? node.children.flatMap(child => gatherPaths(child)) : [];
  };

  return gatherPaths(node);
}
