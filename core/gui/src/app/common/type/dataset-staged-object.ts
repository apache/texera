// Represents a staged dataset object change, corresponding to backend Diff
export interface DatasetStagedObject {
  path: string;
  pathType: "file" | "directory";
  diffType: "added" | "removed" | "changed";
  sizeBytes?: number; // Optional, only present for files
}
