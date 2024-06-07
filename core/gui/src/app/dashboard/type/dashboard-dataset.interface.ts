import { Dataset, DatasetVersion } from "../../../common/type/dataset";
import { FileNode } from "../../../common/type/fileNode";
import { DatasetVersionFileTreeNode } from "../../../common/type/datasetVersionFileTree";

export interface DashboardDataset {
  isOwner: boolean;
  dataset: Dataset;
  accessPrivilege: "READ" | "WRITE" | "NONE";
  datasetRootFileNode: DatasetVersionFileTreeNode;
  versions: {
    datasetVersion: DatasetVersion;
    fileNodes: FileNode[];
  }[];
}
