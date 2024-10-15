import { Dataset, DatasetVersion } from "../../common/type/dataset";
import { DatasetFileNode } from "../../common/type/datasetVersionFileTree";

export interface DashboardDataset {
  isOwner: boolean;
  ownerEmail: string;
  dataset: Dataset;
  accessPrivilege: "READ" | "WRITE" | "NONE";
  versions: {
    datasetVersion: DatasetVersion;
    fileNodes: DatasetFileNode[];
  }[];
  size?: number;
}
