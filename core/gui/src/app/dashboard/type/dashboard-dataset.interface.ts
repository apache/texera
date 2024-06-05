import { Dataset, DatasetVersion} from "../../common/type/dataset";
import {FileNode} from "../../../common/type/fileNode";

export interface DashboardDataset {
  isOwner: boolean;
  dataset: Dataset;
  accessPrivilege: "READ" | "WRITE" | "NONE";
  versions: {
    datasetVersion: DatasetVersion;
    fileNodes: FileNode[];
  }[];
}
