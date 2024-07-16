import { Dataset } from "../../common/type/dataset";

export interface DashboardDataset {
  isOwner: boolean;
  dataset: Dataset;
  accessPrivilege: "READ" | "WRITE" | "NONE";
}
