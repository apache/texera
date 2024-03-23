import { DashboardFile } from "./dashboard-file.interface";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardProject } from "./dashboard-project.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";

export interface SearchResultItem {
  resourceType: "workflow" | "project" | "file" | "dataset";
  workflow?: DashboardWorkflow;
  project?: DashboardProject;
  file?: DashboardFile;
  dataset?: DashboardFile;
}

export interface SearchResult {
  results: SearchResultItem[];
  more: boolean;
}
