import { DashboardFile } from "./dashboard-file.interface";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardProject } from "./dashboard-project.interface";

export interface SearchResult {
  resourceType: "workflow" | "project" | "file";
  workflow?: DashboardWorkflow;
  project?: DashboardProject;
  file?: DashboardFile;
}
