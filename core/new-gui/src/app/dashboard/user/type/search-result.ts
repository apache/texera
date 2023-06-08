import { UserFile } from "./dashboard-file.interface";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { UserProject } from "./user-project";

export interface SearchResult {
  resourceType: "workflow" | "project" | "file";
  workflow?: DashboardWorkflow;
  project?: UserProject;
  file?: UserFile;
}
