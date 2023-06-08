import { UserFile } from "./dashboard-file.interface";
import { DashboardWorkflowEntry } from "./dashboard-workflow-entry";
import { UserProject } from "./user-project";

export interface SearchResult {
  resourceType: "workflow" | "project" | "file";
  workflow?: DashboardWorkflowEntry;
  project?: UserProject;
  file?: UserFile;
}
