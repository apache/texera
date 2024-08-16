import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardProject } from "./dashboard-project.interface";
import { DashboardFile } from "./dashboard-file.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";

export function isDashboardWorkflow(value: any): value is DashboardWorkflow {
  return value && typeof value.workflow === "object";
}

export function isDashboardProject(value: any): value is DashboardProject {
  return value && typeof value.name === "string" && !value.workflow;
}

export function isDashboardFile(value: any): value is DashboardFile {
  return value && typeof value.ownerEmail === "string" && typeof value.file === "object";
}

export function isDashboardDataset(value: any): value is DashboardDataset {
  return value && typeof value.dataset === "object";
}
