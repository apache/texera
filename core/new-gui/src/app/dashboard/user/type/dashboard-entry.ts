import { DashboardWorkflowEntry } from "./dashboard-workflow-entry";
import { UserProject } from "./user-project";

export class DashboardEntry {
  checked = false;
  get type(): "workflow" | "project" {
    if ("workflow" in this.value) {
      return "workflow";
    } else if ("name" in this.value) {
      return "project";
    }
    throw new Error("Unexpected type in DashboardEntry.");
  }
  get name(): string {
    if ("workflow" in this.value) {
      return this.value.workflow.name;
    } else if ("name" in this.value) {
      return this.project.name;
    }
    throw new Error("Unexpected type in DashboardEntry.");
  }

  get creationTime(): number | undefined {
    if ("workflow" in this.value) {
      return this.value.workflow.creationTime;
    } else if ("name" in this.value) {
      return this.value.creationTime;
    }
    throw new Error("Unexpected type in DashboardEntry.");
  }

  get lastModifiedTime(): number | undefined {
    if ("workflow" in this.value) {
      return this.value.workflow.lastModifiedTime;
    } else if ("name" in this.value) {
      return this.value.creationTime;
    }
    throw new Error("Unexpected type in DashboardEntry.");
  }

  get project(): UserProject {
    if (!("name" in this.value)) {
      throw new Error("Value is not of type Workflow.");
    }
    return this.value;
  }

  get workflow(): DashboardWorkflowEntry {
    if (!("workflow" in this.value)) {
      throw new Error("Value is not of type Workflow.");
    }
    return this.value;
  }
  constructor(public value: DashboardWorkflowEntry | UserProject) {}
}
