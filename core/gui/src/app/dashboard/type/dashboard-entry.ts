import { DashboardFile } from "./dashboard-file.interface";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardProject } from "./dashboard-project.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";
import { isDashboardWorkflow, isDashboardProject, isDashboardFile, isDashboardDataset } from "./type-predicates";

export interface UserInfo {
  userName: string;
  googleAvatar?: string;
}

export class DashboardEntry {
  checked = false;
  type: "workflow" | "project" | "file" | "dataset";
  name: string;
  creationTime: number | undefined;
  lastModifiedTime: number | undefined;
  id: number | undefined;
  description: string | undefined;
  accessLevel: string | undefined;
  ownerName: string | undefined;
  ownerEmail: string | undefined;
  ownerGoogleAvatar: string | undefined;

  constructor(public value: DashboardWorkflow | DashboardProject | DashboardFile | DashboardDataset) {
    if (isDashboardWorkflow(value)) {
      this.type = "workflow";
      this.id = value.workflow.wid;
      this.name = value.workflow.name;
      this.description = value.workflow.description;
      this.creationTime = value.workflow.creationTime;
      this.lastModifiedTime = value.workflow.lastModifiedTime;
      this.accessLevel = value.accessLevel;
      this.ownerName = value.ownerName;
      this.ownerEmail = "";
      this.ownerGoogleAvatar = "";
    } else if (isDashboardProject(value)) {
      this.type = "project";
      this.id = value.pid;
      this.name = value.name;
      this.description = "";
      this.creationTime = value.creationTime;
      this.lastModifiedTime = value.creationTime;
      this.accessLevel = value.accessLevel;
      this.ownerName = "";
      this.ownerEmail = "";
      this.ownerGoogleAvatar = "";
    } else if (isDashboardFile(value)) {
      this.type = "file";
      this.id = value.file.fid;
      this.name = value.file.name;
      this.description = value.file.description;
      this.creationTime = value.file.uploadTime;
      this.lastModifiedTime = value.file.uploadTime;
      this.accessLevel = value.accessLevel;
      this.ownerName = "";
      this.ownerEmail = value.ownerEmail;
      this.ownerGoogleAvatar = "";
    } else if (isDashboardDataset(value)) {
      this.type = "dataset";
      this.id = value.dataset.did;
      this.name = value.dataset.name;
      this.description = value.dataset.description;
      this.creationTime = value.dataset.creationTime;
      this.lastModifiedTime = value.dataset.creationTime;
      this.accessLevel = value.accessPrivilege;
      this.ownerName = "";
      this.ownerEmail = value.ownerEmail;
      this.ownerGoogleAvatar = "";
    } else {
      throw new Error("Unexpected type in DashboardEntry.");
    }
  }

  setOwnerName(ownerName: string): void {
    this.ownerName = ownerName;
  }

  setOwnerGoogleAvatar(ownerGoogleAvatar: string): void{
    this.ownerGoogleAvatar = ownerGoogleAvatar
  }

  get project(): DashboardProject {
    if (!isDashboardProject(this.value)) {
      throw new Error("Value is not of type DashboardProject.");
    }
    return this.value;
  }

  get workflow(): DashboardWorkflow {
    if (!isDashboardWorkflow(this.value)) {
      throw new Error("Value is not of type DashboardWorkflow.");
    }
    return this.value;
  }

  get file(): DashboardFile {
    if (!isDashboardFile(this.value)) {
      throw new Error("Value is not of type DashboardFile.");
    }
    return this.value;
  }

  get dataset(): DashboardDataset {
    if (!isDashboardDataset(this.value)) {
      throw new Error("Value is not of type DashboardDataset");
    }
    return this.value;
  }
}
