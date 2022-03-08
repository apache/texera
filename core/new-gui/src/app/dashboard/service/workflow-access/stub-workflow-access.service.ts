import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { of } from "rxjs";
import { WorkflowAccessService } from "./workflow-access.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { jsonCast } from "../../../common/util/storage";
import { AccessEntry } from "../../type/access.interface";

export const MOCK_WORKFLOW: Workflow = {
  wid: 1,
  name: "project 1",
  content: jsonCast<WorkflowContent>(
    ' {"operators":[],"operatorPositions":{},"links":[],"groups":[],"breakpoints":{}}'
  ),
  creationTime: 1,
  lastModifiedTime: 2,
};

type PublicInterfaceOf<Class> = {
  [Member in keyof Class]: Class[Member];
};

@Injectable()
export class StubWorkflowAccessService implements PublicInterfaceOf<WorkflowAccessService> {
  public workflow: Workflow;

  public message: string = "This is testing";

  public mapString: AccessEntry[] = [];

  constructor() {
    this.workflow = MOCK_WORKFLOW;
  }

  public retrieveGrantedWorkflowAccessList(workflow: Workflow): Observable<ReadonlyArray<AccessEntry>> {
    return of(this.mapString);
  }

  public grantUserWorkflowAccess(workflow: Workflow, username: string, accessLevel: string): Observable<Response> {
    return of();
  }

  public revokeWorkflowAccess(workflow: Workflow, username: string): Observable<Response> {
    return of();
  }

  public getWorkflowOwner(workflow: Workflow): Observable<Readonly<{ ownerName: string }>> {
    return of();
  }
}
