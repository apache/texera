import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { WorkflowAccessService } from "./workflow-access.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { jsonCast } from "../../../common/util/storage";
import { AccessEntry, AccessEntry2 } from "../../type/access.interface";

export const MOCK_WORKFLOW: Workflow = {
  wid: 1,
  name: "project 1",
  description: "dummy description",
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

  constructor() {
    this.workflow = MOCK_WORKFLOW;
  }

  public grantAccess(wid: number, email: string, privilege: string): Observable<Response> {
    return of();
  }
  public revokeAccess(wid: number, username: string): Observable<Response> {
    return of();
  }
  public getOwner(wid: number): Observable<string> {
    return of();
  }
  public getList(wid: number | undefined): Observable<readonly AccessEntry2[]> {
    return of();
  }
}
