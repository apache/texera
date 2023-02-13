import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
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
    throw new Error("Method not implemented.");
  }
  public revokeAccess(wid: number, username: string): Observable<Response> {
    throw new Error("Method not implemented.");
  }
  public getOwner(wid: number): Observable<string> {
    throw new Error("Method not implemented.");
  }
  public getList(wid: number | undefined): Observable<readonly AccessEntry2[]> {
    throw new Error("Method not implemented.");
  }
}
