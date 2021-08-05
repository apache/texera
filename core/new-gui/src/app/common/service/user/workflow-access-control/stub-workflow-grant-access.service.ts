import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { of } from 'rxjs';
import { UserWorkflowAccess, WorkflowGrantAccessService } from './workflow-grant-access.service';
import { Workflow, WorkflowContent } from '../../../type/workflow';
import { jsonCast } from '../../../util/storage';

export const MOCK_WORKFLOW: Workflow = {
  wid: 1,
  name: 'project 1',
  content: jsonCast<WorkflowContent>(' {"operators":[],"operatorPositions":{},"links":[],"groups":[],"breakpoints":{}}'),
  creationTime: 1,
  lastModifiedTime: 2,
};

type PublicInterfaceOf<Class> = {
  [Member in keyof Class]: Class[Member];
};

@Injectable()
export class StubWorkflowGrantAccessService implements PublicInterfaceOf<WorkflowGrantAccessService> {


  public workflow: Workflow;

  public message: string = 'This is testing';

  public mapString: UserWorkflowAccess[] = [];

  constructor() {
    this.workflow = MOCK_WORKFLOW;
  }

  public retrieveGrantedWorkflowAccessList(workflow: Workflow): Observable<Readonly<UserWorkflowAccess>[]> {
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
