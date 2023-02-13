import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { WorkflowAccessService } from "./workflow-access.service";
import { AccessEntry2 } from "../../type/access.interface";

type PublicInterfaceOf<Class> = {
  [Member in keyof Class]: Class[Member];
};

@Injectable()
export class StubWorkflowAccessService implements PublicInterfaceOf<WorkflowAccessService> {
  public message: string = "This is testing";

  constructor() {}

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
