import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { AccessService } from "./access.service";
import { ShareAccessEntry } from "../../user/type/access.interface";

type PublicInterfaceOf<Class> = {
  [Member in keyof Class]: Class[Member];
};

@Injectable()
export class StubWorkflowAccessService implements PublicInterfaceOf<AccessService> {
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
  public getAccessList(wid: number | undefined): Observable<readonly ShareAccessEntry[]> {
    return of();
  }
}
