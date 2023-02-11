import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { AccessEntry } from "../../type/access.interface";

export const BASE = `${AppSettings.getApiEndpoint()}/workflow/access`;
export const GRANT_URL = BASE + "/grant";
export const LIST_URL = BASE + "/list";
export const REVOKE_URL = BASE + "/revoke";
export const OWNER_URL = BASE + "/owner";

@Injectable({
  providedIn: "root",
})
export class WorkflowAccessService {
  constructor(private http: HttpClient) {}

  public grantAccess(wid: number, email: string, accessLevel: string): Observable<Response> {
    return this.http.put<Response>(`${GRANT_URL}/${wid}/${email}/${accessLevel}`, null);
  }

  public revokeAccess(wid: number, username: string): Observable<Response> {
    return this.http.delete<Response>(`${REVOKE_URL}/${wid}/${username}`);
  }

  public getOwner(wid: number): Observable<string> {
    return this.http.get(`${OWNER_URL}/${wid}`, { responseType: "text" });
  }

  public getList(wid: number | undefined): Observable<ReadonlyArray<AccessEntry>> {
    return this.http.get<ReadonlyArray<AccessEntry>>(`${LIST_URL}/${wid}`);
  }
}
