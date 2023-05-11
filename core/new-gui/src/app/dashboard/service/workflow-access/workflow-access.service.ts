import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { WorkflowAccessEntry } from "../../type/access.interface";
export const BASE = `${AppSettings.getApiEndpoint()}/workflow/access`;
@Injectable({
  providedIn: "root",
})
export class WorkflowAccessService {
  constructor(private http: HttpClient) {}

  public grantAccess(wid: number, email: string, privilege: string): Observable<Response> {
    return this.http.put<Response>(`${BASE}/grant/${wid}/${email}/${privilege}`, null);
  }

  public revokeAccess(wid: number, username: string): Observable<Response> {
    return this.http.delete<Response>(`${BASE}/revoke/${wid}/${username}`);
  }

  public getOwner(wid: number): Observable<string> {
    return this.http.get(`${BASE}/owner/${wid}`, { responseType: "text" });
  }

  public getAccessList(wid: number | undefined): Observable<ReadonlyArray<WorkflowAccessEntry>> {
    return this.http.get<ReadonlyArray<WorkflowAccessEntry>>(`${BASE}/list/${wid}`);
  }
}
