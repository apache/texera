import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { ShareAccessEntry } from "../../type/share-access.interface";
export const BASE = `${AppSettings.getApiEndpoint()}/access`;
@Injectable({
  providedIn: "root",
})
export class ShareAccessService {
  constructor(private http: HttpClient) {}

  public grantAccess(type: string, id: number, email: string, privilege: string): Observable<Response> {
    return this.http.put<Response>(`${BASE}/${type}/grant/${id}/${email}/${privilege}`, null);
  }

  public revokeAccess(type: string, id: number, username: string): Observable<Response> {
    return this.http.delete<Response>(`${BASE}/${type}/revoke/${id}/${username}`);
  }

  public getOwner(type: string, id: number): Observable<string> {
    return this.http.get(`${BASE}/${type}/owner/${id}`, { responseType: "text" });
  }

  public getAccessList(type: string, id: number | undefined): Observable<ReadonlyArray<ShareAccessEntry>> {
    return this.http.get<ReadonlyArray<ShareAccessEntry>>(`${BASE}/${type}/list/${id}`);
  }
}
