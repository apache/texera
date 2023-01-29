import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { User } from "../../../common/type/user";
export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/user`;
export const USER_LIST_URL = `${USER_BASE_URL}/list`;
export const USER_UPDATE_URL = `${USER_BASE_URL}/update`;

@Injectable({
  providedIn: "root",
})
export class AdminUserService {
  constructor(private http: HttpClient) {}

  public retrieveUserList(): Observable<ReadonlyArray<User>> {
    return this.http.get<ReadonlyArray<User>>(`${USER_LIST_URL}`);
  }

  public updateRole(uid: number, role: number): Observable<void> {
    return this.http.post<void>(`${USER_UPDATE_URL}`, {
      uid: uid,
      role: role,
    });
  }
}
