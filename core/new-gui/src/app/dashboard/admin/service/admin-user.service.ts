import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { Role, User } from "../../../common/type/user";
export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/user`;
export const USER_LIST_URL = `${USER_BASE_URL}/list`;
export const USER_UPDATE_URL = `${USER_BASE_URL}/update`;
export const USER_ADD_URL = `${USER_BASE_URL}/add`;

@Injectable({
  providedIn: "root",
})
export class AdminUserService {
  constructor(private http: HttpClient) {}

  public getUserList(): Observable<ReadonlyArray<User>> {
    return this.http.get<ReadonlyArray<User>>(`${USER_LIST_URL}`);
  }

  public updateUser(uid: number, name: string, email: string, role: Role): Observable<void> {
    return this.http.put<void>(`${USER_UPDATE_URL}`, {
      uid: uid,
      name: name,
      email: email,
      role: role,
    });
  }

  public addUser(): Observable<Response> {
    return this.http.post<Response>(`${USER_ADD_URL}/`, {});
  }
}
