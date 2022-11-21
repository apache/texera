import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { DashboardAdminUserEntry } from "../../type/dashboard-admin-user-entry";
export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/user`;
export const USER_LIST_URL = `${USER_BASE_URL}/list`;
export const USER_AUTOCOMPLETE_FILE_LIST_URL = `${USER_BASE_URL}/autocomplete`;
export const USER_UPDATE_URL = `${USER_BASE_URL}/update`;

@Injectable({
  providedIn: "root",
})
export class AdminUserService {
  private dashboardUserFileEntryChanged = new BehaviorSubject<void>(undefined);

  constructor(private http: HttpClient) {}

  public updateUserFilesChangedEvent(): void {
    this.dashboardUserFileEntryChanged.next();
  }

  public retrieveUserList(): Observable<ReadonlyArray<DashboardAdminUserEntry>> {
    return this.http.get<ReadonlyArray<DashboardAdminUserEntry>>(`${USER_LIST_URL}`);
  }

  public getAutoCompleteUserFileAccessList(query: String): Observable<ReadonlyArray<string>> {
    return this.http.get<ReadonlyArray<string>>(`${USER_AUTOCOMPLETE_FILE_LIST_URL}/${query}`);
  }

  /**
   * updates the permission of a given user
   */
  public updatePermission(uid: number, permission: number): Observable<void> {
    return this.http.post<void>(`${USER_UPDATE_URL}`, {
      uid: uid,
      permission: permission,
    });
  }
}
