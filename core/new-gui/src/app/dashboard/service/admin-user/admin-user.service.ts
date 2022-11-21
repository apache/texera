import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { DashboardAdminUserEntry } from "../../type/dashboard-admin-user-entry";
import { AccessEntry } from "../../type/access.interface";
export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/user`;
export const USER_LIST_URL = `${USER_BASE_URL}/list`;
export const USER_AUTOCOMPLETE_FILE_LIST_URL = `${USER_BASE_URL}/autocomplete`;
export const USER_FILE_ACCESS_BASE_URL = `${USER_BASE_URL}/access`;
export const USER_FILE_ACCESS_LIST_URL = `${USER_FILE_ACCESS_BASE_URL}/list`;
export const USER_FILE_ACCESS_REVOKE_URL = `${USER_FILE_ACCESS_BASE_URL}/revoke`;
export const USER_FILE_NAME_UPDATE_URL = `${USER_BASE_URL}/update/name`;

@Injectable({
  providedIn: "root",
})
export class AdminUserService {
  private dashboardUserFileEntryChanged = new BehaviorSubject<void>(undefined);

  constructor(private http: HttpClient) {}

  public getUserFilesChangedEvent(): Observable<void> {
    return this.dashboardUserFileEntryChanged.asObservable();
  }

  public updateUserFilesChangedEvent(): void {
    this.dashboardUserFileEntryChanged.next();
  }



  /**
   * Retrieve all shared accesses of the given dashboardUserFileEntry.
   * @param userFileEntry the current dashboardUserFileEntry
   * @return ReadonlyArray<AccessEntry> an array of UserFileAccesses, Ex: [{username: TestUser, fileAccess: read}]
   */
  public getUserFileAccessList(userFileEntry: DashboardAdminUserEntry): Observable<ReadonlyArray<AccessEntry>> {
    return this.http.get<ReadonlyArray<AccessEntry>>(
      `${USER_FILE_ACCESS_LIST_URL}/${userFileEntry.name}/${userFileEntry.name}`
    );
  }

  /**
   * Remove an existing access of another user
   * @param userFileEntry the current dashboardUserFileEntry
   * @param username the username of target user
   * @return message of success
   */
  public revokeUserFileAccess(userFileEntry: DashboardAdminUserEntry, username: string): Observable<Response> {
    return this.http.delete<Response>(
      `${USER_FILE_ACCESS_REVOKE_URL}/${userFileEntry.name}/${userFileEntry.name}/${username}`
    );
  }



  public retrieveDashboardUserFileEntryList(): Observable<ReadonlyArray<DashboardAdminUserEntry>> {
    return this.http.get<ReadonlyArray<DashboardAdminUserEntry>>(`${USER_LIST_URL}`);
  }

  public getAutoCompleteUserFileAccessList(query: String): Observable<ReadonlyArray<string>> {
    return this.http.get<ReadonlyArray<string>>(`${USER_AUTOCOMPLETE_FILE_LIST_URL}/${query}`);
  }

  /**
   * updates the file name of a given userFileEntry
   */
  public updateFileName(fid: number, name: string): Observable<void> {
    return this.http.post<void>(`${USER_FILE_NAME_UPDATE_URL}`, {
      fid: fid,
      name: name,
    });
  }
}
