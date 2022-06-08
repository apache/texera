import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { DashboardUserFileEntry, UserFile } from "../../type/dashboard-user-file-entry";
import { AccessEntry } from "../../type/access.interface";

export const USER_FILE_BASE_URL = `${AppSettings.getApiEndpoint()}/user/file`;
export const USER_FILE_LIST_URL = `${USER_FILE_BASE_URL}/list`;
export const USER_FILE_DELETE_URL = `${USER_FILE_BASE_URL}/delete`;
export const USER_FILE_DOWNLOAD_URL = `${USER_FILE_BASE_URL}/download`;
export const USER_FILE_ACCESS_BASE_URL = `${USER_FILE_BASE_URL}/access`;
export const USER_FILE_ACCESS_GRANT_URL = `${USER_FILE_ACCESS_BASE_URL}/grant`;
export const USER_FILE_ACCESS_LIST_URL = `${USER_FILE_ACCESS_BASE_URL}/list`;
export const USER_FILE_ACCESS_REVOKE_URL = `${USER_FILE_ACCESS_BASE_URL}/revoke`;
export const USER_FILE_NAME_UPDATE_URL = `${USER_FILE_BASE_URL}/update/name`;

@Injectable({
  providedIn: "root",
})
export class UserFileService {
  private dashboardUserFileEntryChanged = new BehaviorSubject<void>(undefined);

  constructor(private http: HttpClient) {}

  public getUserFilesChangedEvent(): Observable<void> {
    return this.dashboardUserFileEntryChanged.asObservable();
  }

  public updateUserFilesChangedEvent(): void {
    this.dashboardUserFileEntryChanged.next();
  }

  /**
   * delete the targetFile in the backend.
   * @param targetUserFileEntry
   */
  public deleteDashboardUserFileEntry(targetUserFileEntry: DashboardUserFileEntry): Observable<Response> {
    return this.http.delete<Response>(
      `${USER_FILE_DELETE_URL}/${targetUserFileEntry.file.name}/${targetUserFileEntry.ownerName}`
    );
  }

  /**
   * convert the input file size to the human readable size by adding the unit at the end.
   * eg. 2048 -> 2.0 KB
   * @param fileSize
   */
  public addFileSizeUnit(fileSize: number): string {
    if (fileSize <= 1024) {
      return fileSize + " Byte";
    }

    let i = 0;
    const byteUnits = [" Byte", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"];
    while (fileSize > 1024 && i < byteUnits.length - 1) {
      fileSize = fileSize / 1024;
      i++;
    }
    return Math.max(fileSize, 0.1).toFixed(1) + byteUnits[i];
  }

  /**
   * Assign a new access to/Modify an existing access of another user
   * @param userFileEntry the file entry that is selected
   * @param username the username of target user
   * @param accessLevel the type of access offered
   * @return Response
   */
  public grantUserFileAccess(
    userFileEntry: DashboardUserFileEntry,
    username: string,
    accessLevel: string
  ): Observable<Response> {
    return this.http.post<Response>(`${USER_FILE_ACCESS_GRANT_URL}`, {
      username,
      fileName: userFileEntry.file.name,
      ownerName: userFileEntry.ownerName,
      accessLevel,
    });
  }

  /**
   * Retrieve all shared accesses of the given dashboardUserFileEntry.
   * @param userFileEntry the current dashboardUserFileEntry
   * @return ReadonlyArray<AccessEntry> an array of UserFileAccesses, Ex: [{username: TestUser, fileAccess: read}]
   */
  public getUserFileAccessList(userFileEntry: DashboardUserFileEntry): Observable<ReadonlyArray<AccessEntry>> {
    return this.http.get<ReadonlyArray<AccessEntry>>(
      `${USER_FILE_ACCESS_LIST_URL}/${userFileEntry.file.name}/${userFileEntry.ownerName}`
    );
  }

  /**
   * Remove an existing access of another user
   * @param userFileEntry the current dashboardUserFileEntry
   * @param username the username of target user
   * @return message of success
   */
  public revokeUserFileAccess(userFileEntry: DashboardUserFileEntry, username: string): Observable<Response> {
    return this.http.delete<Response>(
      `${USER_FILE_ACCESS_REVOKE_URL}/${userFileEntry.file.name}/${userFileEntry.ownerName}/${username}`
    );
  }

  public downloadUserFile(targetFile: UserFile): Observable<Blob> {
    const requestURL = `${USER_FILE_DOWNLOAD_URL}/${targetFile.fid}`;
    return this.http.get(requestURL, { responseType: "blob" });
  }

  public retrieveDashboardUserFileEntryList(): Observable<ReadonlyArray<DashboardUserFileEntry>> {
    return this.http.get<ReadonlyArray<DashboardUserFileEntry>>(`${USER_FILE_LIST_URL}`);
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
