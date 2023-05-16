import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { DashboardUserFileEntry, UserFile } from "../../type/dashboard-user-file-entry";

export const USER_FILE_BASE_URL = `${AppSettings.getApiEndpoint()}/user/file`;
export const USER_FILE_LIST_URL = `${USER_FILE_BASE_URL}/list`;
export const USER_AUTOCOMPLETE_FILE_LIST_URL = `${USER_FILE_BASE_URL}/autocomplete`;
export const USER_FILE_DELETE_URL = `${USER_FILE_BASE_URL}/delete`;
export const USER_FILE_DOWNLOAD_URL = `${USER_FILE_BASE_URL}/download`;
export const USER_FILE_ACCESS_BASE_URL = `${USER_FILE_BASE_URL}/access`;
export const USER_FILE_ACCESS_GRANT_URL = `${USER_FILE_ACCESS_BASE_URL}/grant`;
export const USER_FILE_ACCESS_LIST_URL = `${USER_FILE_ACCESS_BASE_URL}/list`;
export const USER_FILE_ACCESS_REVOKE_URL = `${USER_FILE_ACCESS_BASE_URL}/revoke`;
export const USER_FILE_NAME_UPDATE_URL = `${USER_FILE_BASE_URL}/name`;
export const USER_FILE_DESCRIPTION_UPDATE_URL = `${USER_FILE_BASE_URL}/description`;

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
   * @param fid
   */
  public deleteFile(fid: number): Observable<void> {
    return this.http.delete<void>(`${USER_FILE_DELETE_URL}/${fid}`);
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

  public downloadUserFile(targetFile: UserFile): Observable<Blob> {
    return this.http.get(`${USER_FILE_DOWNLOAD_URL}/${targetFile.fid}`, { responseType: "blob" });
  }

  public retrieveDashboardUserFileEntryList(): Observable<ReadonlyArray<DashboardUserFileEntry>> {
    return this.http.get<ReadonlyArray<DashboardUserFileEntry>>(`${USER_FILE_LIST_URL}`);
  }

  public getAutoCompleteUserFileAccessList(query: String): Observable<ReadonlyArray<string>> {
    return this.http.get<ReadonlyArray<string>>(`${USER_AUTOCOMPLETE_FILE_LIST_URL}/${query}`);
  }

  /**
   * updates the file name of a given userFileEntry
   */
  public updateFileName(fid: number, name: string): Observable<void> {
    return this.http.put<void>(`${USER_FILE_NAME_UPDATE_URL}/${fid}/${name}`, null);
  }

  /**
   * updates the file description of a given userFileEntry
   */
  public updateFileDescription(fid: number, description: string): Observable<void> {
    return this.http.put<void>(`${USER_FILE_DESCRIPTION_UPDATE_URL}/${fid}/${description}`, null);
  }
}
