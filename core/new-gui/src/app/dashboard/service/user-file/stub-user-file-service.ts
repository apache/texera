import { Injectable } from "@angular/core";
import { Observable, of, Subject } from "rxjs";
import { DashboardUserFileEntry, UserFile } from "../../type/dashboard-user-file-entry";
import { PublicInterfaceOf } from "../../../common/util/stub";
import { UserFileService } from "./user-file.service";
import { HttpClient } from "@angular/common/http";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { AccessEntry } from "../../type/access.interface";

@Injectable({
  providedIn: "root",
})
export class StubUserFileService implements PublicInterfaceOf<UserFileService> {
  public testUFAs: AccessEntry[] = [];
  private userFiles: DashboardUserFileEntry[] = [];
  private userFilesChanged = new Subject<null>();

  constructor(private http: HttpClient, private userService: StubUserService) {
    StubUserFileService.detectUserChanges();
  }

  public grantUserFileAccess(
    file: DashboardUserFileEntry,
    username: string,
    accessLevel: string
  ): Observable<Response> {
    return of();
  }

  public getUserFileAccessList(dashboardUserFileEntry: DashboardUserFileEntry): Observable<ReadonlyArray<AccessEntry>> {
    return of();
  }

  public revokeUserFileAccess(dashboardUserFileEntry: DashboardUserFileEntry, username: string): Observable<Response> {
    return of();
  }

  public getUserFilesChangedEvent(): Observable<void> {
    return of();
  }

  public updateUserFilesChangedEvent(): void {
    return;
  }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getFileArray}
   */
  public refreshDashboardUserFileEntries(): void {
    return;
  }

  /**
   * delete the targetFile in the backend.
   * this function will automatically refresh the files in the service when succeed.
   * @param targetFile
   */
  public deleteDashboardUserFileEntry(targetFile: DashboardUserFileEntry): Observable<Response> {
    return of();
  }

  addFileSizeUnit(fileSize: number): string {
    return "";
  }

  getUserFiles(): ReadonlyArray<DashboardUserFileEntry> {
    return [];
  }

  getDownloadURL(targetFile: UserFile): string {
    return "";
  }

  requestDownloadUserFile(targetFile: UserFile): Observable<Blob> {
    return of();
  }

  downloadUserFile(targetFile: UserFile): Observable<Blob> {
    return of();
  }

  /**
   * refresh the files in the service whenever the user changes.
   */
  private static detectUserChanges(): void {
    return;
  }

  /**
   * retrieve the files from the backend
   */
  public retrieveDashboardUserFileEntryList(): Observable<ReadonlyArray<DashboardUserFileEntry>> {
    return of();
  }

  public getAutoCompleteUserFileAccessList(): Observable<ReadonlyArray<string>> {
    return of();
  }

  public updateFileName(fid: number, name: string): Observable<void> {
    return of();
  }

  public updateFileDescription(fid: number, description: string): Observable<void> {
    return of();
  }
}
