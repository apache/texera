import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, of, Subject } from 'rxjs';
import { DashboardUserFileEntry } from '../../../type/dashboard-user-file-entry';

export const USER_FILE_LIST_URL = 'user/file/list';
export const USER_FILE_DELETE_URL = 'user/file/delete';
export const USER_FILE_SHARE_ACCESS_URL = 'user/file/share';
export const USER_FILE_PATH_URL = 'user/file/file-path';
export const USER_FILE_GET_ACCESS_URL = 'user/file/all-access-of';
export const USER_REVOKE_ACCESS_URL = 'user/file/revoke';

export interface UserFileAccess {
  username: string;
  fileAccess: string;
}

@Injectable({
  providedIn: 'root'
})

export class StubUserFileService {
  public testUFAs: UserFileAccess[] = [];
  private userFiles: DashboardUserFileEntry[] = [];
  private userFilesChanged = new Subject<null>();

  constructor(
    private http: HttpClient
  ) {
    this.detectUserChanges();
  }

  /**
   * this function will return the fileArray store in the service.
   * This is required for HTML page since HTML can only loop through collection instead of index number.
   * You can change the UserFile inside the array but do not change the array itself.
   */
  public getUserFiles(): ReadonlyArray<DashboardUserFileEntry> {
    return this.userFiles;
  }

  public getUserFilesChangedEvent(): Observable<null> {
    return of();
  }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getFileArray}
   */
  public refreshFiles(): void {
    return;
  }

  /**
   * delete the targetFile in the backend.
   * this function will automatically refresh the files in the service when succeed.
   * @param targetFile
   */
  public deleteFile(targetFile: DashboardUserFileEntry): void {
    return;
  }

  /**
   * convert the input file size to the human readable size by adding the unit at the end.
   * eg. 2048 -> 2.0 KB
   * @param fileSize
   */
  public addFileSizeUnit(fileSize: number): string {
    return 'lala';
  }

  public grantAccess(file: DashboardUserFileEntry, username: string, accessLevel: string): Observable<Response> {
    return of();
  }

  public getSharedAccessesOfFile(file: DashboardUserFileEntry): Observable<Readonly<UserFileAccess>[]> {
    return of(this.testUFAs);
  }

  public revokeFileAccess(file: DashboardUserFileEntry, username: string): Observable<Response> {
    return of();
  }

  private fetchFileList(): Observable<DashboardUserFileEntry[]> {
    return of(this.userFiles);
  }

  /**
   * refresh the files in the service whenever the user changes.
   */
  private detectUserChanges(): void {
    return;
  }

  private clearUserFile(): void {
    return;
  }
}
