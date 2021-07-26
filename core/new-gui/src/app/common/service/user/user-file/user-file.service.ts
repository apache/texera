import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { AppSettings } from '../../../app-setting';
import { UserFile } from '../../../type/user-file';
import { UserService } from '../user.service';

export const USER_FILE_LIST_URL = 'user/file/list';
export const USER_FILE_DELETE_URL = 'user/file/delete';
export const USER_FILE_SHARE_ACCESS_URL = 'user-file-access/grant';
export const USER_FILE_ACCESS_LIST_URL = 'user-file-access/list';
export const USER_REVOKE_ACCESS_URL = 'user-file-access/revoke';

export interface UserFileAccess {
  username: string;
  fileAccess: string;
}

@Injectable({
  providedIn: 'root'
})

export class UserFileService {
  private userFiles: UserFile[] = [];
  private userFilesChanged = new Subject<null>();


  constructor(
    private http: HttpClient,
    private userService: UserService
  ) {
    this.detectUserChanges();
  }

  /**
   * this function will return the fileArray store in the service.
   * This is required for HTML page since HTML can only loop through collection instead of index number.
   * You can change the UserFile inside the array but do not change the array itself.
   */
  public getUserFiles(): ReadonlyArray<UserFile> {
    return this.userFiles;
  }

  public getUserFilesChangedEvent(): Observable<null> {
    return this.userFilesChanged.asObservable();
  }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getFileArray}
   */
  public refreshFiles(): void {
    if (!this.userService.isLogin()) {
      this.clearUserFile();
      return;
    }

    this.fetchFileList().subscribe(
      files => {
        this.userFiles = files;
        this.userFilesChanged.next();
      }
    );
  }

  /**
   * delete the targetFile in the backend.
   * this function will automatically refresh the files in the service when succeed.
   * @param targetFile
   */
  public deleteFile(targetFile: UserFile): void {
    console.log(targetFile)
    this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${USER_FILE_DELETE_URL}/${targetFile.fileName}/${targetFile.ownerName}`).subscribe(
      () => this.refreshFiles(),
      err => alert('Can\'t delete the file: ' + err.error)
    );
  }

  /**
   * convert the input file size to the human readable size by adding the unit at the end.
   * eg. 2048 -> 2.0 KB
   * @param fileSize
   */
  public addFileSizeUnit(fileSize: number): string {
    if (fileSize <= 1024) {
      return fileSize + ' Byte';
    }

    let i = 0;
    const byteUnits = [' Byte', ' KB', ' MB', ' GB', ' TB', ' PB', ' EB', ' ZB', ' YB'];
    while (fileSize > 1024 && i < byteUnits.length - 1) {
      fileSize = fileSize / 1024;
      i++;
    }
    return Math.max(fileSize, 0.1).toFixed(1) + byteUnits[i];
  }

  /**
   * Assign a new access to/Modify an existing access of another user
   * @param file the file that is about to be shared
   * @param username the username of target user
   * @param accessLevel the type of access offered
   * @return Response
   */
  public grantAccess(file: UserFile, username: string, accessLevel: string): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_SHARE_ACCESS_URL}/${file.fileName}/${file.ownerName}/${username}/${accessLevel}`,
      null);
  }

  /**
   * Retrieve all shared accesses of the given workflow
   * @param file the current file
   * @return Readonly<UserFileAccess>[] an array of UserFileAccesses, Ex: [{username: TestUser, fileAccess: read}]
   */
  public getSharedAccessesOfFile(file: UserFile): Observable<Readonly<UserFileAccess>[]> {
    return this.http.get<Readonly<UserFileAccess>[]>(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_ACCESS_LIST_URL}/${file.fileName}/${file.ownerName}`);
  }

  /**
   * Remove an existing access of another user
   * @param file the current file
   * @param username the username of target user
   * @return message of success
   */
  public revokeFileAccess(file: UserFile, username: string): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${USER_REVOKE_ACCESS_URL}/${file.fileName}/${file.ownerName}/${username}`, null);
  }

  private fetchFileList(): Observable<UserFile[]> {
    return this.http.get<UserFile[]>(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_LIST_URL}`);
  }

  /**
   * refresh the files in the service whenever the user changes.
   */
  private detectUserChanges(): void {
    this.userService.userChanged().subscribe(
      () => {
        if (this.userService.isLogin()) {
          this.refreshFiles();
        } else {
          this.clearUserFile();
        }
      }
    );
  }

  private clearUserFile(): void {
    this.userFiles = [];
    this.userFilesChanged.next();
  }
}
