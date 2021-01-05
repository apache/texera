import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { AppSettings } from '../../../app-setting';

import { GenericWebResponse } from '../../../type/generic-web-response';
import { UserFile } from '../../../type/user-file';
import { UserService } from '../user.service';

export const USER_FILE_LIST_URL = 'user/file/list';
export const USER_FILE_DELETE_URL = 'user/file/delete';

@Injectable({
  providedIn: 'root'
})
export class UserFileService {
  private userFiles: UserFile[] | undefined;
  private userFilesChanged = new Subject<ReadonlyArray<UserFile> | undefined>();

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
  public getUserFiles(): ReadonlyArray<UserFile> | undefined {
    return this.userFiles;
  }

  public getUserFilesChangedEvent(): Observable<ReadonlyArray<UserFile> | undefined> {
    return this.userFilesChanged.asObservable();
  }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getFileArray}
   */
  public refreshFiles(): void {
    if (!this.userService.isLogin()) {
      return;
    }

    this.getFilesHttpRequest().subscribe(
      files => {
        this.userFiles = files;
        this.userFilesChanged.next(this.userFiles);
      }
    );
  }

  /**
   * delete the targetFile in the backend.
   * this function will automatically refresh the files in the service when succeed.
   * @param targetFile
   */
  public deleteFile(targetFile: UserFile): void {
    this.deleteFileHttpRequest(targetFile.id).subscribe(
      () => this.refreshFiles()
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

  private deleteFileHttpRequest(fileID: number): Observable<GenericWebResponse> {
    return this.http.delete<GenericWebResponse>(`${AppSettings.getApiEndpoint()}/${USER_FILE_DELETE_URL}/${fileID}`);
  }

  private getFilesHttpRequest(): Observable<UserFile[]> {
    return this.http.get<UserFile[]>(`${AppSettings.getApiEndpoint()}/${USER_FILE_LIST_URL}`);
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
    this.userFilesChanged.next(this.userFiles);
  }

}
