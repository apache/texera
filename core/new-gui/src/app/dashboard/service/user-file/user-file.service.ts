import { Injectable, EventEmitter } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../../../environments/environment';

import { GenericWebResponse } from '../../type/generic-web-response';
import { User } from '../../../common/type/user';
import { UserFile } from '../../type/user-file';
import { UserService } from '../../../common/service/user/user.service';

export const getFilesUrl = 'users/files/get-files';
export const deleteFilesUrl = 'users/files/delete-file';

@Injectable()
export class UserFileService {
  private fileArray: UserFile[] = [];
  // TODO file changed event

  constructor(
    private userService: UserService,
    private http: HttpClient
    ) {
      this.detectUserChanges();
      if (this.userService.isLogin()) {
        this.refreshFiles();
      }
  }

  /**
   * this function will return the fileArray store in the service.
   * This is required for HTML page since HTML can only loop through collection instead of index number.
   * You can change the UserFile inside the array but do not change the array itself.
   */
  public getFileArray(): UserFile[] {
    return this.fileArray;
  }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getFileArray} or {@link getFileField}.
   */
  public refreshFiles(): void {
    if (!this.userService.isLogin()) {return; }

    this.getFilesHttpRequest(
      (this.userService.getUser() as User).userID
      ).subscribe(
      files => {
        this.fileArray = files;
        // TODO emit file changed event
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
    if (fileSize <= 1024) { return fileSize + ' Byte'; }

    let i = 0;
    const byteUnits = [' Byte', ' KB', ' MB', ' GB', ' TB', ' PB', ' EB', ' ZB', ' YB'];
    while (fileSize > 1024 && i < byteUnits.length - 1) {
      fileSize = fileSize / 1024;
      i++;
    }
    return Math.max(fileSize, 0.1).toFixed(1) + byteUnits[i];
}

  private deleteFileHttpRequest(fileID: number): Observable<GenericWebResponse> {
    return this.http.delete<GenericWebResponse>(`${environment.apiUrl}/${deleteFilesUrl}/${fileID}`);
  }

  private getFilesHttpRequest(userID: number): Observable<UserFile[]> {
    return this.http.get<UserFile[]>(`${environment.apiUrl}/${getFilesUrl}/${userID}`);
  }

  /**
   * refresh the files in the service whenever the user changes.
   */
  private detectUserChanges(): void {
    this.userService.getUserChangedEvent().subscribe(
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
    this.fileArray = [];
    // TODO emit file changed event
  }

}
