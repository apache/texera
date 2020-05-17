import { Injectable } from '@angular/core';
import { FileUploadItem } from '../../type/user-file';
import { GenericWebResponse } from '../../type/generic-web-response';
import { Observable } from 'rxjs';
import { UserAccountService } from '../user-account/user-account.service';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../../../environments/environment';
import { UserFileService } from './user-file.service';

export const postFileUrl = 'users/files/upload-file';

@Injectable()
export class UserFileUploadService {
  private fileUploadItemArray: FileUploadItem[] = [];

  constructor(
    private userAccountService: UserAccountService,
    private userFileService: UserFileService,
    private http: HttpClient) {
      this.detectUserChanges();
  }

  /**
   * insert new file into the upload service.
   * @param file
   */
  public insertNewFile(file: File): void {
    this.fileUploadItemArray.push(this.createFileUploadItem(file));
  }


  /**
   * this function will return the fileArray store in the service.
   * This is required for HTML page since HTML can only loop through collection instead of index number.
   * Be carefully with the return array because it may cause unexpected error.
   * You can change the FileUploadItem inside the array but do not change the array itself.
   */
  public getFileArray(): FileUploadItem[] {
    return this.fileUploadItemArray;
  }

  public getFileArrayLength(): number {
    return this.fileUploadItemArray.length;
  }

  /**
   * return the FileUploadItem field at the index.
   * check the array length by calling function {@link getFileArrayLength}.
   * @param index
   */
  public getFileUploadItem(index: number): FileUploadItem {
    if (index >= this.getFileArrayLength()) { throw new Error('index out of bound'); }
    return this.fileUploadItemArray[index];
  }

  public deleteFile(fileUploadItem: FileUploadItem): void {
    this.fileUploadItemArray = this.fileUploadItemArray.filter(
      file => file !== fileUploadItem
    );
  }

  /**
   * upload all the files in this service and then clear it.
   * This method will automatically refresh the user-file service when any files finish uploading.
   */
  public uploadAllFiles(): void {
    this.fileUploadItemArray.forEach(
      fileUploadItem => this.uploadFile(fileUploadItem).subscribe(
        () => {
          this.userFileService.refreshFiles();
          this.deleteFile(fileUploadItem);
        }, error => {
          // TODO: provide user friendly error message
          console.log(error);
          alert(`Error encountered: ${error.status}\nMessage: ${error.message}`);
        }
      )
    );
  }

  /**
   * convert the input file size to the human readable size by adding the unit at the end.
   * eg. 2048 -> 2 kb
   * @param fileSize
   */
  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  /**
   * helper function for the {@link uploadAllFiles}.
   * It will pack the FileUploadItem into formData and upload it to the backend.
   * @param fileUploadItem
   */
  private uploadFile(fileUploadItem: FileUploadItem): Observable<GenericWebResponse> {
    if (!this.userAccountService.isLogin()) {
      throw new Error(`Can not upload files when not login`);
    }
    fileUploadItem.isUploadingFlag = true;
    const formData: FormData = new FormData();
    formData.append('file', fileUploadItem.file, fileUploadItem.name);
    formData.append('size', fileUploadItem.file.size.toString());
    formData.append('description', fileUploadItem.description);
    return this.postFileHttpRequest(formData, this.userAccountService.getUserID());
  }

  private postFileHttpRequest(formData: FormData, userID: number): Observable<GenericWebResponse> {
    return this.http.post<GenericWebResponse>(
      `${environment.apiUrl}/${postFileUrl}/${userID}`,
      formData
      );
  }

  /**
   * clear the files in the service when user log out.
   */
  private detectUserChanges(): void {
    this.userAccountService.getUserChangeEvent().subscribe(
      () => {
        if (!this.userAccountService.isLogin()) {
          this.clearUserFile();
        }
      }
    );
  }

  private clearUserFile(): void {
    this.fileUploadItemArray = [];
  }

  private createFileUploadItem(file: File): FileUploadItem {
    return {
      file: file,
      name: file.name,
      description: '',
      uploadProgress: 0,
      isUploadingFlag: false
    };
  }
}
