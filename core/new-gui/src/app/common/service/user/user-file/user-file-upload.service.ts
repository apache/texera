import { HttpClient, HttpEventType } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { AppSettings } from '../../../app-setting';
import { GenericWebResponse, GenericWebResponseCode } from '../../../type/generic-web-response';
import { FileUploadItem } from '../../../type/user-file';
import { UserService } from '../user.service';
import { UserFileService } from './user-file.service';

export const USER_FILE_UPLOAD_URL = 'user/file/upload';
export const USER_FILE_VALIDATE_URL = 'user/file/validate';

@Injectable({
  providedIn: 'root'
})
export class UserFileUploadService {

  // files a user added to the upload list,
  // these files won't be uploaded until the user hits the "upload" button
  private fileUploadItemArray: FileUploadItem[] = [];

  constructor(
    private userService: UserService,
    private userFileService: UserFileService,
    private http: HttpClient) {
    this.detectUserChanges();
  }

  /**
   * returns all pending files to be uploaded.
   */
  public getFilesToBeUploaded(): ReadonlyArray<Readonly<FileUploadItem>> {
    return this.fileUploadItemArray;
  }

  /**
   * adds new file into the "to be uploaded" file array.
   * @param file
   */
  public addFileToUploadArray(file: File): void {
    this.fileUploadItemArray.push(UserFileUploadService.createFileUploadItem(file));
  }

  /**
   * removes a file from the "to be uploaded" file array.
   */
  public removeFileFromUploadArray(fileUploadItem: FileUploadItem): void {
    this.fileUploadItemArray = this.fileUploadItemArray.filter(
      file => file !== fileUploadItem
    );
  }

  /**
   * upload all the files in this service and then clear it.
   * This method will automatically refresh the user-file service when any files finish uploading.
   */
  public uploadAllFiles(): void {
    this.fileUploadItemArray.filter(fileUploadItem => !fileUploadItem.isUploadingFlag).forEach(
      fileUploadItem => this.validateAndUploadFile(fileUploadItem).subscribe((response) => {
        if (response.code === GenericWebResponseCode.SUCCESS) {
          this.removeFileFromUploadArray(fileUploadItem);
          this.userFileService.refreshFiles();
        } else {
          // TODO: user friendly error message.
          alert(`Uploading file ${fileUploadItem.name} failed\nMessage: ${response.message}`);
        }
      })
    );
  }

  private validateAndUploadFile(fileUploadItem: FileUploadItem): Observable<GenericWebResponse> {
    const formData: FormData = new FormData();
    formData.append('name', fileUploadItem.name);

    return this.http.post<GenericWebResponse>(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_VALIDATE_URL}`, formData).flatMap(
      res => {
        if (res.code === GenericWebResponseCode.SUCCESS) {
          return this.uploadFile(fileUploadItem);
        } else {
          return Observable.of(res);
        }
      }
    );
  }

  /**
   * helper function for the {@link uploadAllFiles}.
   * It will pack the FileUploadItem into formData and upload it to the backend.
   * @param fileUploadItem
   */
  private uploadFile(fileUploadItem: FileUploadItem): Observable<GenericWebResponse> {
    if (!this.userService.isLogin()) {
      throw new Error(`Can not upload files when not login`);
    }
    if (fileUploadItem.isUploadingFlag) {
      throw new Error(`File ${fileUploadItem.file.name} is already uploading`);
    }

    fileUploadItem.isUploadingFlag = true;
    const formData: FormData = new FormData();
    formData.append('file', fileUploadItem.file, fileUploadItem.name);
    formData.append('size', fileUploadItem.file.size.toString());
    formData.append('description', fileUploadItem.description);

    return this.http.post<GenericWebResponse>(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_UPLOAD_URL}`,
      formData, {reportProgress: true, observe: 'events'}
    ).filter(event => { // retrieve and remove upload progress
      if (event.type === HttpEventType.UploadProgress) {
        fileUploadItem.uploadProgress = event.loaded;
        const total = event.total ? event.total : fileUploadItem.file.size;
        // TODO the upload progress does not fit the speed user feel, it seems faster
        // TODO show progress in user friendly way
        console.log(`File ${fileUploadItem.name} is ${(100 * event.loaded / total).toFixed(1)}% uploaded.`);
        return false;
      }
      return event.type === HttpEventType.Response;
    }).map(event => { // convert the type HttpEvent<GenericWebResponse> into GenericWebResponse
      if (event.type === HttpEventType.Response) {
        fileUploadItem.isUploadingFlag = false;
        return (event.body as GenericWebResponse);
      } else {
        throw new Error(`Error Http Event type in uploading file ${fileUploadItem.name}, the event type is ${event.type}`);
      }
    });
  }

  /**
   * clear the files in the service when user log out.
   */
  private detectUserChanges(): void {
    this.userService.userChanged().subscribe(() => {
      if (!this.userService.isLogin()) {
        this.clearUserFile();
      }
    });
  }

  private clearUserFile(): void {
    this.fileUploadItemArray = [];
  }

  private static createFileUploadItem(file: File): FileUploadItem {
    return {
      file: file,
      name: file.name,
      description: '',
      uploadProgress: 0,
      isUploadingFlag: false
    };
  }
}
