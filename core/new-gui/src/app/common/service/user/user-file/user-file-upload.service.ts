import { AppSettings } from '../../../app-setting';
import { Injectable } from '@angular/core';
import { FileUploadItem } from '../../../../dashboard/type/user-file';
import { GenericWebResponse } from '../../../../dashboard/type/generic-web-response';
import { Observable } from 'rxjs';
import { UserService } from '../user.service';
import { HttpClient, HttpEventType, HttpResponse, HttpEvent } from '@angular/common/http';
import { environment } from '../../../../../environments/environment';
import { UserFileService } from './user-file.service';

export const USER_FILE_UPLOAD_URL = 'user/file/upload';
export const USER_FILE_VALIDATE_URL = 'user/file/validate';

@Injectable({
  providedIn: 'root'
})
export class UserFileUploadService {
  private fileUploadItemArray: FileUploadItem[] = [];

  constructor(
    private userService: UserService,
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

  /**
   * return the FileUploadItem field at the index.
   * check the array length by calling function {@link getFileArrayLength}.
   * @param index
   */
  public getFileUploadItem(index: number): FileUploadItem {
    if (index >= this.getFileArray().length) { throw new Error('index out of bound'); }
    return this.fileUploadItemArray[index];
  }

  public deleteFile(fileUploadItem: FileUploadItem): void {
    this.fileUploadItemArray = this.fileUploadItemArray.filter(
      file => file !== fileUploadItem
    );
  }

  public isAllFilesUploading(): boolean {
    return this.fileUploadItemArray.every(fileUploadItem => fileUploadItem.isUploadingFlag);
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
   * upload all the files in this service and then clear it.
   * This method will automatically refresh the user-file service when any files finish uploading.
   */
  public uploadAllFiles(): void {
    this.fileUploadItemArray.filter(fileUploadItem => !fileUploadItem.isUploadingFlag)
      .forEach(
        fileUploadItem => this.validateAndUploadFile(fileUploadItem).subscribe(
          (response) => {
            if (response.code === 0) {
              this.deleteFile(fileUploadItem);
              this.userFileService.refreshFiles();
            } else {
              // TODO: provide user friendly error message
              console.log(response);
              alert(`Uploading file ${fileUploadItem.name} failed\nMessage: ${response.message}`);
            }
          }
        )
      );
  }

  private validateAndUploadFile(fileUploadItem: FileUploadItem): Observable<GenericWebResponse> {
    const formData: FormData = new FormData();
    formData.append('name', fileUploadItem.name);

    return this.fileValidationHttpRequest(formData).flatMap(
      res => {
        if (res.code === 0) {
          return this.uploadFile(fileUploadItem);
        } else {
          return Observable.of(res);
        }
      }
    );
  }

  private fileValidationHttpRequest(formData: FormData): Observable<GenericWebResponse> {
    return this.http.post<GenericWebResponse>(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_VALIDATE_URL}`,
      formData
      );
  }

  /**
   * helper function for the {@link uploadAllFiles}.
   * It will pack the FileUploadItem into formData and upload it to the backend.
   * @param fileUploadItem
   */
  private uploadFile(fileUploadItem: FileUploadItem): Observable<GenericWebResponse> {
    if (!this.userService.isLogin()) {throw new Error(`Can not upload files when not login`); }
    if (fileUploadItem.isUploadingFlag) {throw new Error(`File ${fileUploadItem.file.name} is already uploading`); }

    fileUploadItem.isUploadingFlag = true;
    const formData: FormData = new FormData();
    formData.append('file', fileUploadItem.file, fileUploadItem.name);
    formData.append('size', fileUploadItem.file.size.toString());
    formData.append('description', fileUploadItem.description);

    return this.retrieveFileUploadProgress(
      this.uploadFileHttpRequest(formData),
      fileUploadItem);
  }

  private uploadFileHttpRequest(formData: FormData): Observable<HttpEvent<GenericWebResponse>> {
    return this.http.post<GenericWebResponse>(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_UPLOAD_URL}`,
      formData,
      {reportProgress: true, observe: 'events'}
      );
  }

  /**
   * retrieve the file upload progress and set it to the fileUploadItem.
   * filter out the progress response and convert the result into GenericWebResponse
   * @param responseObservable
   * @param fileUploadItem
   */
  private retrieveFileUploadProgress(responseObservable: Observable<HttpEvent<GenericWebResponse>>, fileUploadItem: FileUploadItem) {
    return responseObservable
    .filter(event => { // retrieve and remove upload progress
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
    this.userService.getUserChangedEvent().subscribe(
      () => {
        if (!this.userService.isLogin()) {
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
