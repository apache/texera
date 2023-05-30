import { HttpClient, HttpEventType } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { FileUploadItem } from "../../type/dashboard-file.interface";
import { UserService } from "../../../../common/service/user/user.service";
import { UserFileService } from "./user-file.service";
import { filter, map } from "rxjs/operators";

@Injectable({
  providedIn: "root",
})
export class UserFileUploadService {
  // files a user added to the upload list,
  // these files won't be uploaded until the user hits the "upload" button
  private filesToBeUploaded: FileUploadItem[] = [];

  constructor(private userService: UserService, private userFileService: UserFileService, private http: HttpClient) {}

  private static createFileUploadItem(file: File): FileUploadItem {
    return {
      file: file,
      name: file.name,
      description: "",
      uploadProgress: 0,
      isUploadingFlag: false,
    };
  }

  /**
   * returns all pending files to be uploaded.
   */
  public getFilesToBeUploaded(): ReadonlyArray<Readonly<FileUploadItem>> {
    return this.filesToBeUploaded;
  }

  /**
   * adds new file into the "to be uploaded" file array.
   * @param file
   */
  public addFileToUploadArray(file: File): void {
    this.filesToBeUploaded.push(UserFileUploadService.createFileUploadItem(file));
  }

  /**
   * removes a file from the "to be uploaded" file array.
   */
  public removeFileFromUploadArray(fileUploadItem: FileUploadItem): void {
    this.filesToBeUploaded = this.filesToBeUploaded.filter(file => file !== fileUploadItem);
  }

  /**
   * upload all the files in this service and then clear it.
   */
  public uploadAllFiles(): void {
    this.filesToBeUploaded
      .filter(fileUploadItem => !fileUploadItem.isUploadingFlag)
      .forEach((fileUploadItem: FileUploadItem) =>
        this.uploadFile(fileUploadItem).subscribe(
          () => {
            this.removeFileFromUploadArray(fileUploadItem);
          },
          (err: unknown) => {
            alert(
              // @ts-ignore // TODO: fix this with notification component
              `Uploading file ${fileUploadItem.name} failed\nMessage: ${err.error}`
            );
          }
        )
      );
  }

  /**
   * helper function for the {@link uploadAllFiles}.
   * It will pack the FileUploadItem into formData and upload it to the backend.
   * @param fileUploadItem
   */
  private uploadFile(fileUploadItem: FileUploadItem): Observable<Response> {
    if (!this.userService.isLogin()) {
      throw new Error("Can not upload files when not login");
    }
    if (fileUploadItem.isUploadingFlag) {
      throw new Error(`File ${fileUploadItem.file.name} is already uploading`);
    }

    fileUploadItem.isUploadingFlag = true;
    const formData: FormData = new FormData();
    formData.append("file", fileUploadItem.file);
    formData.append("name", fileUploadItem.name);

    return this.http
      .post<Response>(`${AppSettings.getApiEndpoint()}/user/file/upload`, formData, {
        reportProgress: true,
        observe: "events",
      })
      .pipe(
        filter(event => {
          // retrieve and remove upload progress
          if (event.type === HttpEventType.UploadProgress) {
            fileUploadItem.uploadProgress = event.loaded;
            const total = event.total ? event.total : fileUploadItem.file.size;
            // TODO the upload progress does not fit the speed user feel, it seems faster
            // TODO show progress in user friendly way
            console.log(`File ${fileUploadItem.name} is ${((100 * event.loaded) / total).toFixed(1)}% uploaded.`);
            return false;
          }
          return event.type === HttpEventType.Response;
        }),
        map(event => {
          // convert the type HttpEvent<GenericWebResponse> into GenericWebResponse
          if (event.type === HttpEventType.Response) {
            fileUploadItem.isUploadingFlag = false;
            return event.body as Response;
          } else {
            throw new Error(
              `Error Http Event type in uploading file ${fileUploadItem.name}, the event type is ${event.type}`
            );
          }
        })
      );
  }
}
