import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { AppSettings } from "src/app/common/app-setting";
import { GenericWebResponse, GenericWebResponseCode } from "../../../common/type/generic-web-response";
import { DictionaryUploadItem, ManualDictionaryUploadItem } from "../../../common/type/user-dictionary";
import { UserService } from "../../../common/service/user/user.service";
import { UserDictionaryService } from "./user-dictionary.service";
import { mergeMap } from "rxjs/operators";

export const USER_DICTIONARY_UPLOAD_URL = "user/dictionary/upload";
export const USER_MANUAL_DICTIONARY_UPLOAD_URL = "user/dictionary/upload-manual-dict";
export const USER_DICTIONARY_VALIDATE_URL = "user/dictionary/validate";

@Injectable({
  providedIn: "root",
})
export class UserDictionaryUploadService {
  private manualDictionary: ManualDictionaryUploadItem = UserDictionaryUploadService.createEmptyManualDictionary();
  private dictionaryUploadItemArray: DictionaryUploadItem[] = [];

  constructor(
    private userService: UserService,
    private userDictionaryService: UserDictionaryService,
    private http: HttpClient
  ) {
    this.detectUserChanges();
  }

  public getDictionariesToBeUploaded(): ReadonlyArray<Readonly<DictionaryUploadItem>> {
    return this.dictionaryUploadItemArray;
  }

  public getManualDictionary(): ManualDictionaryUploadItem {
    return this.manualDictionary;
  }

  /**
   * check if the manual dictionary inside the service is valid for uploading.
   * eg. name and content is not empty.
   */
  public validateManualDictionary(): boolean {
    return this.manualDictionary.name !== "" && this.manualDictionary.content !== "";
  }

  /**
   * check if all the item in the service is valid so that we can upload them.
   */
  public validateAllDictionaryUploadItems(): boolean {
    return this.dictionaryUploadItemArray.every(dictionaryUploadItem =>
      this.validateDictionaryUploadItem(dictionaryUploadItem)
    );
  }

  /**
   * check if this item is valid for uploading.
   * eg. the type is text and the name is unique
   * @param dictionaryUploadItem
   */
  public validateDictionaryUploadItem(dictionaryUploadItem: DictionaryUploadItem): boolean {
    return dictionaryUploadItem.file.type.includes("text/plain") && this.isItemNameUnique(dictionaryUploadItem);
  }

  public isItemNameUnique(dictionaryUploadItem: DictionaryUploadItem): boolean {
    return this.dictionaryUploadItemArray.filter(item => item.name === dictionaryUploadItem.name).length === 1;
  }

  /**
   * insert new file into the upload service.
   * @param file
   */
  public addDictionaryToUploadArray(file: File): void {
    this.dictionaryUploadItemArray.push(UserDictionaryUploadService.createDictionaryUploadItem(file));
  }

  public removeFileFromUploadArray(dictionaryUploadItem: DictionaryUploadItem): void {
    this.dictionaryUploadItemArray = this.dictionaryUploadItemArray.filter(dict => dict !== dictionaryUploadItem);
  }

  /**
   * upload all the dictionaries in this service and then clear it.
   * This method will automatically refresh the user-dictionary serivce when any dictionaries finish uploading.
   * This method will not upload manual dictionary.
   */
  public uploadAllDictionaries() {
    this.dictionaryUploadItemArray
      .filter(dictionaryUploadItem => !dictionaryUploadItem.isUploadingFlag)
      .forEach(dictionaryUploadItem =>
        this.validateAndUploadDictionary(dictionaryUploadItem).subscribe(res => {
          if (res.code === GenericWebResponseCode.SUCCESS) {
            this.removeFileFromUploadArray(dictionaryUploadItem);
            this.userDictionaryService.refreshDictionaries();
          } else {
            dictionaryUploadItem.isUploadingFlag = false;
            // TODO: user friendly error message.
            alert(`Uploading dictionary ${dictionaryUploadItem.name} failed\nMessage: ${res.message}`);
          }
        })
      );
  }

  /**
   * upload the manual dictionary to the backend.
   * This method will automatically refresh the user-dictionary service when succeed.
   */
  public uploadManualDictionary(): void {
    if (!this.userService.isLogin()) {
      throw new Error("Can not upload manual dictionary when not login");
    }
    if (!this.validateManualDictionary()) {
      throw new Error("Can not upload invalid manual dictionary");
    }

    if (this.manualDictionary.separator === "") {
      this.manualDictionary.separator = ",";
    }
    this.manualDictionary.isUploadingFlag = true;

    this.manualDictionaryUploadHttpRequest(this.manualDictionary).subscribe(res => {
      if (res.code === GenericWebResponseCode.SUCCESS) {
        this.manualDictionary = UserDictionaryUploadService.createEmptyManualDictionary();
        this.userDictionaryService.refreshDictionaries();
      } else {
        this.manualDictionary.isUploadingFlag = false;
        // TODO: user friendly error message.
        alert(`Uploading dictionary ${this.manualDictionary.name} failed\nMessage: ${res.message}`);
      }
    });
  }

  private manualDictionaryUploadHttpRequest(
    manualDictionary: ManualDictionaryUploadItem
  ): Observable<GenericWebResponse> {
    return this.http.post<GenericWebResponse>(
      `${AppSettings.getApiEndpoint()}/${USER_MANUAL_DICTIONARY_UPLOAD_URL}`,
      JSON.stringify(manualDictionary),
      {
        headers: new HttpHeaders({
          "Content-Type": "application/json",
        }),
      }
    );
  }

  private validateAndUploadDictionary(dictionaryUploadItem: DictionaryUploadItem): Observable<GenericWebResponse> {
    const formData: FormData = new FormData();
    formData.append("name", dictionaryUploadItem.name);
    formData.append("size", dictionaryUploadItem.file.size.toString());

    return this.http
      .post<GenericWebResponse>(`${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_VALIDATE_URL}`, formData)
      .pipe(
        mergeMap(res => {
          if (res.code === GenericWebResponseCode.SUCCESS) {
            return this.uploadDictionary(dictionaryUploadItem);
          } else {
            return of(res);
          }
        })
      );
  }

  /**
   * helper function for the {@link uploadAllDictionary}.
   * It will pack the dictionaryUploadItem into formData and upload it to the backend.
   * @param dictionaryUploadItem
   */
  private uploadDictionary(dictionaryUploadItem: DictionaryUploadItem): Observable<GenericWebResponse> {
    if (!this.userService.isLogin()) {
      throw new Error("Can not upload dictionary when not login");
    }
    if (dictionaryUploadItem.isUploadingFlag) {
      throw new Error(`Dictionary ${dictionaryUploadItem.file.name} is already uploading`);
    }

    dictionaryUploadItem.isUploadingFlag = true;
    const formData: FormData = new FormData();
    formData.append("file", dictionaryUploadItem.file, dictionaryUploadItem.name);
    formData.append("size", dictionaryUploadItem.file.size.toString());
    formData.append("description", dictionaryUploadItem.description);

    return this.http.post<GenericWebResponse>(
      `${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_UPLOAD_URL}`,
      formData
    );
  }

  /**
   * clear the dictionaries in the service when user log out.
   */
  private detectUserChanges(): void {
    this.userService.userChanged().subscribe(() => {
      if (!this.userService.isLogin()) {
        this.clearUserDictionary();
      }
    });
  }

  private clearUserDictionary(): void {
    this.dictionaryUploadItemArray = [];
    this.manualDictionary = UserDictionaryUploadService.createEmptyManualDictionary();
  }

  private static createEmptyManualDictionary(): ManualDictionaryUploadItem {
    return {
      name: "",
      content: "",
      separator: "",
      description: "",
      isUploadingFlag: false,
    };
  }

  private static createDictionaryUploadItem(file: File): DictionaryUploadItem {
    return {
      file: file,
      name: file.name,
      description: "",
      isUploadingFlag: false,
    };
  }
}
