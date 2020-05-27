import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';

import { GenericWebResponse } from '../../../../dashboard/type/generic-web-response';
import { environment } from '../../../../../environments/environment';
import { User } from '../../../type/user';
import { UserDictionaryService } from './user-dictionary.service';
import { ManualDictionary, DictionaryUploadItem } from '../../../type/user-dictionary';
import { UserService } from '../user.service';

const postDictUrl = 'user/dictionary/upload';
const putManualDictUrl = 'user/dictionary/upload-manual-dict';

@Injectable({
  providedIn: 'root'
})
export class UserDictionaryUploadService {
  public manualDictionary: ManualDictionary = this.createEmptyManualDictionary();
  private dictionaryUploadItemArray: DictionaryUploadItem[] = [];

  constructor(
    private userService: UserService,
    private userDictionaryService: UserDictionaryService,
    private http: HttpClient
    ) {
      this.detectUserChanges();
  }

  /**
   * this function will return the dictionaryArray store in the service.
   * This is required for HTML page since HTML can only loop through collection instead of index number.
   * Be carefully with the return array because it may cause unexpected error.
   * You can change the DictionaryUploadItem inside the array but do not change the array itself.
   */
  public getDictionaryArray(): DictionaryUploadItem[] {
    return this.dictionaryUploadItemArray;
  }

  public getDictionaryArrayLength(): number {
    return this.dictionaryUploadItemArray.length;
  }

  /**
   * return the DictionaryUploadItem at the index.
   * check the array length by calling function {@link getDictionaryArrayLength}.
   * @param index
   */
  public getDictionaryUploadItem(index: number): DictionaryUploadItem {
    if (index >= this.getDictionaryArrayLength()) {throw new Error('Index out of bound'); }
    return this.dictionaryUploadItemArray[index];
  }

  /**
   * check if the manual dictionary inside the service is valid for uploading.
   * eg. name and content is not empty.
   */
  public isManualDictionaryValid(): boolean {
    return this.manualDictionary.name !== '' && this.manualDictionary.content !== '';
  }

  /**
   * check if this item is valid for uploading.
   * eg. the type is text and the name is unique
   * @param dictionaryUploadItem
   */
  public isItemValid(dictionaryUploadItem: DictionaryUploadItem): boolean {
    return dictionaryUploadItem.file.type.includes('text/plain') && this.isItemNameUnique(dictionaryUploadItem);
  }

  public isItemNameUnique(dictionaryUploadItem: DictionaryUploadItem): boolean {
    return this.dictionaryUploadItemArray
      .filter(item => item.name === dictionaryUploadItem.name)
      .length === 1;
  }

  /**
   * check if all the item in the service is valid so that we can upload them.
   */
  public isAllItemsValid(): boolean {
    return this.dictionaryUploadItemArray.every(
      dictionaryUploadItem => this.isItemValid(dictionaryUploadItem)
    );
  }

  /**
   * insert new file into the upload service.
   * @param file
   */
  public insertNewDictionary(file: File): void {
    this.dictionaryUploadItemArray.push(this.createDictionaryUploadItem(file));
  }

  public deleteDictionary(dictionaryUploadItem: DictionaryUploadItem): void {
    this.dictionaryUploadItemArray = this.dictionaryUploadItemArray.filter(
      dict => dict !== dictionaryUploadItem
    );
  }

  /**
   * upload all the dictionaries in this service and then clear it.
   * This method will automatically refresh the user-dictionary serivce when any dictionaries finish uploading.
   * This method will not upload manual dictionary.
   */
  public uploadAllDictionary() {
    this.dictionaryUploadItemArray.forEach(
      dictionaryUploadItem => this.uploadDictionary(dictionaryUploadItem).subscribe(
        () => {
          this.userDictionaryService.refreshDictionary();
          this.deleteDictionary(dictionaryUploadItem);
        }, error => {
          // TODO: user friendly error message.
          console.log(error);
          alert(`Error encountered: ${error.status}\nMessage: ${error.message}`);
        }
      )
    );
  }

  /**
   * upload the manual dictionary to the backend.
   * This method will automatically refresh the user-dictionary service when succeed.
   */
  public uploadManualDictionary(): void {
    if (!this.userService.isLogin()) {throw new Error(`Can not upload manual dictionary when not login`); }
    if (!this.isManualDictionaryValid) {throw new Error(`Can not upload invalid manual dictionary`); }

    if (this.manualDictionary.separator === '') { this.manualDictionary.separator = ','; }
    this.putManualDictionaryHttpRequest(this.manualDictionary, (this.userService.getUser() as User).userID)
      .subscribe(
        () => {
          this.manualDictionary = this.createEmptyManualDictionary();
          this.userDictionaryService.refreshDictionary();
        }, error => {
          // TODO: user friendly error message.
          console.log(error);
          alert(`Error encountered: ${error.status}\nMessage: ${error.message}`);
        }
      );
  }

  private putManualDictionaryHttpRequest(manualDictionary: ManualDictionary, userID: number): Observable<GenericWebResponse> {
    return this.http.put<GenericWebResponse>(
      `${environment.apiUrl}/${putManualDictUrl}/${userID}`,
      JSON.stringify(manualDictionary),
      {
        headers: new HttpHeaders({
          'Content-Type':  'application/json',
        })
      }
    );
  }

  /**
   * helper function for the {@link uploadAllDictionary}.
   * It will pack the dictionaryUploadItem into formData and upload it to the backend.
   * @param dictionaryUploadItem
   */
  private uploadDictionary(dictionaryUploadItem: DictionaryUploadItem): Observable<GenericWebResponse> {
    if (!this.userService.isLogin()) {
      throw new Error(`Can not upload files when not login`);
    }
    const formData: FormData = new FormData();
    formData.append('file', dictionaryUploadItem.file, dictionaryUploadItem.name);
    formData.append('description', dictionaryUploadItem.description);
    return this.postDictionaryHttpRequest(formData, (this.userService.getUser() as User).userID);
  }

  private postDictionaryHttpRequest(formData: FormData, userID: number): Observable<GenericWebResponse> {
    return this.http.post<GenericWebResponse>(
      `${environment.apiUrl}/${postDictUrl}/${userID}`,
      formData
      );
  }


  /**
   * clear the dictionaries in the service when user log out.
   */
  private detectUserChanges(): void {
    this.userService.getUserChangedEvent().subscribe(
      () => {
        if (!this.userService.isLogin()) {
          this.clearUserDictionary();
        }
      }
    );
  }

  private clearUserDictionary(): void {
    this.dictionaryUploadItemArray = [];
    this.manualDictionary = this.createEmptyManualDictionary();
  }

  private createEmptyManualDictionary(): ManualDictionary {
    return {
      name : '',
      content: '',
      separator: '',
      description: ''
    };
  }

  private createDictionaryUploadItem(file: File): DictionaryUploadItem {
    return {
      file : file,
      name: file.name,
      description: ''
    };
  }
}
