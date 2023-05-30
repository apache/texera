import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { DashboardFile } from "../../type/dashboard-file.interface";
export const USER_FILE_BASE_URL = `${AppSettings.getApiEndpoint()}/user/file`;

@Injectable({
  providedIn: "root",
})
export class UserFileService {
  constructor(private http: HttpClient) {}

  public deleteFile(fid: number): Observable<void> {
    return this.http.delete<void>(`${USER_FILE_BASE_URL}/delete/${fid}`);
  }

  public downloadFile(fid: number): Observable<Blob> {
    return this.http.get(`${USER_FILE_BASE_URL}/download/${fid}`, { responseType: "blob" });
  }

  public changeFileName(fid: number, name: string): Observable<void> {
    return this.http.put<void>(`${USER_FILE_BASE_URL}/name/${fid}/${name}`, null);
  }

  public changeFileDescription(fid: number, description: string): Observable<void> {
    return this.http.put<void>(`${USER_FILE_BASE_URL}/description/${fid}/${description}`, null);
  }

  public getFileList(): Observable<ReadonlyArray<DashboardFile>> {
    return this.http.get<ReadonlyArray<DashboardFile>>(`${USER_FILE_BASE_URL}/list`);
  }

  public getAutoCompleteFileList(query: String): Observable<ReadonlyArray<string>> {
    return this.http.get<ReadonlyArray<string>>(`${USER_FILE_BASE_URL}/autocomplete/${query}`);
  }

  /**
   * convert the input file size to the human readable size by adding the unit at the end.
   * eg. 2048 -> 2.0 KB
   * @param fileSize
   */
  public addFileSizeUnit(fileSize: number): string {
    if (fileSize <= 1024) {
      return fileSize + " Byte";
    }

    let i = 0;
    const byteUnits = [" Byte", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"];
    while (fileSize > 1024 && i < byteUnits.length - 1) {
      fileSize = fileSize / 1024;
      i++;
    }
    return Math.max(fileSize, 0.1).toFixed(1) + byteUnits[i];
  }
}
