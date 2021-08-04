import { Injectable } from '@angular/core';
import { Observable, of, Subject } from 'rxjs';
import { DashboardUserFileEntry, UserFileAccess } from '../../../type/dashboard-user-file-entry';
import { PublicInterfaceOf } from '../../../util/stub';
import { UserFileService } from './user-file.service';
import { HttpClient } from '@angular/common/http';
import { StubUserService } from '../stub-user.service';

@Injectable({
  providedIn: 'root'
})

export class StubUserFileService implements PublicInterfaceOf<UserFileService> {
  public testUFAs: UserFileAccess[] = [];
  private userFiles: DashboardUserFileEntry[] = [];
  private userFilesChanged = new Subject<null>();

  constructor(private http: HttpClient,
              private userService: StubUserService) {
    StubUserFileService.detectUserChanges();
  }

  public grantUserFileAccess(file: DashboardUserFileEntry, username: string, accessLevel: string): Observable<Response> {
    return of();
  }

  public getUserFileAccessList(dashboardUserFileEntry: DashboardUserFileEntry): Observable<readonly UserFileAccess[]> {
    return of();
  }

  public revokeUserFileAccess(dashboardUserFileEntry: DashboardUserFileEntry, username: string): Observable<Response> {
    return of();
  }


  public getUserFilesChangedEvent(): Observable<null> {
    return of();
  }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getFileArray}
   */
  public refreshFiles(): void {
    return;
  }

  /**
   * delete the targetFile in the backend.
   * this function will automatically refresh the files in the service when succeed.
   * @param targetFile
   */
  public deleteFile(targetFile: DashboardUserFileEntry): void {
    return;
  }


  public grantAccess(file: DashboardUserFileEntry, username: string, accessLevel: string): Observable<Response> {
    return of();
  }

  public getSharedAccessesOfFile(file: DashboardUserFileEntry): Observable<Readonly<UserFileAccess>[]> {
    return of(this.testUFAs);
  }

  addFileSizeUnit(fileSize: number): string {
    return '';
  }

  getUserFiles(): ReadonlyArray<DashboardUserFileEntry> {
    return [];
  }

  /**
   * refresh the files in the service whenever the user changes.
   */
  private static detectUserChanges(): void {
    return;
  }


}
