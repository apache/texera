import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { DashboardWorkflowEntry } from "../../type/dashboard-workflow-entry";
import { DashboardUserFileEntry } from "../../type/dashboard-user-file-entry";
import { UserProject } from "../../type/user-project";
import { UserFileService } from "../user-file/user-file.service";

export const USER_PROJECT_BASE_URL = `${AppSettings.getApiEndpoint()}/project`;
export const USER_PROJECT_LIST_URL = `${USER_PROJECT_BASE_URL}/list`;
export const DELETE_PROJECT_URL = `${USER_PROJECT_BASE_URL}/delete`;
export const CREATE_PROJECT_URL = `${USER_PROJECT_BASE_URL}/create`;

export const USER_FILE_BASE_URL = `${AppSettings.getApiEndpoint()}/user/file`;
export const USER_FILE_DELETE_URL = `${USER_FILE_BASE_URL}/delete`;

@Injectable({
  providedIn: "root",
})
export class UserProjectService {
  private files: ReadonlyArray<DashboardUserFileEntry> = [];

  constructor(private http: HttpClient, private userFileService: UserFileService) {}

  public retrieveProjectList(): Observable<UserProject[]> {
    return this.http.get<UserProject[]>(`${USER_PROJECT_LIST_URL}`);
  }

  public retrieveWorkflowsOfProject(pid: number): Observable<DashboardWorkflowEntry[]> {
    return this.http.get<DashboardWorkflowEntry[]>(`${USER_PROJECT_BASE_URL}/${pid}/workflows`);
  }

  public retrieveFilesOfProject(pid: number): Observable<DashboardUserFileEntry[]> {
    return this.http.get<DashboardUserFileEntry[]>(`${USER_PROJECT_BASE_URL}/${pid}/files`);
  }

  public getProjectFiles(): ReadonlyArray<DashboardUserFileEntry> {
    return this.files;
  }

  public refreshFilesOfProject(pid: number): void {
    this.retrieveFilesOfProject(pid).subscribe(files => {
      this.files = files;
    });
  }

  public retrieveProject(pid: number): Observable<UserProject> {
    return this.http.get<UserProject>(`${USER_PROJECT_BASE_URL}/` + pid);
  }

  public updateProjectName(pid: number, name: string): Observable<Response> {
    return this.http.post<Response>(`${USER_PROJECT_BASE_URL}/${pid}/rename/${name}`, {});
  }

  public deleteProject(pid: number): Observable<Response> {
    return this.http.delete<Response>(`${DELETE_PROJECT_URL}/` + pid);
  }

  public createProject(name: string): Observable<UserProject> {
    return this.http.post<UserProject>(`${CREATE_PROJECT_URL}/` + name, {});
  }

  public addWorkflowToProject(pid: number, wid: number): Observable<Response> {
    return this.http.post<Response>(`${USER_PROJECT_BASE_URL}/${pid}/workflow/${wid}/add`, {});
  }

  public removeWorkflowFromProject(pid: number, wid: number): Observable<Response> {
    return this.http.delete<Response>(`${USER_PROJECT_BASE_URL}/${pid}/workflow/${wid}/delete`, {});
  }

  public addFileToProject(pid: number, fid: number): Observable<Response> {
    return this.http.post<Response>(`${USER_PROJECT_BASE_URL}/${pid}/user-file/${fid}/add`, {});
  }

  public updateProjectColor(pid: number, colorHex: string): Observable<Response> {
    return this.http.post<Response>(`${USER_PROJECT_BASE_URL}/${pid}/color/${colorHex}/add`, {});
  }

  public deleteProjectColor(pid: number): Observable<Response> {
    return this.http.post<Response>(`${USER_PROJECT_BASE_URL}/${pid}/color/delete`, {});
  }

  public removeFileFromProject(pid: number, fid: number): Observable<Response> {
    return this.http.delete<Response>(`${USER_PROJECT_BASE_URL}/${pid}/user-file/${fid}/delete`, {});
  }

  /**
   * same as UserFileService"s deleteDashboardUserFileEntry method, except
   * it is modified to refresh the project"s list of files
   */
  public deleteDashboardUserFileEntry(pid: number, targetUserFileEntry: DashboardUserFileEntry): void {
    this.http
      .delete<Response>(`${USER_FILE_DELETE_URL}/${targetUserFileEntry.file.name}/${targetUserFileEntry.ownerName}`)
      .subscribe(
        () => {
          this.refreshFilesOfProject(pid); // refresh files within project
        },
        // @ts-ignore // TODO: fix this with notification component
        (err: unknown) => alert("Cannot delete the file entry: " + err.error)
      );
  }

  /**
   * Helper function to determine if a project color is light
   * or dark, which can be helpful for styling decisions
   *
   * @param color (HEX formatted color string)
   * @returns boolean indicating whether color is "light" or "dark"
   */
  public isLightColor(color: string): boolean {
    if (this.isInvalidColorFormat(color)) {
      return false; // default color is dark
    }

    // ensure format is in 6 digit HEX
    if (color.length == 3) {
      color = color
        .split("")
        .map(s => s + s)
        .join("");
    }

    // convert to RGB form
    let colorRGB: number = +("0x" + color);

    let r: number = colorRGB >> 16;
    let g: number = (colorRGB >> 8) & 255;
    let b: number = colorRGB & 255;

    // estimate HSV value
    let hsv: number = Math.sqrt(0.299 * (r * r) + 0.587 * (g * g) + 0.114 * (b * b));
    return hsv > 200;
  }

  /**
   * Helper function to validate if a project color is in HEX format
   *
   * @param color
   * @returns boolean indicating whether color is in valid HEX format
   */
  public isInvalidColorFormat(color: string) {
    return color == null || (color.length != 6 && color.length != 3) || !/^([0-9A-Fa-f]{3}){1,2}$/.test(color);
  }
}
