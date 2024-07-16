import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { DashboardWorkflow } from "../../../type/dashboard-workflow.interface";
import { DashboardFile } from "../../../type/dashboard-file.interface";
import { DashboardProject } from "../../../type/dashboard-project.interface";
import { NotificationService } from "../../../../common/service/notification/notification.service";

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
  private files: ReadonlyArray<DashboardFile> = [];

  constructor(
    private http: HttpClient,
    private notificationService: NotificationService
  ) {}

  public getProjectList(): Observable<DashboardProject[]> {
    return this.http.get<DashboardProject[]>(`${USER_PROJECT_LIST_URL}`);
  }

  public retrieveWorkflowsOfProject(pid: number): Observable<DashboardWorkflow[]> {
    return this.http.get<DashboardWorkflow[]>(`${USER_PROJECT_BASE_URL}/${pid}/workflows`);
  }

  public retrieveFilesOfProject(pid: number): Observable<DashboardFile[]> {
    return this.http.get<DashboardFile[]>(`${USER_PROJECT_BASE_URL}/${pid}/files`);
  }

  public getProjectFiles(): ReadonlyArray<DashboardFile> {
    return this.files;
  }

  public refreshFilesOfProject(pid: number): void {
    this.retrieveFilesOfProject(pid).subscribe(files => {
      this.files = files;
    });
  }

  public retrieveProject(pid: number): Observable<DashboardProject> {
    return this.http.get<DashboardProject>(`${USER_PROJECT_BASE_URL}/${pid}`);
  }

  public updateProjectName(pid: number, name: string): Observable<Response> {
    return this.http.post<Response>(`${USER_PROJECT_BASE_URL}/${pid}/rename/${name}`, {});
  }

  public updateProjectDescription(pid: number, description: string): Observable<Response> {
    return this.http.post<Response>(`${USER_PROJECT_BASE_URL}/${pid}/update/description`, `${description}`);
  }

  public deleteProject(pid: number): Observable<Response> {
    return this.http.delete<Response>(`${DELETE_PROJECT_URL}/` + pid);
  }

  public createProject(name: string): Observable<DashboardProject> {
    return this.http.post<DashboardProject>(`${CREATE_PROJECT_URL}/` + name, {});
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
  public deleteDashboardUserFileEntry(pid: number, targetUserFileEntry: DashboardFile): void {
    this.http
      .delete<Response>(`${USER_FILE_DELETE_URL}/${targetUserFileEntry.file.name}/${targetUserFileEntry.ownerEmail}`)
      .subscribe({
        next: () => {
          this.refreshFilesOfProject(pid); // refresh files within project
        },
        // @ts-ignore // TODO: fix this with notification component
        error: (err: unknown) => alert("Cannot delete the file entry: " + err.error),
      });
  }

  /**
   * Helper function to determine if a project color is light
   * or dark, which can be helpful for styling decisions
   *
   * @param color (HEX formatted color string)
   * @returns boolean indicating whether color is "light" or "dark"
   */
  public static isLightColor(color: string): boolean {
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
  public static isInvalidColorFormat(color: string) {
    return color == null || (color.length != 6 && color.length != 3) || !/^([0-9A-Fa-f]{3}){1,2}$/.test(color);
  }
}
