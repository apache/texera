import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { PublicProject } from "../../../type/dashboard-project.interface";

export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/public/project`;

@Injectable({
  providedIn: "root",
})
export class PublicProjectService {
  constructor(private http: HttpClient) {}

  public getType(pid: number): Observable<string> {
    return this.http.get(`${USER_BASE_URL}/type/${pid}`, { responseType: "text" });
  }

  public makePublic(pid: number): Observable<void> {
    return this.http.put<void>(`${USER_BASE_URL}/public/${pid}`, null);
  }

  public makePrivate(pid: number): Observable<void> {
    return this.http.put<void>(`${USER_BASE_URL}/private/${pid}`, null);
  }

  public getPublicProjects(): Observable<PublicProject[]> {
    return this.http.get<PublicProject[]>(`${USER_BASE_URL}/list`);
  }

  public addPublicProjects(CheckedId: number[]): Observable<void> {
    return this.http.put<void>(`${USER_BASE_URL}/add`, CheckedId);
  }
}
