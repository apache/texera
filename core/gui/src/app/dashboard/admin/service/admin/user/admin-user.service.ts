import { HttpClient, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../../common/app-setting";
import { Role, User, File, Workflow, MongoExecution } from "../../../../../common/type/user";

export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/user`;
export const USER_LIST_URL = `${USER_BASE_URL}/list`;
export const USER_UPDATE_URL = `${USER_BASE_URL}/update`;
export const USER_ADD_URL = `${USER_BASE_URL}/add`;
export const USER_CREATED_FILES = `${USER_BASE_URL}/uploaded_files`;
export const USER_UPLOADED_DATASE_SIZE = `${USER_BASE_URL}/dataset_size`;
export const USER_UPLOADED_DATASET_COUNT = `${USER_BASE_URL}/uploaded_dataset`;
export const USER_CREATED_WORKFLOWS = `${USER_BASE_URL}/created_workflows`;
export const USER_ACCESS_WORKFLOWS = `${USER_BASE_URL}/access_workflows`;
export const USER_ACCESS_FILES = `${USER_BASE_URL}/access_files`;
export const USER_MONGODB_SIZE = `${USER_BASE_URL}/mongodb_size`;
export const USER_DELETE_MONGODB_COLLECTION_NAME = `${USER_BASE_URL}/deleteCollection`;

@Injectable({
  providedIn: "root",
})
export class AdminUserService {
  constructor(private http: HttpClient) {}

  public getUserList(): Observable<ReadonlyArray<User>> {
    return this.http.get<ReadonlyArray<User>>(`${USER_LIST_URL}`);
  }

  public updateUser(uid: number, name: string, email: string, role: Role): Observable<void> {
    return this.http.put<void>(`${USER_UPDATE_URL}`, {
      uid: uid,
      name: name,
      email: email,
      role: role,
    });
  }

  public addUser(): Observable<Response> {
    return this.http.post<Response>(`${USER_ADD_URL}/`, {});
  }

  public getUploadedFiles(uid: number): Observable<ReadonlyArray<File>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<File>>(`${USER_CREATED_FILES}`, { params: params });
  }

  public getTotalUploadedDatasetSize(uid: number): Observable<number> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<number>(`${USER_UPLOADED_DATASE_SIZE}`, { params: params });
  }

  public getTotalUploadedDatasetCount(uid: number): Observable<number> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<number>(`${USER_UPLOADED_DATASET_COUNT}`, { params: params });
  }

  public getCreatedWorkflows(uid: number): Observable<ReadonlyArray<Workflow>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<Workflow>>(`${USER_CREATED_WORKFLOWS}`, { params: params });
  }

  public getAccessFiles(uid: number): Observable<ReadonlyArray<number>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<number>>(`${USER_ACCESS_FILES}`, { params: params });
  }

  public getAccessWorkflows(uid: number): Observable<ReadonlyArray<number>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<number>>(`${USER_ACCESS_WORKFLOWS}`, { params: params });
  }

  public getMongoDBs(uid: number): Observable<ReadonlyArray<MongoExecution>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<MongoExecution>>(`${USER_MONGODB_SIZE}`, { params: params });
  }

  public deleteMongoDBCollection(collectionName: string): Observable<void> {
    return this.http.delete<void>(`${USER_DELETE_MONGODB_COLLECTION_NAME}/${collectionName}`);
  }
}
