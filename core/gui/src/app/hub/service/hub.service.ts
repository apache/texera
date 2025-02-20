import { HttpClient, HttpHeaders, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../common/app-setting";
import { SearchResultItem } from "../../dashboard/type/search-result";

export const WORKFLOW_BASE_URL = `${AppSettings.getApiEndpoint()}/workflow`;

@Injectable({
  providedIn: "root",
})
export class HubService {
  readonly BASE_URL: string = `${AppSettings.getApiEndpoint()}/hub`;

  constructor(private http: HttpClient) {}

  public getCount(entityType: string): Observable<number> {
    return this.http.get<number>(`${this.BASE_URL}/count`, {
      params: { entityType: entityType },
    });
  }

  public cloneWorkflow(wid: number): Observable<number> {
    return this.http.post<number>(`${WORKFLOW_BASE_URL}/clone/${wid}`, null);
  }

  public isLiked(entityId: number, userId: number, entityType: string): Observable<boolean> {
    return this.http.get<boolean>(`${this.BASE_URL}/isLiked`, {
      params: { workflowId: entityId.toString(), userId: userId.toString(), entityType },
    });
  }

  public postLike(entityId: number, userId: number, entityType: string): Observable<boolean> {
    const body = { entityId, userId, entityType };
    return this.http.post<boolean>(`${this.BASE_URL}/like`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  public postUnlike(entityId: number, userId: number, entityType: string): Observable<boolean> {
    const body = { entityId, userId, entityType };
    return this.http.post<boolean>(`${this.BASE_URL}/unlike`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  public getLikeCount(entityId: number, entityType: string): Observable<number> {
    const params = new HttpParams().set("entityId", entityId.toString()).set("entityType", entityType);

    return this.http.get<number>(`${this.BASE_URL}/likeCount`, { params });
  }

  public getCloneCount(entityId: number, entityType: string): Observable<number> {
    const params = new HttpParams().set("entityId", entityId.toString()).set("entityType", entityType);

    return this.http.get<number>(`${this.BASE_URL}/cloneCount`, { params });
  }

  public postView(entityId: number, userId: number, entityType: string): Observable<number> {
    const body = { entityId, userId, entityType };
    return this.http.post<number>(`${this.BASE_URL}/view`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  public getViewCount(entityId: number, entityType: string): Observable<number> {
    const params = new HttpParams().set("entityId", entityId.toString()).set("entityType", entityType);

    return this.http.get<number>(`${this.BASE_URL}/viewCount`, { params });
  }

  public getTops(entityType: string, actionType: string, currentUid?: number): Observable<SearchResultItem[]> {
    const params: any = {
      entityType,
      actionType,
    };

    params.uid = currentUid !== undefined ? currentUid : -1;

    return this.http.get<SearchResultItem[]>(`${this.BASE_URL}/getTops`, { params });
  }
}
