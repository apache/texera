import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable } from "rxjs";
import { Clusters } from "../../../dashboard/type/clusters";
import { AppSettings } from "../../app-setting";

@Injectable({
  providedIn: "root",
})
export class ClusterService {
  public CLUSTER_BASE_URL = "cluster";
  public CLUSTER_LAUNCH_URL = this.CLUSTER_BASE_URL + "/launch";
  public CLUSTER_TERMINATE_URL = this.CLUSTER_BASE_URL + "/terminate";
  public CLUSTER_STOP_URL = this.CLUSTER_BASE_URL + "/stop";
  public CLUSTER_START_URL = this.CLUSTER_BASE_URL + "/start";
  public CLUSTER_UPDATE_URL = this.CLUSTER_BASE_URL + "/update/name";

  constructor(private http: HttpClient) {}

  getClusters(available = false): Observable<Clusters[]> {
    return this.http.get<Clusters[]>(`${AppSettings.getApiEndpoint()}/${this.CLUSTER_BASE_URL}`, {
      params: { available },
    });
  }

  launchCluster(formData: FormData): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${this.CLUSTER_LAUNCH_URL}`, formData);
  }

  terminateCluster(cluster: Clusters): Observable<any> {
    return this.http.post(`${AppSettings.getApiEndpoint()}/${this.CLUSTER_TERMINATE_URL}`, cluster);
  }

  stopCluster(cluster: Clusters): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${this.CLUSTER_STOP_URL}`, cluster);
  }

  startCluster(cluster: Clusters): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${this.CLUSTER_START_URL}`, cluster);
  }

  updateCluster(cluster: Clusters): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${this.CLUSTER_UPDATE_URL}`, cluster);
  }
}
