import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../app-setting";

@Injectable({
  providedIn: "root",
})
export class GoogleAuthService {
  constructor(private http: HttpClient) {}

  getClientId(): Observable<string> {
    return this.http.get(`${AppSettings.getApiEndpoint()}/auth/google/clientid`, { responseType: "text" });
  }
}
