import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../../common/app-setting";
declare var window: any;
@Injectable({
  providedIn: "root",
})
export class GmailService {
  public client: any;
  constructor(private http: HttpClient) {}
  public authInit() {
    this.http
      .get(`${AppSettings.getApiEndpoint()}/auth/google/clientid`, { responseType: "text" })
      .subscribe(response => {
        this.client = window.google.accounts.oauth2.initCodeClient({
          access_type: "offline",
          scope: "https://mail.google.com/",
          client_id: response,
          callback: (auth: any) => {
            this.http.post(`${AppSettings.getApiEndpoint()}/gmail/auth`, `${auth.code}`).subscribe();
          },
        });
      });
  }
}
