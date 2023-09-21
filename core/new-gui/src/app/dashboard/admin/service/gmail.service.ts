import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../../common/app-setting";
import { Subject } from "rxjs";
import { CredentialResponse } from "../../../common/service/user/google-auth.service";
declare var window: any;
@Injectable({
  providedIn: "root",
})
export class GmailService {
  public client: any;
  private _googleCredentialResponse = new Subject<CredentialResponse>();
  constructor(private http: HttpClient) {}
  public auth() {
    this.http
      .get(`${AppSettings.getApiEndpoint()}/auth/google/clientid`, { responseType: "text" })
      .subscribe(response => {
        this.client = window.google.accounts.oauth2.initCodeClient({
          access_type: "offline",
          scope: "email https://www.googleapis.com/auth/gmail.send",
          client_id: response,
          callback: (auth: any) => {
            this.http
              .post(`${AppSettings.getApiEndpoint()}/gmail/auth`, `${auth.code}`)
              .subscribe(() => this._googleCredentialResponse.next(auth));
          },
        });
      });
  }

  public sendEmail(subject: string, content: string, email: string = "") {
    this.http
      .put(`${AppSettings.getApiEndpoint()}/gmail/send`, { email: email, subject: subject, content: content })
      .subscribe();
  }

  public getEmail() {
    return this.http.get(`${AppSettings.getApiEndpoint()}/gmail/email`, { responseType: "text" });
  }

  public deleteEmail() {
    return this.http.delete(`${AppSettings.getApiEndpoint()}/gmail/revoke`);
  }

  get googleCredentialResponse() {
    return this._googleCredentialResponse.asObservable();
  }
}
