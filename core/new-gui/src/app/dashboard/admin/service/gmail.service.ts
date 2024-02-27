import { Injectable } from "@angular/core";
import { HttpClient, HttpErrorResponse } from "@angular/common/http";
import { AppSettings } from "../../../common/app-setting";
import { Subject } from "rxjs";
import { CredentialResponse } from "../../../common/service/user/google-auth.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
declare var window: any;
@Injectable({
  providedIn: "root",
})
export class GmailService {
  public client: any;
  private _googleCredentialResponse = new Subject<CredentialResponse>();
  constructor(
    private http: HttpClient,
    private notificationService: NotificationService
  ) {}
  public authSender() {
    this.http
      .get(`${AppSettings.getApiEndpoint()}/auth/google/clientid`, { responseType: "text" })
      .subscribe(response => {
        this.client = window.google.accounts.oauth2.initCodeClient({
          access_type: "offline",
          scope: "email https://www.googleapis.com/auth/gmail.send",
          client_id: response,
          callback: (auth: any) => {
            this.http
              .post(`${AppSettings.getApiEndpoint()}/gmail/sender/auth`, `${auth.code}`)
              .subscribe(() => this._googleCredentialResponse.next(auth));
          },
        });
      });
  }

  public revokeAuth() {
    return this.http.delete(`${AppSettings.getApiEndpoint()}/gmail/sender/revoke`);
  }
  public getSenderEmail() {
    return this.http.get(`${AppSettings.getApiEndpoint()}/gmail/sender/email`, { responseType: "text" });
  }

  public sendEmail(subject: string, content: string, receiver: string = "") {
    this.http
      .put(`${AppSettings.getApiEndpoint()}/gmail/send`, { receiver: receiver, subject: subject, content: content })
      .subscribe({
        next: () => this.notificationService.success("Email sent successfully"),
        error: (error: unknown) => {
          if (error instanceof HttpErrorResponse) {
            console.error(error.error);
          }
        },
      });
  }

  get googleCredentialResponse() {
    return this._googleCredentialResponse.asObservable();
  }
}
