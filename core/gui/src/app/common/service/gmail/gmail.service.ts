import { Injectable } from "@angular/core";
import { HttpClient, HttpErrorResponse } from "@angular/common/http";
import { AppSettings } from "../../app-setting";
import { NotificationService } from "../notification/notification.service";
@Injectable({
  providedIn: "root",
})
export class GmailService {
  constructor(
    private http: HttpClient,
    private notificationService: NotificationService
  ) {}

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
}
