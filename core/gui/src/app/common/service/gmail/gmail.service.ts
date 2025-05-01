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

  public notifyUnauthorizedLogin(userEmail: string): void {
    this.http.post(`${AppSettings.getApiEndpoint()}/gmail/notify-unauthorized`, { receiver: userEmail }).subscribe({
      next: () => this.notificationService.success("Admin has been notified about your account request."),
      error: (error: unknown) => {
        if (error instanceof HttpErrorResponse) {
          this.notificationService.error("Failed to notify admin about your account request.");
          console.error("Notify error:", error.error);
        }
      },
    });
  }
}
