import { Injectable } from "@angular/core";
import { Observable, Subject } from "rxjs";

export interface Notification {
  type: "success" | "info" | "error" | "warning" | "loading";
  message: string;
  duration: number;
}

/**
 * NotificationService is an entry service for sending notifications
 * to show on NotificationComponent.
 */
@Injectable({
  providedIn: "root",
})
export class NotificationService {
  private notificationStream = new Subject<Notification>();

  getNotificationStream(): Observable<Notification> {
    return this.notificationStream.asObservable();
  }

  sendNotification(notification: Notification) {
    this.notificationStream.next(notification);
  }

  success(message: string, duration: number = 3000) {
    this.sendNotification({ type: "success", message, duration });
  }

  info(message: string, duration: number = 3000) {
    this.sendNotification({ type: "info", message, duration });
  }

  error(cause: Error | any, duration: number = 3000) {
    this.sendNotification({
      type: "error",
      message: cause instanceof Error ? cause.message : cause.toString(),
      duration,
    });
  }

  warning(message: string, duration: number = 3000) {
    this.sendNotification({ type: "warning", message, duration });
  }

  loading(message: string, duration: number = 3000) {
    return this.sendNotification({ type: "loading", message, duration });
  }
}
