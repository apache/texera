import { Injectable } from "@angular/core";
import { Observable, Subject } from "rxjs";
import { NzMessageDataOptions } from "ng-zorro-antd/message";

export interface Notification {
  type: "success" | "info" | "error" | "warning" | "loading";
  message: string;
  options: NzMessageDataOptions;
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

  success(message: string, options: NzMessageDataOptions = {}) {
    this.sendNotification({ type: "success", message, options });
  }

  info(message: string, options: NzMessageDataOptions = {}) {
    this.sendNotification({ type: "info", message, options });
  }

  error(cause: Error | any, options: NzMessageDataOptions = {}) {
    this.sendNotification({
      type: "error",
      message: cause instanceof Error ? cause.message : cause.toString(),
      options,
    });
  }

  warning(message: string, options: NzMessageDataOptions = {}) {
    this.sendNotification({ type: "warning", message, options });
  }

  loading(message: string, options: NzMessageDataOptions = {}) {
    return this.sendNotification({ type: "loading", message, options });
  }
}
