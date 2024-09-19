import { Injectable } from "@angular/core";
import { NzMessageDataOptions, NzMessageService } from "ng-zorro-antd/message";
import { NzNotificationService } from "ng-zorro-antd/notification";

/**
 * NotificationService is an entry service for sending notifications
 */
@Injectable({
  providedIn: "root",
})
export class NotificationService {
  constructor(
    private message: NzMessageService,
    private notification: NzNotificationService
  ) {}

  // Only blank can be removed manually
  blank(title: string, content: string, options: NzMessageDataOptions = {}): void {
    this.notification.blank(title, content, options);
  }

  // Remove current blank notification only
  remove(): void {
    this.notification.remove();
  }

  success(message: string, options: NzMessageDataOptions = {}) {
    this.message.success(message, options);
  }

  info(message: string, options: NzMessageDataOptions = {}) {
    this.message.info(message, options);
  }

  error(message: string, options: NzMessageDataOptions = {}) {
    this.message.error(message, options);
  }

  warning(message: string, options: NzMessageDataOptions = {}) {
    this.message.warning(message, options);
  }

  loading(message: string, options: NzMessageDataOptions = {}) {
    return this.message.loading(message, options);
  }
}
