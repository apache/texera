import { Injectable } from "@angular/core";
import { NzMessageDataOptions, NzMessageService} from "ng-zorro-antd/message";
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
    private notification: NzNotificationService) {}

  createReportNotification(): string {
    const notificationId = this.notification.blank(
      "报告生成中",
      "报告正在生成，请稍候...",
      { nzDuration: 0 } // 确保通知不会自动关闭
    ).messageId; // 保存通知的ID，稍后可以用来关闭

    return notificationId;
  }

  blank(title: string, content: string): void {
    this.notification.blank(title, content, { nzDuration: 0 });
  }

  remove(id?: string): void {
    this.notification.remove(id);
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
