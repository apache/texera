import { Injectable } from "@angular/core";
import { NzMessageDataOptions, NzMessageService } from "ng-zorro-antd/message";

/**
 * NotificationService is an entry service for sending notifications
 */
@Injectable({
  providedIn: "root",
})
export class NotificationService {
  constructor(private message: NzMessageService) {}

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
