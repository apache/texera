import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';

export interface Notification {
  type: 'success' | 'info' | 'error' | 'warning' | 'loading';
  message: string;
}

/**
 * NotificationService is an entry service for sending notifications
 * to show on NotificationComponent.
 */
@Injectable({
  providedIn: 'root'
})
export class NotificationService {
  private notificationStream = new Subject<Notification>();

  getNotificationStream(): Observable<Notification> {
    return this.notificationStream.asObservable();
  }

  sendNotification(notification: Notification) {
    this.notificationStream.next(notification);
  }

  success(message: string) {
    this.sendNotification({ type: 'success', message });
  }

  info(message: string) {
    this.sendNotification({ type: 'info', message });
  }

  error(message: string) {
    this.sendNotification({ type: 'error', message });
  }

  warning(message: string) {
    this.sendNotification({ type: 'warning', message });
  }

  loading(message: string) {
    this.sendNotification({ type: 'loading', message });
  }
}
