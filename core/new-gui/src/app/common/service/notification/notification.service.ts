import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';

export interface Notification {
  type: 'info' | 'error';
  message: string;
}

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
}
