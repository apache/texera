import { Component, OnDestroy, OnInit } from '@angular/core';
import { NzMessageService } from 'ng-zorro-antd/message';
import { Notification, NotificationService } from '../../../service/notification/notification.service';
import { Subscription } from 'rxjs';

@Component({
  selector: 'texera-notification',
  templateUrl: './notification.component.html',
  styleUrls: ['./notification.component.scss']
})
export class NotificationComponent implements OnInit, OnDestroy {

  subscriptions = new Subscription();

  constructor(
    private message: NzMessageService,
    private notificationService: NotificationService
  ) { }

  ngOnInit(): void {
    this.subscriptions.add(this.notificationService.getNotificationStream().subscribe((notification: Notification) => {
      if (notification.type === 'info') {
        this.message.info(notification.message);
      } else if (notification.type === 'error') {
        this.message.error(notification.message);
      }
    }));
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }


}
