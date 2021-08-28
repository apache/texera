import { Component, OnDestroy, OnInit } from "@angular/core";
import { NzMessageService } from "ng-zorro-antd/message";
import {
  Notification,
  NotificationService
} from "../../../service/notification/notification.service";
import { Subscription } from "rxjs";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-notification",
  templateUrl: "./notification.component.html",
  styleUrls: ["./notification.component.scss"]
})
export class NotificationComponent implements OnInit, OnDestroy {
  subscriptions = new Subscription();

  constructor(
    private message: NzMessageService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.subscriptions.add(
      this.notificationService
        .getNotificationStream()
        .pipe(untilDestroyed(this))
        .subscribe((notification: Notification) => {
          if (notification.type === "success") {
            this.message.success(notification.message);
          } else if (notification.type === "info") {
            this.message.info(notification.message);
          } else if (notification.type === "error") {
            this.message.error(notification.message);
          } else if (notification.type === "warning") {
            this.message.warning(notification.message);
          } else if (notification.type === "loading") {
            this.message.loading(notification.message);
          }
        })
    );
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }
}
