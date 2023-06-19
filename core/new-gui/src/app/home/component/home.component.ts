import { Component, OnDestroy, OnInit } from "@angular/core";
import { environment } from "../../../environments/environment";
import { UserService } from "../../common/service/user/user.service";
import { ActivatedRoute } from "@angular/router";
import { GoogleService } from "../service/google.service";
import { mergeMap, takeUntil } from "rxjs/operators";
import { Subject } from "rxjs";
import { HttpErrorResponse } from "@angular/common/http";
import { NotificationService } from "../../common/service/notification/notification.service";

@Component({
  selector: "texera-login",
  templateUrl: "./home.component.html",
  styleUrls: ["./home.component.scss"],
})
export class HomeComponent implements OnInit, OnDestroy {
  localLogin = environment.localLogin;
  unsubscriber = new Subject();
  constructor(
    private userService: UserService,
    private route: ActivatedRoute,
    private googleService: GoogleService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.googleService.googleInit(document.getElementById("googleButton"));
    this.googleService.googleCredentialResponse
      .pipe(
        mergeMap(res => this.userService.googleLogin(res.credential)),
        takeUntil(this.unsubscriber)
      )
      .subscribe({
        next: () => {
          window.location.href = this.route.snapshot.queryParams["returnUrl"] || "/dashboard/workflow";
        },
        error: (err: unknown) => {
          if (err instanceof HttpErrorResponse) {
            this.notificationService.error(err.error.message, {
              nzDuration: 0,
            });
          }
        },
      });
  }
  ngOnDestroy(): void {
    this.unsubscriber.next(1);
    this.unsubscriber.complete();
  }
}
