import { Component, OnInit } from "@angular/core";
import { environment } from "../../../environments/environment";
import { UserService } from "../../common/service/user/user.service";
import { ActivatedRoute, Router } from "@angular/router";
import { catchError, mergeMap } from "rxjs/operators";
import { throwError } from "rxjs";
import { HttpErrorResponse } from "@angular/common/http";
import { NotificationService } from "../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { GoogleAuthService } from "../../common/service/user/google-auth.service";

@UntilDestroy()
@Component({
  selector: "texera-login",
  templateUrl: "./home.component.html",
  styleUrls: ["./home.component.scss"],
})
export class HomeComponent implements OnInit {
  localLogin = environment.localLogin;
  constructor(
    private userService: UserService,
    private route: ActivatedRoute,
    private googleAuthService: GoogleAuthService,
    private notificationService: NotificationService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.googleAuthService.googleAuthInit(document.getElementById("googleButton"));
    this.googleAuthService.googleCredentialResponse
      .pipe(mergeMap(res => this.userService.googleLogin(res.credential)))
      .pipe(
        catchError((err: unknown) => {
          if (err instanceof HttpErrorResponse) {
            this.notificationService.error(err.error.message, {
              nzDuration: 0,
            });
          }
          return throwError(() => err);
        }),
        untilDestroyed(this)
      )
      .subscribe(
        // The new page will append to the bottom of the page, and the original page does not remove, zone solves this issue
        Zone.current.wrap(() => {
          this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || "/dashboard/workflow");
        }, "")
      );
  }
}
