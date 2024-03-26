import { Component, OnInit } from "@angular/core";
import { environment } from "../../../environments/environment";
import { UserService } from "../../common/service/user/user.service";
import { ActivatedRoute, Router } from "@angular/router";
import { catchError, mergeMap } from "rxjs/operators";
import { throwError } from "rxjs";
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
        catchError((e: unknown) => {
          this.notificationService.error((e as Error).message, { nzDuration: 10 });
          return throwError(() => e);
        }),
        untilDestroyed(this)
      )
      .subscribe(() =>
        this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || "/dashboard/workflow")
      );
  }
}
