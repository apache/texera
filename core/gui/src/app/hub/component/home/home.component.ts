import { UntilDestroy } from "@ngneat/until-destroy";
import { Component, OnInit } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { UserService } from "../../../common/service/user/user.service";
import { ActivatedRoute, Router } from "@angular/router";
import { GoogleAuthService } from "../../../common/service/user/google-auth.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { catchError, mergeMap } from "rxjs/operators";
import { throwError } from "rxjs";

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
    if (!sessionStorage.getItem("homePageReloaded")) {
      sessionStorage.setItem("homePageReloaded", "true");
      window.location.reload();
    } else {
      sessionStorage.removeItem("homePageReloaded");

      this.googleAuthService.googleAuthInit(document.getElementById("googleButton"));
      this.googleAuthService.googleCredentialResponse
        .pipe(mergeMap(res => this.userService.googleLogin(res.credential)))
        .pipe(
          catchError((e: unknown) => {
            this.notificationService.error((e as Error).message, { nzDuration: 10 });
            return throwError(() => e);
          })
        )
        // eslint-disable-next-line rxjs-angular/prefer-takeuntil
        .subscribe(() =>
          this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || "/dashboard/user/workflow")
        );
    }
  }
}
