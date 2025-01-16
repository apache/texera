import { AfterViewInit, Component } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { mergeMap } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DASHBOARD_USER_WORKFLOW } from "../../../../app-routing.constant";
import { ActivatedRoute, Router } from "@angular/router";
import { SocialAuthService } from "@abacritt/angularx-social-login";

@UntilDestroy()
@Component({
  selector: "texera-google-login",
  templateUrl: "./google-login.component.html",
})
export class GoogleLoginComponent implements AfterViewInit {
  constructor(
    private userService: UserService,
    private route: ActivatedRoute,
    private router: Router,
    private socialAuthService: SocialAuthService
  ) {}

  ngAfterViewInit() {
    this.socialAuthService.authState
      .pipe(
        mergeMap(res => this.userService.googleLogin(res.idToken)),
        untilDestroyed(this)
      )
      .subscribe(() => {
        this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || DASHBOARD_USER_WORKFLOW);
      });
  }
}
