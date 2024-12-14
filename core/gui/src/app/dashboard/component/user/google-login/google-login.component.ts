import { AfterViewInit, Component, ElementRef, ViewChild } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { mergeMap } from "rxjs/operators";
import { GoogleAuthService } from "../../../../common/service/user/google-auth.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DASHBOARD_USER_WORKFLOW } from "../../../../app-routing.constant";
import { ActivatedRoute, Router } from "@angular/router";

@UntilDestroy()
@Component({
  selector: "texera-google-login",
  template: "",
})
export class GoogleLoginComponent implements AfterViewInit {
  @ViewChild("googleButton") googleButton!: ElementRef;
  constructor(
    private userService: UserService,
    private route: ActivatedRoute,
    private googleAuthService: GoogleAuthService,
    private router: Router,
    private elementRef: ElementRef
  ) {}

  ngAfterViewInit(): void {
    this.googleAuthService.googleAuthInit(this.elementRef.nativeElement);
    this.googleAuthService.googleCredentialResponse
      .pipe(
        mergeMap(res => this.userService.googleLogin(res.credential)),
        untilDestroyed(this)
      )
      .subscribe(() => {
        this.router.navigateByUrl(this.route.snapshot.queryParams["returnUrl"] || DASHBOARD_USER_WORKFLOW);
      });
  }
}
