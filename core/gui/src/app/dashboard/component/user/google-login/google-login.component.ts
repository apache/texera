import { AfterViewInit, Component, ElementRef, ViewChild } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { ActivatedRoute, Router } from "@angular/router";
import { mergeMap } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { GoogleAuthService } from "../../../../common/service/user/google-auth.service";

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
      .subscribe(() => {});
  }
}
