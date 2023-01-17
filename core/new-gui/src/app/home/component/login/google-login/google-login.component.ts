import { Component } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Router } from "@angular/router";

@UntilDestroy()
@Component({
  selector: "texera-google-login",
  templateUrl: "./google-login.component.html",
  styleUrls: ["./google-login.component.scss"],
})
export class GoogleLoginComponent {
  constructor(private userService: UserService, private router: Router) {}

  /**
   * this method will retrieve a usable Google OAuth Instance first,
   * with that available instance, get googleUsername and authorization code respectively,
   * then sending the code to the backend
   */
  public googleLogin(): void {
    this.userService
      .googleLogin()
      .pipe(untilDestroyed(this))
      .subscribe(
        Zone.current.wrap(() => {
          // TODO temporary solution: the new page will append to the bottom of the page, and the original page does not remove, zone solves this issue
          this.router.navigate(["/dashboard/workflow"]);
        }, "")
      );
  }
}
